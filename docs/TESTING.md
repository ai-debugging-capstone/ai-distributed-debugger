# Automated testing on Windows

Run commands from the repository root. Python 3.11+ is required; the M5 run used
Python 3.13.7 and PostgreSQL 17.6. Linux containers use Python 3.12. No native
PostgreSQL installation, model credentials, optional scikit-learn/PyTorch package,
or model retraining is needed for these tests. The shipped portable model is loaded
read-only. Existing model training tests use their own temporary fixture artifacts.

## Python dependencies

Use a local environment; activation and global execution-policy changes are unnecessary:

```powershell
if (!(Test-Path .venv\Scripts\python.exe)) { py -3 -m venv .venv }
.\.venv\Scripts\python.exe -m pip install -r app/requirements-dev.txt -r rca/requirements-dev.txt
.\.venv\Scripts\python.exe -m pip check
```

The declared requirements provide pytest, pytest-asyncio, FastAPI/TestClient,
HTTPX, Psycopg/pool, protobuf, and OpenTelemetry protobuf definitions. Root
`pytest.ini` supplies import paths; no `PYTHONPATH` assignment is required. Use
`-c pytest.ini` when selecting individual files so the app's narrower config
does not take precedence. The existing app-directory-only test command also works.

## Offline unit and component tests

```powershell
# Unit tests, excluding component/database integration:
.\.venv\Scripts\python.exe -m pytest -c pytest.ini -m "not postgres and not component"

# In-memory component integration, including actual deployed-model inference:
.\.venv\Scripts\python.exe -m pytest -c pytest.ini -m component

# Both groups together, without PostgreSQL:
.\.venv\Scripts\python.exe -m pytest -c pytest.ini -m "not postgres"
```

M5 results: 148 unit tests and 6 component tests passed. The component marker also
identifies the four existing synthetic end-to-end tests. Payloads and fault
scenarios are synthetic. Inference uses genuine fitted parameters, not canned
predictions. Tests check feature format and execution, not diagnosis accuracy.

## Real PostgreSQL prerequisites

Docker Desktop must be running with its Linux/WSL backend. The tests need only
PostgreSQL; the API is instantiated in-process with its real `PsycopgStorage`.
The collector, worker, frontend, and upstream demo are not prerequisites.

Use a **separate test-only container**, leaving the product stack and its volumes
alone. The following fixed password is only for this disposable local test server.
If the name or port is occupied, choose an unused test name/port and adjust the URL;
do not remove an unrelated container to make room.

```powershell
$taskDockerCommand = Get-Command docker -ErrorAction SilentlyContinue
$taskDocker = if ($taskDockerCommand) { $taskDockerCommand.Source } else {
    "$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin\docker.exe"
}
& $taskDocker version
& $taskDocker run --rm -d --name m5-testing-postgres -e POSTGRES_USER=m5_test -e POSTGRES_PASSWORD=m5-test-only -e POSTGRES_DB=m5_admin -p 127.0.0.1:55433:5432 postgres:17.6-alpine
& $taskDocker exec m5-testing-postgres pg_isready -U m5_test -d m5_admin
```

Wait for `pg_isready` to report accepting connections; repeat that check if startup
is still in progress. No global PATH/Windows setting changes are needed.

## Database and full-suite commands

```powershell
$env:TEST_POSTGRES_ADMIN_URL = 'postgresql://m5_test:m5-test-only@127.0.0.1:55433/m5_admin'

# Only real PostgreSQL integration (11 tests):
.\.venv\Scripts\python.exe -m pytest -c pytest.ini -m postgres --run-postgres

# All 165 tests, including PostgreSQL; save presentation evidence:
.\.venv\Scripts\python.exe -m pytest -c pytest.ini --run-postgres --junitxml=tmp/m5-testing-full.xml
```

The PostgreSQL role must have `CREATEDB` and permission to install `pgcrypto` in
its new databases. The local Docker image's test role has these privileges.
`TEST_POSTGRES_ADMIN_URL` is deliberately separate from application `DATABASE_URL`:
the fixture does not load `.env` or reuse the application's database as a test store.
An explicitly supplied administrative URL can use another prepared PostgreSQL
server, but prefer the disposable container above.

Without `--run-postgres`, database tests are skipped even if the URL is set. With
the flag but no URL, pytest reports a configuration error. With both configured,
connection, permissions, migration, or readiness failures are errors, not skips
and not fake successful integration.

An unfiltered offline run (`python -m pytest -c pytest.ini`) reports **154 passed,
11 skipped**; that is intentionally different from the full verified run's
**165 passed, 0 failed, 0 skipped**. Two existing unittest subtests are additionally
reported as passed by pytest 9; they are not extra collected test functions.

## Isolation and cleanup

Each database test creates `m5_test_<random UUID hex>` from `template0`, checks its
connection's database name, and applies unchanged SQL from `db/init/001` through
`004`. It then uses the actual API, normalizer, async pool, PostgreSQL constraints,
and JSONB storage. Synchronous queries and the existing capture-summary endpoint
verify committed rows. All telemetry sent by the tests is clearly synthetic OTLP
or JSON, not recorded production/demo service traffic.

The fixture closes API/pool and query connections, then drops only the database
it successfully created. Cleanup runs even after a test/setup failure. It never
migrates, truncates, drops, or resets the administrative/application database;
there is no forced disconnection. On Windows the TestClient uses its own selector
event loop for Psycopg, without changing the process event-loop policy.

Check cleanup and stop only the test server you created:

```powershell
& $taskDocker exec m5-testing-postgres psql -U m5_test -d m5_admin -Atc "SELECT datname FROM pg_database WHERE datname LIKE 'm5_test_%';"
# Expected: zero rows.
& $taskDocker stop m5-testing-postgres
Remove-Item Env:TEST_POSTGRES_ADMIN_URL -ErrorAction SilentlyContinue
```

`--rm` removes this container and its anonymous test volume when stopped. Never
use `docker compose down -v` as a test cleanup command. If Python is forcibly
killed, fixture finalizers cannot run; inspect leftover names before cleanup.
The disposable server can be removed instead of touching any product database.

## Known limitation and ongoing milestones

`test_jsonb_storage_failure_rolls_back_valid_sibling_and_run_placeholder` deliberately
verifies a known failure mode: an OTLP NaN attribute reaches JSONB, returns HTTP
503, and rolls back even a valid sibling. It proves atomic rollback and recovery,
not good malformed-record quarantine. Per-record handling of nonfinite values is
still a follow-up defect. The repaired malformed-ID/empty-point cases do exercise
successful quarantine alongside valid records.

Identical payloads without source timestamps now deduplicate across retries; they
cannot distinguish two separate identical occurrences. Producers should send
timestamps. Already stored fallback-based hashes are not migrated.

For every future milestone/PR, retain these tests, add a regression for changed
behavior, and run both offline and database commands. Save JUnit results and
report skipped integration explicitly. Update feature-contract tests when Reza
intentionally changes the deployed model. CI enforcement can be added separately;
no remote CI run is claimed by this local implementation.
