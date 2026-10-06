.DEFAULT_GOAL := help

.PHONY: help init config up down restart logs ps validate smoke demo-fetch demo-up demo-core demo-experiment demo-down dataset-export train clean-runtime

DATABASE_URL ?= postgresql://rca:rca-local-password@localhost:55432/rca
DATASET ?= data/exports/candidates.jsonl
MODEL_DIR ?= artifacts/champion

help:
	@echo "Telemetry RCA development commands"
	@echo "  make init        Create .env (if missing), validate, and start the product stack"
	@echo "  make up          Build and start PostgreSQL, API, worker, gateway, and Grafana"
	@echo "  make down        Stop the product stack without deleting data"
	@echo "  make logs        Follow product stack logs"
	@echo "  make validate    Render and validate the Docker Compose model"
	@echo "  make demo-fetch  Fetch the pinned OpenTelemetry Demo source"
	@echo "  make demo-up     Start the pinned full demo and route it to the gateway"
	@echo "  make demo-core   Start the smaller core demo"
	@echo "  make smoke       Run a self-contained normalized-event RCA smoke test"
	@echo "  make demo-experiment  Run a reversible payment-failure experiment"
	@echo "  make demo-down   Stop the demo safely"
	@echo "  make dataset-export  Export completed labeled runs without split leakage"
	@echo "  make train       Train and evaluate the portable baseline from DATASET"

init: config validate up

config:
	@test -f .env || cp .env.example .env

up:
	docker compose up -d --build

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

validate:
	docker compose config --quiet

demo-fetch:
	./scripts/fetch-demo.sh

demo-up:
	./scripts/start-demo.sh --full

demo-core:
	./scripts/start-demo.sh --core

smoke:
	./scripts/run-synthetic-demo.sh

demo-experiment:
	./scripts/run-demo-experiment.sh

demo-down:
	./scripts/stop-demo.sh

dataset-export:
	python -m rca.export_dataset --database-url "$(DATABASE_URL)" --output "$(DATASET)"

train:
	python -m rca.train "$(DATASET)" --output-dir "$(MODEL_DIR)" --portable --database-url "$(DATABASE_URL)"

# Removes generated integration metadata only. Database and Grafana volumes are untouched.
clean-runtime:
	./scripts/stop-demo.sh
	@test ! -d .runtime || find .runtime -mindepth 1 -maxdepth 1 -type f -delete
