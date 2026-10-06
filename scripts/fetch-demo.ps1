[CmdletBinding()]
param()

. (Join-Path $PSScriptRoot "Common.ps1")
Assert-Command -Name "git"

$repository = Get-ConfigValue -Name "OTEL_DEMO_REPOSITORY" -Default "https://github.com/open-telemetry/opentelemetry-demo.git"
$commit = Get-ConfigValue -Name "OTEL_DEMO_COMMIT" -Default "dedc0178918e260823323b8d95005a8cb924b007"
$directory = Get-DemoDirectory
$parent = Split-Path -Parent $directory
New-Item -ItemType Directory -Force -Path $parent | Out-Null

if ((Test-Path -LiteralPath $directory) -and -not (Test-Path -LiteralPath (Join-Path $directory ".git"))) {
    throw "Refusing to overwrite non-Git path: $directory"
}

if (-not (Test-Path -LiteralPath (Join-Path $directory ".git"))) {
    Write-Host "Cloning OpenTelemetry Demo into $directory"
    & git clone --filter=blob:none --no-checkout $repository $directory
    if ($LASTEXITCODE -ne 0) { throw "git clone failed" }
} else {
    $actualOrigin = (& git -C $directory remote get-url origin).Trim()
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect demo origin" }
    if ($actualOrigin.TrimEnd(".git") -ne $repository.TrimEnd(".git")) {
        throw "Existing checkout has unexpected origin: $actualOrigin"
    }
    $changes = (& git -C $directory status --porcelain) -join "`n"
    if (-not [string]::IsNullOrWhiteSpace($changes)) {
        throw "The demo checkout has local changes; refusing to overwrite them: $directory"
    }
}

& git -C $directory fetch --depth=1 origin $commit
if ($LASTEXITCODE -ne 0) { throw "git fetch failed" }
& git -C $directory checkout --detach $commit
if ($LASTEXITCODE -ne 0) { throw "git checkout failed" }

$actualCommit = (& git -C $directory rev-parse HEAD).Trim()
if ($actualCommit -ne $commit) {
    throw "Pin verification failed: expected $commit, found $actualCommit"
}

Write-Host "OpenTelemetry Demo is pinned at $actualCommit"
