[CmdletBinding()]
param(
    [ValidateSet("core", "full")][string]$Profile = "full",
    [switch]$SkipPlatform
)

. (Join-Path $PSScriptRoot "Common.ps1")
Assert-Command -Name "docker"

& (Join-Path $PSScriptRoot "fetch-demo.ps1")
if ($LASTEXITCODE -ne 0) { throw "Could not fetch the pinned demo" }

if (-not $SkipPlatform) {
    Write-Host "Starting the RCA platform..."
    Invoke-Docker -Arguments @("compose", "--file", (Join-Path $script:ProjectRoot "docker-compose.yml"), "up", "--detach", "--build")
}

Set-DemoEnvironment
Ensure-SharedNetwork -Name $env:RCA_SHARED_NETWORK

$directory = Get-DemoDirectory
$compose = Get-DemoComposeArguments -Profile $Profile -DemoDirectory $directory
Write-Host "Pulling immutable OpenTelemetry Demo 3.1.0 images..."
Invoke-Docker -Arguments ($compose + @("pull"))
Invoke-Docker -Arguments ($compose + @("up", "--detach", "--no-build", "--remove-orphans"))

$runtimeDirectory = Join-Path $script:ProjectRoot ".runtime"
New-Item -ItemType Directory -Force -Path $runtimeDirectory | Out-Null
Set-Content -LiteralPath (Join-Path $runtimeDirectory "demo-profile") -Value $Profile -NoNewline

Write-Host -NoNewline "Waiting for the demo frontend proxy to become healthy"
$deadline = [DateTime]::UtcNow.AddMinutes(5)
$health = ""
do {
    $health = (& docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' frontend-proxy 2>$null).Trim()
    if ($health -eq "healthy" -or $health -eq "running") { break }
    Write-Host -NoNewline "."
    Start-Sleep -Seconds 5
} while ([DateTime]::UtcNow -lt $deadline)
Write-Host

if ($health -ne "healthy" -and $health -ne "running") {
    throw "Demo did not become healthy within five minutes. Inspect frontend-proxy logs with Docker Compose."
}

$grafanaPort = Get-ConfigValue -Name "GRAFANA_PORT" -Default "3000"
$apiPort = Get-ConfigValue -Name "RCA_API_PORT" -Default "8000"
Write-Host "OpenTelemetry Demo: http://localhost:8080"
Write-Host "RCA dashboard:      http://localhost:$grafanaPort/d/rca-overview"
Write-Host "RCA API:            http://localhost:$apiPort/docs"
Write-Host "All demo telemetry is routed to otel-gateway on network $env:RCA_SHARED_NETWORK."
