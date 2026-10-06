Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:ProjectRoot = Split-Path -Parent $PSScriptRoot
$script:DotEnvFile = Join-Path $script:ProjectRoot ".env"

function Get-ConfigValue {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [string]$Default = ""
    )

    $environmentValue = [Environment]::GetEnvironmentVariable($Name)
    if (-not [string]::IsNullOrWhiteSpace($environmentValue)) {
        return $environmentValue
    }

    if (Test-Path -LiteralPath $script:DotEnvFile) {
        $line = Get-Content -LiteralPath $script:DotEnvFile |
            Where-Object { $_ -match "^\s*$([regex]::Escape($Name))=" } |
            Select-Object -Last 1
        if ($null -ne $line) {
            $value = ($line -split "=", 2)[1].Trim()
            if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
                ($value.StartsWith("'") -and $value.EndsWith("'"))) {
                $value = $value.Substring(1, $value.Length - 2)
            }
            return $value
        }
    }

    return $Default
}

function Assert-Command {
    param([Parameter(Mandatory = $true)][string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found: $Name"
    }
}

function Get-DemoDirectory {
    $configured = Get-ConfigValue -Name "OTEL_DEMO_DIR" -Default ".runtime/opentelemetry-demo"
    if ([System.IO.Path]::IsPathRooted($configured)) {
        return [System.IO.Path]::GetFullPath($configured)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $script:ProjectRoot $configured))
}

function Ensure-SharedNetwork {
    param([Parameter(Mandatory = $true)][string]$Name)
    & docker network inspect $Name *> $null
    if ($LASTEXITCODE -ne 0) {
        & docker network create --label telemetry-rca.managed=true $Name *> $null
        if ($LASTEXITCODE -ne 0) { throw "Could not create Docker network $Name" }
        Write-Host "Created shared Docker network $Name"
    }
}

function Set-DemoEnvironment {
    $env:RCA_SHARED_NETWORK = Get-ConfigValue -Name "RCA_SHARED_NETWORK" -Default "opentelemetry-demo"
    $env:RCA_DEMO_COLLECTOR_CONFIG = [System.IO.Path]::GetFullPath(
        (Join-Path $script:ProjectRoot "infra/demo/otelcol-rca.yml")
    )
    $env:DEMO_VERSION = "3.1.0"
    $env:IMAGE_VERSION = "3.1.0"
    # Preserve the Demo's sanitization and infrastructure receivers. Its
    # collector forwards the processed signals to TraceLens in compose.rca.yaml.
    $env:OTEL_COLLECTOR_HOST = "otel-collector"
    $env:OTEL_EXPORTER_OTLP_ENDPOINT = "http://otel-collector:4317"
    $env:PUBLIC_OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = "http://localhost:8080/otlp-http/v1/traces"
    $env:GRAFANA_HOST = "rca-grafana"
    $env:GRAFANA_PORT = "3000"
    $env:LOCUST_AUTOSTART = "true"
    # Keep the web listener enabled: upstream's healthcheck probes it on 8089.
    # LOCUST_AUTOSTART still begins traffic without operator interaction.
    $env:LOCUST_HEADLESS = "false"
    if ([string]::IsNullOrWhiteSpace($env:LOCUST_USERS)) { $env:LOCUST_USERS = "10" }
}

function Get-DemoComposeArguments {
    param(
        [Parameter(Mandatory = $true)][ValidateSet("core", "full")][string]$Profile,
        [Parameter(Mandatory = $true)][string]$DemoDirectory
    )

    $arguments = @(
        "compose",
        "--project-name", "telemetry-rca-demo",
        "--env-file", (Join-Path $DemoDirectory ".env"),
        "--file", (Join-Path $DemoDirectory "compose.yaml")
    )
    if ($Profile -eq "full") {
        $arguments += @("--file", (Join-Path $DemoDirectory "compose.full.yaml"))
    }
    $arguments += @("--file", (Join-Path $script:ProjectRoot "infra/demo/compose.rca.yaml"))
    return $arguments
}

function Invoke-Docker {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Docker command failed with exit code $LASTEXITCODE"
    }
}

function Get-DemoProfile {
    $stateFile = Join-Path $script:ProjectRoot ".runtime/demo-profile"
    if (Test-Path -LiteralPath $stateFile) {
        return (Get-Content -LiteralPath $stateFile -Raw).Trim()
    }
    return Get-ConfigValue -Name "OTEL_DEMO_PROFILE" -Default "full"
}

function Get-PythonCommand {
    if (Get-Command python -ErrorAction SilentlyContinue) { return "python" }
    if (Get-Command python3 -ErrorAction SilentlyContinue) { return "python3" }
    throw "Python 3 is required for safe feature-flag editing."
}
