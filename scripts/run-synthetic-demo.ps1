[CmdletBinding()]
param(
    [ValidateSet("database_lock", "payment_failure", "catalog_latency", "kafka_backlog", "checkout_cpu", "recommendation_memory", "traffic_flood")]
    [string]$Scenario = "database_lock",
    [int]$Seed = 42,
    [double]$Severity = 1.25,
    [ValidateRange(10, 900)][int]$TimeoutSeconds = 120
)

. (Join-Path $PSScriptRoot "Common.ps1")
$python = Get-PythonCommand
$apiPort = Get-ConfigValue -Name "RCA_API_PORT" -Default "8000"
$helper = Join-Path $PSScriptRoot "synthetic_smoke.py"

Push-Location $script:ProjectRoot
try {
    & $python $helper `
        --api-url "http://localhost:$apiPort" `
        --scenario $Scenario `
        --seed $Seed `
        --severity $Severity `
        --incident-timeout $TimeoutSeconds
    if ($LASTEXITCODE -ne 0) { throw "Synthetic end-to-end smoke test failed" }
} finally {
    Pop-Location
}
