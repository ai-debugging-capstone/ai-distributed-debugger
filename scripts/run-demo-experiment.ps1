[CmdletBinding()]
param(
    [string]$Scenario = "paymentFailure",
    [string]$Variant = "50%",
    [ValidateRange(0, 2147483647)][int]$Seed = 42,
    [ValidateSet("train", "validation", "test", "holdout", "unassigned")]
    [string]$Split = "unassigned",
    [ValidateRange(0, 86400)][int]$WarmupSeconds = 60,
    [ValidateRange(0, 86400)][int]$DurationSeconds = 120,
    [ValidateRange(0, 86400)][int]$RecoverySeconds = 60,
    [ValidateRange(1, 100)][int]$AcceptanceTopK = 3
)

. (Join-Path $PSScriptRoot "Common.ps1")
Assert-Command -Name "docker"
$python = Get-PythonCommand
$directory = Get-DemoDirectory
$flagFile = Join-Path $directory "src/flagd/demo.flagd.json"
if (-not (Test-Path -LiteralPath $flagFile)) {
    throw "Demo is not fetched. Run scripts/start-demo.ps1 first."
}

Set-DemoEnvironment
$profile = Get-DemoProfile
$compose = Get-DemoComposeArguments -Profile $profile -DemoDirectory $directory

function Wait-DemoReady {
    param(
        [Parameter(Mandatory = $true)][string[]]$ComposeArguments,
        [int]$TimeoutSeconds = 300
    )

    $requiredServices = @(
        & docker @ComposeArguments config --services |
            ForEach-Object { $_.Trim() } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    if ($LASTEXITCODE -ne 0 -or $requiredServices.Count -eq 0) {
        throw "Could not determine the required Demo service set"
    }
    foreach ($required in @("flagd", "frontend-proxy", "load-generator")) {
        if ($requiredServices -notcontains $required) {
            throw "Required Demo service '$required' is absent from the Compose model"
        }
    }

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $pending = [System.Collections.Generic.List[string]]::new()
        foreach ($service in $requiredServices) {
            $containerIds = @(
                & docker @ComposeArguments ps --quiet $service 2>$null |
                    ForEach-Object { $_.Trim() } |
                    Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
            )
            if ($LASTEXITCODE -ne 0 -or $containerIds.Count -eq 0) {
                $pending.Add("${service}:not-running")
                continue
            }
            foreach ($containerId in $containerIds) {
                $state = (& docker inspect --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' $containerId 2>$null).Trim()
                if ($LASTEXITCODE -ne 0 -or
                    ($state -ne "running|healthy" -and $state -ne "running|none")) {
                    $pending.Add("${service}:$state")
                }
            }
        }
        if ($pending.Count -eq 0) {
            Write-Host "Demo reset complete: all $($requiredServices.Count) required services are running and healthy where health checks exist."
            return
        }
        Start-Sleep -Seconds 3
    } while ([DateTime]::UtcNow -lt $deadline)

    throw "Demo did not recover after reset. Pending services: $($pending -join ', ')"
}

$runningServices = (& docker @compose ps --status running --services) -join "`n"
if ($LASTEXITCODE -ne 0 -or ($runningServices -split "`n") -notcontains "flagd") {
    throw "The demo flagd service is not running. Run scripts/start-demo.ps1 first."
}

$helper = Join-Path $PSScriptRoot "demo_experiment.py"
$originalVariant = (& $python $helper get-variant --file $flagFile --flag $Scenario).Trim()
if ($LASTEXITCODE -ne 0) { throw "Could not inspect feature flag $Scenario" }

$experimentId = [guid]::NewGuid().ToString()
$manifestDirectory = Join-Path $script:ProjectRoot ".runtime/experiments"
New-Item -ItemType Directory -Force -Path $manifestDirectory | Out-Null
$manifestFile = Join-Path $manifestDirectory "$experimentId.json"
$flagBackupFile = Join-Path $manifestDirectory "$experimentId.demo.flagd.json.backup"
$commit = Get-ConfigValue -Name "OTEL_DEMO_COMMIT" -Default "dedc0178918e260823323b8d95005a8cb924b007"

& $python $helper start-manifest `
    --file $manifestFile `
    --experiment-id $experimentId `
    --commit $commit `
    --profile $profile `
    --seed $Seed `
    --split $Split `
    --scenario $Scenario `
    --variant $Variant `
    --original-variant $originalVariant `
    --warmup $WarmupSeconds `
    --duration $DurationSeconds `
    --recovery $RecoverySeconds
if ($LASTEXITCODE -ne 0) { throw "Could not create experiment manifest" }

& $python $helper snapshot-file --file $flagFile --backup $flagBackupFile
if ($LASTEXITCODE -ne 0) { throw "Could not snapshot the original feature-flag file" }

# Stamp one UUID onto all three signals for the complete warmup/fault/recovery
# window. Recreating only the lightweight gateway avoids touching the demo.
$previousRunId = [Environment]::GetEnvironmentVariable("RCA_EXPERIMENT_RUN_ID")
$platformCompose = @(
    "compose", "--file", (Join-Path $script:ProjectRoot "docker-compose.yml")
)
$apiPort = Get-ConfigValue -Name "RCA_API_PORT" -Default "8000"
$apiUrl = "http://localhost:$apiPort"
$restored = $false
$completed = $false
$resetRecorded = $false
$runFailure = $null
$cleanupFailures = [System.Collections.Generic.List[string]]::new()
try {
    # The manifest requires an independent-run boundary. Restart every service,
    # including Locust, and verify complete running/healthy coverage before any
    # experiment row is registered or the warm-up clock starts.
    Write-Host "Resetting the complete Demo before capture seed $Seed..."
    Invoke-Docker -Arguments ($compose + @("restart"))
    Wait-DemoReady -ComposeArguments $compose
    & $python $helper mark-reset --file $manifestFile
    if ($LASTEXITCODE -ne 0) { throw "Could not record the successful Demo reset" }
    $resetRecorded = $true

    $env:RCA_EXPERIMENT_RUN_ID = $experimentId
    Invoke-Docker -Arguments ($platformCompose + @("up", "--detach", "--force-recreate", "otel-gateway"))
    & $python $helper sync-api --file $manifestFile --api-url $apiUrl
    if ($LASTEXITCODE -ne 0) { throw "Could not register experiment $experimentId" }

    Write-Host "Experiment ${experimentId}: healthy warm-up for ${WarmupSeconds}s..."
    Start-Sleep -Seconds $WarmupSeconds

    # Bound the intervention before touching flagd so no injected failures can
    # precede their recorded ground-truth interval.
    & $python $helper mark-manifest --file $manifestFile --status fault_active
    if ($LASTEXITCODE -ne 0) { throw "Could not mark the experiment fault interval start" }
    & $python $helper sync-api --file $manifestFile --api-url $apiUrl
    if ($LASTEXITCODE -ne 0) { throw "Could not update experiment fault state" }
    & $python $helper set-variant --file $flagFile --flag $Scenario --variant $Variant
    if ($LASTEXITCODE -ne 0) { throw "Could not enable $Scenario=$Variant" }
    # flagd's file provider watches the mounted JSON file and reloads it live.
    # A restart creates an unrelated flagd outage and contaminates RCA labels.
    Start-Sleep -Seconds 3
    Write-Host "Injected $Scenario=$Variant for ${DurationSeconds}s. Built-in Locust traffic remains active."
    Start-Sleep -Seconds $DurationSeconds

    # The recovery boundary is the beginning of the restoration intervention,
    # not the later moment at which the flagd restart finishes.
    & $python $helper mark-manifest --file $manifestFile --status recovering
    if ($LASTEXITCODE -ne 0) { throw "Could not mark the experiment recovery start" }
    & $python $helper sync-api --file $manifestFile --api-url $apiUrl
    if ($LASTEXITCODE -ne 0) { throw "Could not persist experiment fault interval" }
    & $python $helper set-variant --file $flagFile --flag $Scenario --variant $originalVariant
    if ($LASTEXITCODE -ne 0) { throw "Could not restore $Scenario" }
    Start-Sleep -Seconds 3
    $restored = $true
    Write-Host "Restored $Scenario=$originalVariant; observing recovery for ${RecoverySeconds}s..."
    Start-Sleep -Seconds $RecoverySeconds

    Write-Host "Validating persisted telemetry and top-$AcceptanceTopK root-cause ranking..."
    & $python $helper accept-capture `
        --file $manifestFile `
        --api-url $apiUrl `
        --top-k $AcceptanceTopK
    if ($LASTEXITCODE -ne 0) {
        throw "Experiment $experimentId failed the capture acceptance gate"
    }
    & $python $helper mark-manifest --file $manifestFile --status completed
    if ($LASTEXITCODE -ne 0) { throw "Could not mark the experiment completed" }
    & $python $helper sync-api --file $manifestFile --api-url $apiUrl
    if ($LASTEXITCODE -ne 0) { throw "Could not finalize experiment lineage" }
    $completed = $true
    Write-Host "Experiment phases complete; restoring the exact pre-run state..."
} catch {
    $runFailure = $_
} finally {
    # Always restore the exact original bytes. set-variant intentionally writes
    # JSON atomically, but a semantic restoration alone may change line endings
    # or mode bits and is not a faithful checkout restoration.
    try {
        & $python $helper restore-file --file $flagFile --backup $flagBackupFile
        if ($LASTEXITCODE -ne 0) { throw "restore-file exited with code $LASTEXITCODE" }
    } catch {
        $message = "Could not restore the exact feature-flag snapshot: $($_.Exception.Message)"
        $cleanupFailures.Add($message)
        Write-Warning $message
    }

    if (-not $restored) {
        try {
            & docker @compose restart flagd *> $null
            if ($LASTEXITCODE -ne 0) { throw "docker restart exited with code $LASTEXITCODE" }
        } catch {
            $message = "Could not restart flagd after emergency restoration: $($_.Exception.Message)"
            $cleanupFailures.Add($message)
            Write-Warning $message
        }
    }
    if (-not $completed) {
        try {
            & $python $helper mark-manifest --file $manifestFile --status aborted
            if ($LASTEXITCODE -ne 0) { throw "mark-manifest exited with code $LASTEXITCODE" }
        } catch {
            $message = "Could not mark the interrupted experiment as aborted: $($_.Exception.Message)"
            $cleanupFailures.Add($message)
            Write-Warning $message
        }
        if ($resetRecorded) {
            try {
                & $python $helper sync-api --file $manifestFile --api-url $apiUrl
                if ($LASTEXITCODE -ne 0) { throw "sync-api exited with code $LASTEXITCODE" }
            } catch {
                $message = "Could not persist the aborted experiment state: $($_.Exception.Message)"
                $cleanupFailures.Add($message)
                Write-Warning $message
            }
        }
    }
    try {
        if ([string]::IsNullOrWhiteSpace($previousRunId)) {
            Remove-Item Env:RCA_EXPERIMENT_RUN_ID -ErrorAction SilentlyContinue
        } else {
            $env:RCA_EXPERIMENT_RUN_ID = $previousRunId
        }
    } catch {
        $message = "Could not restore RCA_EXPERIMENT_RUN_ID: $($_.Exception.Message)"
        $cleanupFailures.Add($message)
        Write-Warning $message
    }
    try {
        & docker @platformCompose up --detach --force-recreate otel-gateway *> $null
        if ($LASTEXITCODE -ne 0) { throw "docker compose exited with code $LASTEXITCODE" }
    } catch {
        $message = "Could not restore the gateway's pre-experiment run ID: $($_.Exception.Message)"
        $cleanupFailures.Add($message)
        Write-Warning $message
    }
}

if ($cleanupFailures.Count -gt 0) {
    $detail = $cleanupFailures -join "; "
    if ($null -ne $runFailure) {
        $detail = "$detail; original experiment failure: $($runFailure.Exception.Message)"
    }
    throw "Experiment cleanup failed: $detail"
}
if ($null -ne $runFailure) {
    throw $runFailure
}

Write-Host "Experiment complete. Ground truth: $manifestFile"
$grafanaPort = Get-ConfigValue -Name "GRAFANA_PORT" -Default "3000"
Write-Host "Review the incident at http://localhost:$grafanaPort/d/rca-overview"
