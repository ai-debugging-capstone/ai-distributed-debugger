[CmdletBinding()]
param()

. (Join-Path $PSScriptRoot "Common.ps1")
Assert-Command -Name "docker"

$directory = Get-DemoDirectory
if (-not (Test-Path -LiteralPath (Join-Path $directory "compose.yaml"))) {
    Write-Host "No fetched demo checkout found; nothing to stop."
    exit 0
}

Set-DemoEnvironment
$profile = Get-DemoProfile
$compose = Get-DemoComposeArguments -Profile $profile -DemoDirectory $directory
Invoke-Docker -Arguments ($compose + @("down", "--remove-orphans"))

Write-Host "Stopped the demo. RCA PostgreSQL and Grafana data were preserved."
Write-Host "Run docker compose -f $script:ProjectRoot/docker-compose.yml down to stop the product stack too."
