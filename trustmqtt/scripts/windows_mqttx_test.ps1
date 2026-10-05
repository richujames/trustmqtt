param(
    [ValidateSet('hivemq', 'flashmq')]
    [string]$Broker = 'hivemq',
    [string]$ClientId = 'windows-mqttx-01',
    [int]$Duration = 75,
    [int]$ExpectedPublishes = 1
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw 'Docker Desktop is not running. Start Docker Desktop and run this command again.'
}

if ($Broker -eq 'hivemq') {
    $HostPort = 1885
    docker compose stop flashmq mosquitto | Out-Host
    docker compose --profile hivemq up -d hivemq redis postgres tmq-worker | Out-Host
} else {
    $HostPort = 1884
    docker compose stop hivemq mosquitto | Out-Host
    docker compose --profile flashmq up -d flashmq redis postgres tmq-worker | Out-Host
}
if ($LASTEXITCODE -ne 0) {
    throw "The $Broker Docker stack did not start successfully."
}

Write-Host ''
Write-Host 'MQTTX Desktop connection' -ForegroundColor Cyan
Write-Host "  Name:        TrustMQTT $Broker"
Write-Host '  Host:        localhost'
Write-Host "  Port:        $HostPort"
Write-Host '  Protocol:    MQTT 5.0'
Write-Host "  Client ID:   $ClientId"
Write-Host '  Credentials: leave username and password empty'
Write-Host '  Clean Start: enabled'
Write-Host '  Keep Alive:  30 seconds'
Write-Host "  Topic:       fleet/$ClientId/telemetry/temperature"
Write-Host '  QoS:         1'
Write-Host ''

& "$ProjectRoot\.venv\Scripts\python.exe" `
    "$PSScriptRoot\watch_mqttx_desktop.py" `
    --broker $Broker `
    --client-id $ClientId `
    --duration $Duration `
    --expected-publishes $ExpectedPublishes
exit $LASTEXITCODE
