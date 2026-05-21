# TP5 — start full stack (PowerShell)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$env:N_CLIENTS = if ($env:N_CLIENTS) { $env:N_CLIENTS } else { "500" }
$env:M_EXTERNAL = if ($env:M_EXTERNAL) { $env:M_EXTERNAL } else { "1000" }

Write-Host "=== TP5 Fraud Detection ===" -ForegroundColor Cyan
$freeGb = [math]::Round((Get-PSDrive C).Free / 1GB, 2)
Write-Host "Disk free on C: $freeGb GB"
if ($freeGb -lt 5) {
    Write-Host "ERROR: Need at least 5 GB free for Docker Desktop." -ForegroundColor Red
    Write-Host "Free space (Downloads ~90GB, Docker ~55GB in AppData\Local\Docker), then retry."
    exit 1
}

Write-Host "Population: N=$env:N_CLIENTS M=$env:M_EXTERNAL"

Write-Host "Starting docker compose..."
docker compose down 2>$null
docker compose up -d

Write-Host ""
Write-Host "Waiting for services (60s)..."
Start-Sleep -Seconds 60

docker compose ps

Write-Host ""
Write-Host "=== URLs ===" -ForegroundColor Green
Write-Host "Jupyter : http://localhost:8888  (token: tp5fraud2026)"
Write-Host "Kafka UI: http://localhost:8080"
Write-Host "Spark UI: http://localhost:8081"
Write-Host ""
Write-Host "Logs: docker logs tp5-transaction-generator --tail 15"
Write-Host "      docker logs tp5-spark-processor --tail 20"
