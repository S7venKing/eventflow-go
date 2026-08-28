<#
.SYNOPSIS
    Step 11 - outbox worker concurrency benchmark.

.DESCRIPTION
    Runs ./cmd/outboxbench at 1, 2, 4 and 8 workers with everything else
    held fixed. Each level truncates and reseeds its own 1000 PENDING
    events, so no level inherits another's leftovers.

    Postgres must be reachable first:
        docker compose up -d postgres

.EXAMPLE
    ./benchmark/run_worker_bench.ps1

.EXAMPLE
    # Model a broker that takes 5ms per publish.
    ./benchmark/run_worker_bench.ps1 -PublishLatency 5ms

.EXAMPLE
    # Publish through the real Kafka broker from docker compose.
    docker compose up -d postgres kafka kafka-init
    ./benchmark/run_worker_bench.ps1 -Publisher kafka
#>

[CmdletBinding()]
param(
    [int[]]$WorkerLevels = @(1, 2, 4, 8),
    [int]$BatchSize = 10,
    [int]$Events = 1000,
    [string]$Interval = "50ms",
    [string]$PublishLatency = "0s",
    # inmemory = count-only publisher (the BEFORE baseline),
    # kafka    = real broker via internal/platform/kafka (the AFTER run).
    [ValidateSet("inmemory", "kafka")]
    [string]$Publisher = "inmemory",
    [string]$KafkaBrokers = "",
    [string]$KafkaTopic = "",
    [string]$Out = "",
    [string]$DatabaseUrl = ""
)

if ($Out -eq "") {
    $Out = "benchmark/results-worker-concurrency-$Publisher.md"
}

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (Test-Path $Out) {
    Write-Host "Removing previous results file: $Out"
    Remove-Item $Out
}

foreach ($workers in $WorkerLevels) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host " workers=$workers batch=$BatchSize events=$Events publisher=$Publisher" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan

    $benchArgs = @(
        "run", "./cmd/outboxbench",
        "-workers", $workers,
        "-batch", $BatchSize,
        "-events", $Events,
        "-interval", $Interval,
        "-publish-latency", $PublishLatency,
        "-publisher", $Publisher,
        "-out", $Out
    )

    if ($DatabaseUrl -ne "") {
        $benchArgs += @("-database-url", $DatabaseUrl)
    }

    if ($KafkaBrokers -ne "") {
        $benchArgs += @("-kafka-brokers", $KafkaBrokers)
    }

    if ($KafkaTopic -ne "") {
        $benchArgs += @("-kafka-topic", $KafkaTopic)
    }

    & go @benchArgs

    if ($LASTEXITCODE -ne 0) {
        throw "benchmark failed at workers=$workers (exit $LASTEXITCODE)"
    }
}

Write-Host ""
Write-Host "All levels complete. Results table:" -ForegroundColor Green
Get-Content $Out
