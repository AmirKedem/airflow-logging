param(
    [ValidateSet("api", "workers", "scheduler", "airflow", "infra", "all")]
    [string]$Target = "airflow",

    [switch]$Recreate,

    [switch]$Build
)

$ErrorActionPreference = "Stop"

$serviceMap = @{
    api = @("airflow-apiserver")
    workers = @("airflow-worker", "airflow-triggerer")
    scheduler = @("airflow-scheduler", "airflow-dag-processor")
    airflow = @(
        "airflow-apiserver",
        "airflow-scheduler",
        "airflow-dag-processor",
        "airflow-triggerer",
        "airflow-worker"
    )
    infra = @("postgres", "redis", "elasticsearch")
    all = @(
        "postgres",
        "redis",
        "elasticsearch",
        "airflow-apiserver",
        "airflow-scheduler",
        "airflow-dag-processor",
        "airflow-triggerer",
        "airflow-worker"
    )
}

$services = $serviceMap[$Target]
if (-not $services) {
    throw "Unknown target: $Target"
}

$upArgs = @("compose", "up", "-d")
if ($Recreate) {
    $upArgs += "--force-recreate"
}
if ($Build) {
    $upArgs += "--build"
}
$upArgs += $services

Write-Host "Restart target: $Target"
Write-Host "Services: $($services -join ', ')"
Write-Host "Command: docker $($upArgs -join ' ')"

docker @upArgs
