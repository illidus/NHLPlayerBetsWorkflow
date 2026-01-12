$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

# Venv detection
$VenvPaths = @(".venv", "venv", "env")
$VenvFound = $false
foreach ($Path in $VenvPaths) {
    if (Test-Path "$Path\Scripts\Activate.ps1") {
        Write-Host "Activating venv at $Path"
        & "$Path\Scripts\Activate.ps1"
        $VenvFound = $true
        break
    }
}

if (-not $VenvFound) {
    Write-Warning "No venv found. Assuming Python is in PATH or global."
}

# Ensure logs dir
New-Item -ItemType Directory -Force -Path "outputs/logs" | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile = "outputs/logs/weekly_robustness_$Timestamp.log"

Write-Host "Starting Weekly Robustness Sweep (Full)..."
Write-Host "Logging to: $LogFile"

try {
    # Run full sweep (no --dev flag) with primary_lines scope
    python scripts/run_robustness_sweep.py --scope primary_lines *>&1 | Tee-Object -FilePath $LogFile
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Weekly Sweep PASSED." -ForegroundColor Green
    } else {
        Write-Host "Weekly Sweep FAILED. Exit Code: $LASTEXITCODE" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Error "Execution failed: $_"
    exit 1
}
