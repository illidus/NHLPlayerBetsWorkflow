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
$LogFile = "outputs/logs/nightly_healthcheck_$Timestamp.log"

Write-Host "Starting Nightly Healthcheck (Dev Mode)..."
Write-Host "Logging to: $LogFile"

try {
    # Run with --dev as requested for nightly
    python scripts/nightly_model_healthcheck.py --dev *>&1 | Tee-Object -FilePath $LogFile
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Nightly Healthcheck PASSED." -ForegroundColor Green
    } else {
        Write-Host "Nightly Healthcheck FAILED. Exit Code: $LASTEXITCODE" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Error "Execution failed: $_"
    exit 1
}
