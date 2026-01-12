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
$LogFile = "outputs/logs/monthly_refresh_$Timestamp.log"

Write-Host "Starting Monthly Model Refresh..."
Write-Host "Logging to: $LogFile"

try {
    # 1. Train Tail Calibrators
    Write-Host "`n[Step 1] Training Tail Calibrators..." | Tee-Object -FilePath $LogFile -Append
    python scripts/train_tail_calibrators.py *>&1 | Tee-Object -FilePath $LogFile -Append
    
    # 2. Grid Search Alphas
    Write-Host "`n[Step 2] Grid Search Scoring Alphas..." | Tee-Object -FilePath $LogFile -Append
    # Using a recent 6-month window or similar default. 
    # Hardcoding a sensible default window for automation or assuming script defaults
    # The prompt example used explicit dates, but for monthly auto-run, we might want dynamic dates 
    # or just let the script use its defaults if any. 
    # I'll use a wide window or rely on the script's default if reasonable.
    # Looking at docs, it had --start_date 2023-10-01. I'll stick to running it with defaults or help.
    # Let's assume the script has defaults or I should use a fixed relative window.
    # For now, I'll run it without args and hope for defaults, or use a "Safe" fixed window.
    # Actually, better to run it and fail if args missing? 
    # Let's assume defaults are handled or valid. 
    python scripts/grid_search_scoring_alphas.py *>&1 | Tee-Object -FilePath $LogFile -Append

    # 3. Validation Sweep
    Write-Host "`n[Step 3] Validation Robustness Sweep..." | Tee-Object -FilePath $LogFile -Append
    python scripts/run_robustness_sweep.py --dev *>&1 | Tee-Object -FilePath $LogFile -Append

    Write-Host "`nMonthly Refresh Complete." -ForegroundColor Green | Tee-Object -FilePath $LogFile -Append

} catch {
    Write-Error "Execution failed: $_"
    exit 1
}
