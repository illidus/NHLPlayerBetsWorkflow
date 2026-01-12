# Setting up Windows Task Scheduler for NHL Model

This guide details how to automate the NHL Player Bets model healthchecks using Windows Task Scheduler.

## Prerequisites
*   **Python Environment:** Ensure you have a working Python environment (venv) where `pip install -r requirements_backtesting.txt` has been run.
*   **PowerShell:** Ensure PowerShell execution policy allows scripts (or use `-ExecutionPolicy Bypass`).

## 1. Nightly Healthcheck (Daily)
**Goal:** Run quick verification and checks every night.

1.  Open **Task Scheduler**.
2.  Click **Create Task...**
3.  **General Tab:**
    *   Name: `NHL_Nightly_Healthcheck`
    *   Description: Runs unit tests and regression gate (Dev mode).
    *   Select "Run whether user is logged on or not" (optional, but recommended for servers).
    *   Configure for: Windows 10/11/Server 2019.
4.  **Triggers Tab:**
    *   New... -> Begin the task: On a schedule.
    *   Daily. Start: Tomorrow at **03:00:00**. Recur every: 1 days.
5.  **Actions Tab:**
    *   New... -> Action: Start a program.
    *   **Program/script:** `powershell.exe`
    *   **Add arguments:** `-ExecutionPolicy Bypass -File "C:\Path\To\Repo\scripts\windows_run_nightly_healthcheck.ps1"`
        *   *Replace `C:\Path\To\Repo` with your actual absolute path.*
    *   **Start in:** `C:\Path\To\Repo` (Important for relative paths!)
6.  **Conditions/Settings:** Adjust as needed (e.g., "Wake the computer to run this task").

## 2. Robustness Sweep (Weekly)
**Goal:** Run a full-history validation sweep.

1.  Create Task -> Name: `NHL_Weekly_Robustness`
2.  **Triggers:**
    *   Weekly. Start: Sunday at **04:00:00**.
    *   Recur every: 1 weeks on: Sunday.
3.  **Actions:**
    *   Program: `powershell.exe`
    *   Arguments: `-ExecutionPolicy Bypass -File "C:\Path\To\Repo\scripts\windows_run_weekly_robustness.ps1"`
    *   Start in: `C:\Path\To\Repo`

## 3. Model Refresh (Monthly)
**Goal:** Retrain calibrators and alphas.

1.  Create Task -> Name: `NHL_Monthly_Refresh`
2.  **Triggers:**
    *   Monthly. Start: 1st of month at **05:00:00**.
    *   Months: Select all. Days: 1.
3.  **Actions:**
    *   Program: `powershell.exe`
    *   Arguments: `-ExecutionPolicy Bypass -File "C:\Path\To\Repo\scripts\windows_run_monthly_refresh.ps1"`
    *   Start in: `C:\Path\To\Repo`

## Troubleshooting

*   **Exit Code 1:** Check the logs in `outputs/logs/` for details.
*   **"File not found":** Ensure "Start in" directory is set correctly in the Action.
*   **Permissions:** Ensure the user account running the task has write access to the `outputs/` folder.
