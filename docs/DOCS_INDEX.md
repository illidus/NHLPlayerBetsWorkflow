# Docs Index & Inventory

**Status:** Active
**Maintainer:** Governance Agent

This index tracks the status of all documentation to ensure the repository remains a "Source of Truth" without losing historical context.

## 🟢 Canonical (Active & Trusted)
These documents define the current system behavior. If code contradicts these, it is a bug.

| Document | Purpose |
| :--- | :--- |
| **[START_HERE.md](START_HERE.md)** | **Entrypoint.** The single map of the vault. |
| **[SYSTEM_HISTORY_AND_ROLLBACK.md](SYSTEM_HISTORY_AND_ROLLBACK.md)** | **Truth.** Timeline of generations, active profiles, and rollback commands. |
| **[PRODUCTION_TRUTH_TABLE.md](PRODUCTION_TRUTH_TABLE.md)** | **Config.** Detailed mapping of Gen 3 vs Gen 2 behavior/files. |
| **[MODEL_PROJECTION_THEORY.md](MODEL_PROJECTION_THEORY.md)** | **Math.** The statistical constitution (NB vs Poisson, etc.). |
| **[LEADERBOARD_GOVERNANCE.md](LEADERBOARD_GOVERNANCE.md)** | **Rules.** How to register official backtests. |
| **[OPERATIONS_MODEL_REFRESH.md](OPERATIONS_MODEL_REFRESH.md)** | **SOP.** How to run nightly/weekly maintenance. |
| **[ARTIFACTS_INDEX.md](ARTIFACTS_INDEX.md)** | **Map.** Where to find key outputs (Leaderboard, Calibrators). |

## 🟡 Experimental / In-Progress
Documents describing features not yet in `production_experiment_b`.

| Document | Status |
| :--- | :--- |
| `phase11_historical_odds/PHASE11_IMPLEMENTATION.md` | **Active Dev.** Odds ingestion logic. |
| `phase12_odds_api/OPERATIONS.md` | **Draft.** Future integration plan. |

## 🔴 Historical / Superseded
Do not rely on these for current operations. They are preserved for context.

| Document | Status | Replacement |
| :--- | :--- | :--- |
| `docs/archive/MODEL_PROJECTION_THEORY_PREGEN3.md` | **Archived.** Gen 2 Theory. | [MODEL_PROJECTION_THEORY.md](MODEL_PROJECTION_THEORY.md) |
| `docs/refactor_plan.md` | **Completed.** | N/A |
| `docs/PRODUCTION_MODEL_JUSTIFICATION.md` | **Superseded** by Leaderboard. | [MASTER_BACKTEST_LEADERBOARD.md](../outputs/eval/MASTER_BACKTEST_LEADERBOARD.md) |

---

## 🛡️ Governance Rules
1.  **Do not delete** old docs; move them to `archive/` or tag them as `[STATUS: HISTORICAL]`.
2.  **Update this index** when adding new core documentation.
