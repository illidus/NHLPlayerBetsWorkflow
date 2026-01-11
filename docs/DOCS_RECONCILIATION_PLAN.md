# Documentation Reconciliation Plan

**Objective:** Synchronize documentation with the "Generation 3" production architecture (Jan 2026) while preserving historical records.

---

## 1. Inventory & Status

| Document | Path | Status | Action Required |
| :--- | :--- | :--- | :--- |
| **Model Theory** | `docs/MODEL_PROJECTION_THEORY.md` | **Conflict** | **CRITICAL:** Update Section 2 to reflect `all_nb` (Negative Binomial) usage in Prod. Currently claims Poisson for Goals/Assists/Points. |
| **Production Profile** | `config/production_profile.json` | **Truth** | None. This is the source of truth. |
| **Refactor Plan** | `docs/refactor_plan.md` | Legacy | Mark as completed/historical. |
| **Ops Refresh** | `docs/OPERATIONS_MODEL_REFRESH.md` | Outdated | Update with `run_production_pipeline.py --profile` commands. |
| **Gemini Context** | `GEMINI.md` | Partial | Update "Phase 11" section to reference new Leaderboard and Rollback docs. |
| **Audit Guide** | `docs/AUDIT_AND_VERIFICATION_GUIDE.md` | Valid | Keep. Add note about "Tail Bucket" calibration checking. |

---

## 2. Reconciliation Tasks (Prioritized)

### Priority 1: Fix Theoretical Contradictions
The most dangerous risk is a developer reading `MODEL_PROJECTION_THEORY.md` and assuming Poisson distributions for Goals, while the code runs Negative Binomial.

*   **Task:** Update `docs/MODEL_PROJECTION_THEORY.md`.
    *   *Change:* Explicitly state that while Poisson is the *theoretical base*, Production Gen 3 uses Negative Binomial (`all_nb`) to handle over-dispersion.
    *   *Add:* Section on "Tail Bucket Calibration" (currently missing).

### Priority 2: Standardize Operations
*   **Task:** Update `docs/OPERATIONS_MODEL_REFRESH.md`.
    *   Replace manual script calls with the `run_production_pipeline.py` wrapper.
    *   Document how to switch profiles (Production vs. Research).

### Priority 3: Archive Legacy Plans
*   **Task:** Move `refactor_plan.md` and `phase11_*.md` (once complete) to `docs/archive/` or tag them clearly as [IMPLEMENTED].

---

## 3. Preservation Strategy
**DO NOT DELETE** old theory files.
*   Retain `docs/archive/MODEL_PROJECTION_THEORY_OLD.md` as Gen 1 reference.
*   If major changes occur to `MODEL_PROJECTION_THEORY.md`, create `docs/archive/MODEL_PROJECTION_THEORY_GEN2.md` before overwriting.

---

## 4. Execution Plan
1.  **Immediate:** User to approve this plan.
2.  **Step A:** Update `MODEL_PROJECTION_THEORY.md` to match `production_profile.json`.
3.  **Step B:** Create `docs/PRODUCTION_TRUTH_TABLE.md` (See separate deliverable).
4.  **Step C:** Update `GEMINI.md` with pointers to the new Truth Table.
