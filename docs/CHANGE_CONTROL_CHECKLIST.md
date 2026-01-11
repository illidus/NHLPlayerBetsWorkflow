# Change Control Checklist

**Copy this checklist into your Pull Request or Change Issue.**

## 1. Impact Assessment
- [ ] **Does this change Production Behavior (Gen 3)?**
    - [ ] Yes (Requires Regression Gate + Leaderboard Entry)
    - [ ] No (Refactoring, Docs, Experimental)
- [ ] **Does this touch LOCKED files?**
    - `run_production_pipeline.py`, `produce_game_context.py`, `single_game_probs.py`
    - [ ] No
    - [ ] Yes (Justification: ______________________)

## 2. Validation
- [ ] **Profile Check:** Verified `production_profile` still loads and runs?
- [ ] **Baseline Check:** Verified `baseline_profile` still runs?
- [ ] **Tests:** Ran `pytest`?
- [ ] **Docs:** Updated `DOCS_INDEX.md` or `SYSTEM_HISTORY_AND_ROLLBACK.md`?

## 3. Evidence (For Model Changes)
- [ ] **Run Manifest:** Link to `outputs/runs/run_manifest_*.json`
- [ ] **Log Loss Delta:** Old: ______ vs New: ______
- [ ] **Leaderboard:** Added entry to `MASTER_BACKTEST_LEADERBOARD.md`?

## 4. Rollback Plan
- [ ] **Soft:** Can we just switch profiles?
- [ ] **Hard:** Do we need to revert git commit?
