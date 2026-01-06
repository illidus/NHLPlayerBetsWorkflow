# Long-Run Procedure: Gemini CLI (or Codex CLI) for Phase 11
**Last updated:** 2026-01-05

This is an operator procedure for running a long, multi-hour refactor/implementation safely.

---

## 1) Recommended tool choice
### Gemini CLI + Gemini 3 Flash (recommended for long-run tasks)
Use Gemini 3 Flash when you want:
- fast iterative progress
- frequent checkpoint commits
- good doc generation + code scaffolding

### Codex CLI (recommended when you want tighter code correctness)
Use Codex CLI when you want:
- stronger implementation discipline
- fewer “creative” jumps
- more consistent handling of large refactors

**Practical recommendation:** Start with Gemini 3 Flash to build the doc/architecture + scaffolding, then switch to Codex CLI for the vendor parsers and idempotent DuckDB inserts if Gemini shows fragility.

---

## 2) Safety workflow (non-negotiable)
1. Create a branch:
   - `git checkout -b phase11/historical-odds-ingestion`
2. Ensure .gitignore blocks:
   - `outputs/`
   - `data/raw/`
   - `data/db/*.duckdb`
   - any cookies/tokens/secrets
3. Commit early and often:
   - one logical unit per commit
4. If something goes wrong:
   - `git reset --hard <known_good_commit>`
   - or revert a specific commit: `git revert <sha>`

---

## 3) Agent execution model (how to let it run for hours)
### Use a constitution file
Make the agent treat this file as binding:
- `docs/phase11_historical_odds/PHASE11_IMPLEMENTATION.md`

### Use a long TODO list with verification
Make the agent follow:
- `docs/phase11_historical_odds/LONG_TODO_PHASE11.md`
- `docs/phase11_historical_odds/VERIFICATION_CHECKLIST.md`

### Enforce checkpoints
Instruct the agent to:
- write/update `docs/phase11_historical_odds/ARCHITECTURE.md` before coding
- run verification per subsection
- record commands and outputs in `docs/phase11_historical_odds/OPERATIONS.md`

---

## 4) “Mini ROI” handling (safe and compliant)
- ROI is **observational only** and must not gate merges.
- Require explicit disclaimer in output files.
- Flat stakes only; no threshold optimization.

---

## 5) End-to-end merge gate
Before merging Phase 11:
- Phase 10 golden validations PASS
- Unabated + PlayNow ingestion PASS
- OddsShark optional PASS or is disabled with documented reason
- EV report includes multi-book odds
- join coverage report produced
