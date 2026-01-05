# NHL Player Prop Bets Documentation Hub

This directory serves as the centralized repository for all non-canonical documentation, experiment logs, audits, and accuracy reports.

## Directory Structure

- `_handover/`: Context and handoff notes for new sessions or agents.
- `experiments/`: Detailed logs of model changes, window testing, and distribution experiments.
- `audits/`: Point-in-time snapshots of model behavior and EV calculation traces.
- `accuracy/`: Performance metrics (Log Loss, Brier, ECE) from historical backtests.
- `meta/`: Reports on data integrity, duplicates, and system health.

## Governance Note

**Canonical Logic Lives Outside This Folder.**
All authoritative model math, rolling window definitions, and distributional constraints are defined in:
1. `GEMINI.md` (Root)
2. `docs/MODEL_PROJECTION_THEORY.md`

Do not treat files within `/docs/` as the source of truth for current implementation; they are historical records and evidence artifacts.

## Checklist: Files Safe to Send to ChatGPT / External Reviewers

The following files are sanitized for external review and provide the necessary context for model troubleshooting without exposing proprietary infrastructure:

- [ ] `docs/README.md` (This file)
- [ ] `docs/MODEL_PROJECTION_THEORY.md` (Methodology overview)
- [ ] `docs/accuracy/forecast_accuracy.md` (Performance stats)
- [ ] `docs/experiments/*.md` (Experiment results)
- [ ] `docs/audits/*.md` (Specific game audits)
