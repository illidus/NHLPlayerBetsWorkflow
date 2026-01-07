EV Forensics Checklist (10-minute review)
=========================================

Use this checklist to diagnose a single high-EV ASSISTS/POINTS bet.
This is diagnostic only. No model or policy changes.

1) Identify the bet
------------------
- Player, market, line, side, book.
- Capture timestamp and event start time (UTC).

2) Confirm line mapping
-----------------------
- Odds line is x.5 (0.5, 1.5, ...).
- Model line uses integer thresholds.
- Expected mapping: model line = odds line + 0.5.
- If mismatch, tag as LINE_MISMATCH.

3) Confirm probability source
-----------------------------
- ASSISTS/POINTS use calibrated probabilities when available.
- Record `p_over_calibrated` and `p_over`.

4) Check implied odds consistency
---------------------------------
- Compute implied_prob = 1 / odds_decimal.
- Verify EV% = (Model_Prob * odds_decimal) - 1.
- If EV is large, implied_prob should be extreme.

5) Tail sanity (Poisson)
------------------------
- Recompute Poisson tail at the same line using mu_used.
- Compare to stored probability (should be close).
- Large deviations indicate data join issues.

6) Calibration plateau check
----------------------------
- Identify the calibrated bucket value.
- Check if multiple high-EV bets share the same bucket.
- Clustering suggests isotonic plateau effect.

7) Book dispersion check
------------------------
- Compare implied probs across books for the same prop.
- If one book is a clear outlier, label as OUTLIER_BOOK.

8) Mapping sanity
-----------------
- Confirm player and event mapping exist (canonical IDs).
- Missing mappings can cause false joins.

9) Freshness sanity
-------------------
- Ensure capture_ts is near prob snapshot.
- Stale odds can inflate EV when markets have moved.

10) Final classification
------------------------
- Legitimate pricing anomaly (math checks out, multi-book agreement).
- Calibration/tail artifact (plateau or rare tail).
- Line/side mismatch or mapping error.
- Single-book outlier.
