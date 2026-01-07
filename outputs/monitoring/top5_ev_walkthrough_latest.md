# Top 5 EV Walkthrough Report - 2026-01-06

This report provides a forensic walkthrough of the top 5 bets by EV% as of the latest run.

## Selection Summary
- **Source File:** `outputs/ev_analysis/MultiBookBestBets.xlsx`
- **Snapshot Anchor:** 2026-01-07T07:03:25.062819+00:00
- **Total Rows Evaluated:** 25

---

## Bet 1: Tyler Kleven (OTT) - GOALS 0.5 OVER

### A) Bet Identity
- **Player / Team:** Tyler Kleven / OTT
- **Market / Line / Side:** GOALS / 0.5 / OVER
- **Book, source_vendor:** PlayNow, PLAYNOW
- **Odds:** 1600 (American), **Implied_Prob:** 5.9%, **Model_Prob:** 11.4%, **EV%:** +93.5%
- **capture_ts_utc:** 2026-01-07 07:03:23.126463
- **prob_snapshot_ts:** 2026-01-07T07:03:25.062819+00:00
- **raw_payload_hash:** 2d2420e0a22418bfc99e26f8a4068235229a6ee66c06bb0214e5e222c8872e4a
- **Prob_Source:** Raw, **Source_Col:** p_G_1plus

### B) Odds Provenance
- **Query:** `SELECT book_name_raw, source_vendor, capture_ts_utc, market_type, line, side, odds_american FROM fact_prop_odds WHERE raw_payload_hash = '2d2420e0a22418bfc99e26f8a4068235229a6ee66c06bb0214e5e222c8872e4a' AND player_name_raw = 'Tyler Kleven'`
- **Record Found:** `PlayNow | PLAYNOW | 2026-01-07 07:03:23.126463 | GOALS | 0.5 | OVER | 1600`

### C) Probability Derivation
1) **Parameters:** mu = 0.1208, distribution = Poisson, alpha = N/A
2) **Threshold mapping:** `k = 1`.
3) **Raw distribution probability:** 0.11379
4) **Calibration:** None
5) **Reconciliation:** Diff=0.00001 (MATCH)

### D) EV Calculation
1) **Convert odds:** American 1600 -> Decimal = 17.000
2) **EV:** `0.1138 * 17.000 - 1 = +93.44%`

### E) Sensitivity
- **EV_low (P-0.02):** +59.44%
- **Verdict:** **ROBUST**

---

## Bet 2: Joe Veleno (MTL) - GOALS 0.5 OVER

### A) Bet Identity
- **Player / Team:** Joe Veleno / MTL
- **Market / Line / Side:** GOALS / 0.5 / OVER
- **Book, source_vendor:** PlayNow, PLAYNOW
- **Odds:** 900 (American), **Implied_Prob:** 10.0%, **Model_Prob:** 18.3%, **EV%:** +83.4%
- **capture_ts_utc:** 2026-01-07 07:03:23.126463
- **prob_snapshot_ts:** 2026-01-07T07:03:25.062819+00:00
- **raw_payload_hash:** 2d2420e0a22418bfc99e26f8a4068235229a6ee66c06bb0214e5e222c8872e4a
- **Prob_Source:** Raw, **Source_Col:** p_G_1plus

### B) Odds Provenance
- **Query:** `SELECT book_name_raw, source_vendor, capture_ts_utc, market_type, line, side, odds_american FROM fact_prop_odds WHERE raw_payload_hash = '2d2420e0a22418bfc99e26f8a4068235229a6ee66c06bb0214e5e222c8872e4a' AND player_name_raw = 'Joe Veleno'`
- **Record Found:** `PlayNow | PLAYNOW | 2026-01-07 07:03:23.126463 | GOALS | 0.5 | OVER | 900`

### C) Probability Derivation
1) **Parameters:** mu = 0.2026, distribution = Poisson, alpha = N/A
2) **Threshold mapping:** `k = 1`.
3) **Raw distribution probability:** 0.18340
4) **Calibration:** None
5) **Reconciliation:** Diff=0.00000 (MATCH)

### D) EV Calculation
1) **Convert odds:** American 900 -> Decimal = 10.000
2) **EV:** `0.1834 * 10.000 - 1 = +83.40%`

### E) Sensitivity
- **EV_low (P-0.02):** +63.40%
- **Verdict:** **ROBUST**

---

## Bet 3: Igor Chernyshov (SJS) - GOALS 0.5 OVER

### A) Bet Identity
- **Player / Team:** Igor Chernyshov / SJS
- **Market / Line / Side:** GOALS / 0.5 / OVER
- **Book, source_vendor:** Novig, UNABATED
- **Odds:** 378 (American), **Implied_Prob:** 20.9%, **Model_Prob:** 37.2%, **EV%:** +77.8%
- **capture_ts_utc:** 2026-01-07 07:02:30.149836
- **prob_snapshot_ts:** 2026-01-07T07:03:25.062819+00:00
- **raw_payload_hash:** e0cd7ff1c4ee9eb30d5d01d243e91f006be636a5ce0c4c1ce725a32e0d417094
- **Prob_Source:** Raw, **Source_Col:** p_G_1plus

### B) Odds Provenance
- **Query:** `SELECT book_name_raw, source_vendor, capture_ts_utc, market_type, line, side, odds_american FROM fact_prop_odds WHERE raw_payload_hash = 'e0cd7ff1c4ee9eb30d5d01d243e91f006be636a5ce0c4c1ce725a32e0d417094' AND player_name_raw = 'Igor Chernyshov'`
- **Record Found:** `Novig | UNABATED | 2026-01-07 07:02:30.149836 | GOALS | 0.5 | OVER | 378`

### C) Probability Derivation
1) **Parameters:** mu = 0.4650, distribution = Poisson, alpha = N/A
2) **Threshold mapping:** `k = 1`.
3) **Raw distribution probability:** 0.37186
4) **Calibration:** None
5) **Reconciliation:** Diff=0.00004 (MATCH)

### D) EV Calculation
1) **Convert odds:** American 378 -> Decimal = 4.780
2) **EV:** `0.3719 * 4.780 - 1 = +77.75%`

### E) Sensitivity
- **EV_low (P-0.02):** +68.19%
- **Verdict:** **ROBUST**

---

## Bet 4: Fabian Zetterlund (OTT) - GOALS 0.5 OVER

### A) Bet Identity
- **Player / Team:** Fabian Zetterlund / OTT
- **Market / Line / Side:** GOALS / 0.5 / OVER
- **Book, source_vendor:** Sports Interaction Logo, ODDSSHARK
- **Odds:** 425 (American), **Implied_Prob:** 19.0%, **Model_Prob:** 33.0%, **EV%:** +73.4%
- **capture_ts_utc:** 2026-01-07 07:03:00.141698
- **prob_snapshot_ts:** 2026-01-07T07:03:25.062819+00:00
- **raw_payload_hash:** 54114c323ac970e527d5faa7d53313bbe9791c0ce93d3c65f44f240d3ed95a42
- **Prob_Source:** Raw, **Source_Col:** p_G_1plus

### B) Odds Provenance
- **Query:** `SELECT book_name_raw, source_vendor, capture_ts_utc, market_type, line, side, odds_american FROM fact_prop_odds WHERE raw_payload_hash = '54114c323ac970e527d5faa7d53313bbe9791c0ce93d3c65f44f240d3ed95a42' AND player_name_raw = 'Fabian Zetterlund'`
- **Record Found:** `Sports Interaction Logo | ODDSSHARK | 2026-01-07 07:03:00.141698 | GOALS | 0.5 | OVER | 425`

### C) Probability Derivation
1) **Parameters:** mu = 0.4009, distribution = Poisson, alpha = N/A
2) **Threshold mapping:** `k = 1`.
3) **Raw distribution probability:** 0.33028
4) **Calibration:** None
5) **Reconciliation:** Diff=0.00002 (MATCH)

### D) EV Calculation
1) **Convert odds:** American 425 -> Decimal = 5.250
2) **EV:** `0.3303 * 5.250 - 1 = +73.40%`

### E) Sensitivity
- **EV_low (P-0.02):** +62.90%
- **Verdict:** **ROBUST**

---

## Bet 5: Fabian Zetterlund (OTT) - GOALS 0.5 OVER

### A) Bet Identity
- **Player / Team:** Fabian Zetterlund / OTT
- **Market / Line / Side:** GOALS / 0.5 / OVER
- **Book, source_vendor:** Novig, UNABATED
- **Odds:** 418 (American), **Implied_Prob:** 19.3%, **Model_Prob:** 33.0%, **EV%:** +71.1%
- **capture_ts_utc:** 2026-01-07 07:02:30.149836
- **prob_snapshot_ts:** 2026-01-07T07:03:25.062819+00:00
- **raw_payload_hash:** e0cd7ff1c4ee9eb30d5d01d243e91f006be636a5ce0c4c1ce725a32e0d417094
- **Prob_Source:** Raw, **Source_Col:** p_G_1plus

### B) Odds Provenance
- **Query:** `SELECT book_name_raw, source_vendor, capture_ts_utc, market_type, line, side, odds_american FROM fact_prop_odds WHERE raw_payload_hash = 'e0cd7ff1c4ee9eb30d5d01d243e91f006be636a5ce0c4c1ce725a32e0d417094' AND player_name_raw = 'Fabian Zetterlund'`
- **Record Found:** `Novig | UNABATED | 2026-01-07 07:02:30.149836 | GOALS | 0.5 | OVER | 418`

### C) Probability Derivation
1) **Parameters:** mu = 0.4009, distribution = Poisson, alpha = N/A
2) **Threshold mapping:** `k = 1`.
3) **Raw distribution probability:** 0.33028
4) **Calibration:** None
5) **Reconciliation:** Diff=0.00002 (MATCH)

### D) EV Calculation
1) **Convert odds:** American 418 -> Decimal = 5.180
2) **EV:** `0.3303 * 5.180 - 1 = +71.09%`

### E) Sensitivity
- **EV_low (P-0.02):** +60.73%
- **Verdict:** **ROBUST**

---
