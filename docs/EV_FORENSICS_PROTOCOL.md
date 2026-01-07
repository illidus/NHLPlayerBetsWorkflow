EV Forensics Protocol (ASSISTS/POINTS)
=====================================

Purpose
-------
Provide a diagnostic-only, reproducible protocol to explain very high EV values
for calibrated markets (ASSISTS, POINTS). This is a forensic workflow and does
not change model logic, calibration, or pricing rules.

Scope
-----
- Markets: ASSISTS, POINTS (calibrated only).
- Data sources: `fact_prop_odds`, `fact_probabilities`, mapping tables.
- Output: diagnostics and explanations, not model changes.

Non-Negotiables
---------------
- Do not alter distributions, mu formulas, feature windows, or calibration.
- Do not cap EV or reweight books.
- No ROI optimization or policy changes.

Hypotheses to Test
-----------------
1) Line/side misalignment (e.g., odds line 0.5 vs model line 1).
2) Tail behavior for rare events (Poisson tails).
3) Calibration plateau effects (isotonic buckets).
4) Single-book outliers vs multi-book agreement.
5) Stale or sparse data in specific segments.

Key Mapping Rule (Line Alignment)
---------------------------------
Odds lines for ASSISTS/POINTS are half-integers (0.5, 1.5, ...).
Model lines in `fact_probabilities` are integer thresholds.

Mapping rule:
- odds line 0.5 -> model line 1
- odds line 1.5 -> model line 2
- general: `line_int = line + 0.5` when line is x.5

Diagnostics (Step Order)
------------------------

Step 1 - High-EV Triage (Top 50)
--------------------------------
Goal: identify patterns in markets, lines, books, and calibrated probabilities.

Note for live runs:
If `fact_probabilities` is not current for the slate, use the latest
`outputs/projections/SingleGamePropProbabilities.csv` and join by normalized
player name (same logic as `runner_duckdb.py`). This preserves production
alignment while keeping the SQL diagnostics as the canonical reference.

Diagnostic SQL (non-production, bounded):
```sql
-- DIAGNOSTIC / NON-PRODUCTION
SET memory_limit = '8GB';
SET threads = 8;
SET temp_directory = './duckdb_temp/';

WITH odds AS (
    SELECT
        o.*,
        pm.canonical_player_id AS player_id,
        CAST(em.canonical_game_id AS BIGINT) AS game_id,
        CASE
            WHEN o.line IS NOT NULL AND (o.line - floor(o.line)) = 0.5
                THEN CAST(o.line + 0.5 AS BIGINT)
            ELSE CAST(o.line AS BIGINT)
        END AS line_int
    FROM fact_prop_odds o
    LEFT JOIN dim_players_mapping pm
        ON o.player_name_raw = pm.vendor_player_name
       AND o.source_vendor = pm.source_vendor
    LEFT JOIN dim_events_mapping em
        ON o.event_id_vendor = em.vendor_event_id
       AND o.source_vendor = em.source_vendor
    WHERE o.market_type IN ('ASSISTS','POINTS')
),
latest_odds AS (
    SELECT * FROM (
        SELECT
            o.*,
            ROW_NUMBER() OVER (
                PARTITION BY source_vendor, book_name_raw, player_name_raw,
                             market_type, line, side, event_id_vendor
                ORDER BY capture_ts_utc DESC
            ) AS rn
        FROM odds o
    )
    WHERE rn = 1
)
SELECT
    l.player_name_raw AS player,
    l.market_type AS market,
    l.line,
    l.side,
    COALESCE(p.p_over_calibrated, p.p_over) AS p_over_selected,
    CASE WHEN p.p_over_calibrated IS NULL THEN 'RAW' ELSE 'CALIBRATED' END AS prob_source,
    l.book_name_raw AS book,
    l.odds_decimal,
    l.odds_american,
    (1.0 / l.odds_decimal) AS implied_prob,
    ((CASE WHEN l.side = 'OVER' THEN COALESCE(p.p_over_calibrated, p.p_over)
           ELSE 1 - COALESCE(p.p_over_calibrated, p.p_over) END) * l.odds_decimal) - 1 AS ev_pct,
    l.capture_ts_utc,
    l.event_start_time_utc
FROM latest_odds l
LEFT JOIN fact_probabilities p
    ON p.player_id = l.player_id
   AND p.game_id = l.game_id
   AND p.market = l.market_type
   AND p.line = l.line_int
WHERE l.odds_decimal IS NOT NULL
ORDER BY ev_pct DESC
LIMIT 50;
```

Interpretation:
- Look for repeated calibrated probabilities or repeated books.
- Check whether extreme EVs align to outlier implied odds.

Step 2 - Line & Side Alignment Check
------------------------------------
Goal: confirm model line/side corresponds to odds line/side.

Diagnostic SQL (non-production, bounded):
```sql
-- DIAGNOSTIC / NON-PRODUCTION
WITH odds AS (
    SELECT
        o.*,
        pm.canonical_player_id AS player_id,
        CAST(em.canonical_game_id AS BIGINT) AS game_id,
        CASE
            WHEN o.line IS NOT NULL AND (o.line - floor(o.line)) = 0.5
                THEN CAST(o.line + 0.5 AS BIGINT)
            ELSE CAST(o.line AS BIGINT)
        END AS line_int
    FROM fact_prop_odds o
    LEFT JOIN dim_players_mapping pm
        ON o.player_name_raw = pm.vendor_player_name
       AND o.source_vendor = pm.source_vendor
    LEFT JOIN dim_events_mapping em
        ON o.event_id_vendor = em.vendor_event_id
       AND o.source_vendor = em.source_vendor
    WHERE o.market_type IN ('ASSISTS','POINTS')
),
latest_odds AS (
    SELECT * FROM (
        SELECT
            o.*,
            ROW_NUMBER() OVER (
                PARTITION BY source_vendor, book_name_raw, player_name_raw,
                             market_type, line, side, event_id_vendor
                ORDER BY capture_ts_utc DESC
            ) AS rn
        FROM odds o
    )
    WHERE rn = 1
)
SELECT
    CASE
        WHEN p.line IS NULL THEN 'NO_PROB_MATCH'
        WHEN p.line = l.line_int THEN 'LINE_MATCH'
        ELSE 'LINE_MISMATCH'
    END AS line_status,
    COUNT(*) AS record_count
FROM latest_odds l
LEFT JOIN fact_probabilities p
    ON p.player_id = l.player_id
   AND p.game_id = l.game_id
   AND p.market = l.market_type
   AND p.line = l.line_int
GROUP BY 1;
```

Interpretation:
- Any LINE_MISMATCH in top EVs is a primary artifact source.
- NO_PROB_MATCH indicates missing mapping or missing probabilities.

Step 3 - Tail Probability Sanity (Poisson)
------------------------------------------
Goal: recompute Poisson tail at the same line and compare to stored `p_over`.

Diagnostic SQL (non-production, bounded to line_int 1..3):
```sql
-- DIAGNOSTIC / NON-PRODUCTION
WITH probs AS (
    SELECT
        player_id,
        game_id,
        market,
        line,
        mu_used,
        p_over
    FROM fact_probabilities
    WHERE market IN ('ASSISTS','POINTS')
      AND line IN (1,2,3)
)
SELECT
    *,
    CASE
        WHEN line = 1 THEN 1 - exp(-mu_used)
        WHEN line = 2 THEN 1 - exp(-mu_used) * (1 + mu_used)
        WHEN line = 3 THEN 1 - exp(-mu_used) * (1 + mu_used + (mu_used * mu_used / 2.0))
        ELSE NULL
    END AS p_over_recalc,
    ABS(p_over - CASE
        WHEN line = 1 THEN 1 - exp(-mu_used)
        WHEN line = 2 THEN 1 - exp(-mu_used) * (1 + mu_used)
        WHEN line = 3 THEN 1 - exp(-mu_used) * (1 + mu_used + (mu_used * mu_used / 2.0))
        ELSE NULL
    END) AS abs_dev
FROM probs
ORDER BY abs_dev DESC
LIMIT 50;
```

Interpretation:
- Large deviations indicate data join issues or a different probability source.
- Minor deviations are expected from numerical precision.

Step 4 - Calibration Plateau Audit
----------------------------------
Goal: determine if high EVs cluster in a single calibrated bucket.

Diagnostic SQL (non-production, bounded):
```sql
-- DIAGNOSTIC / NON-PRODUCTION
WITH top_ev AS (
    SELECT
        l.player_name_raw AS player,
        l.market_type AS market,
        l.line,
        l.side,
        COALESCE(p.p_over_calibrated, p.p_over) AS p_over_selected,
        p.p_over_calibrated,
        l.odds_decimal,
        ((CASE WHEN l.side = 'OVER' THEN COALESCE(p.p_over_calibrated, p.p_over)
               ELSE 1 - COALESCE(p.p_over_calibrated, p.p_over) END) * l.odds_decimal) - 1 AS ev_pct
    FROM fact_prop_odds l
    LEFT JOIN dim_players_mapping pm
        ON l.player_name_raw = pm.vendor_player_name
       AND l.source_vendor = pm.source_vendor
    LEFT JOIN dim_events_mapping em
        ON l.event_id_vendor = em.vendor_event_id
       AND l.source_vendor = em.source_vendor
    LEFT JOIN fact_probabilities p
        ON p.player_id = pm.canonical_player_id
       AND p.game_id = CAST(em.canonical_game_id AS BIGINT)
       AND p.market = l.market_type
       AND p.line = CASE
            WHEN l.line IS NOT NULL AND (l.line - floor(l.line)) = 0.5
                THEN CAST(l.line + 0.5 AS BIGINT)
            ELSE CAST(l.line AS BIGINT)
         END
    WHERE l.market_type IN ('ASSISTS','POINTS')
      AND l.odds_decimal IS NOT NULL
    ORDER BY ev_pct DESC
    LIMIT 50
)
SELECT
    round(p_over_calibrated, 6) AS p_over_calibrated_bucket,
    COUNT(*) AS count_high_ev,
    AVG(ev_pct) AS avg_ev
FROM top_ev
WHERE p_over_calibrated IS NOT NULL
GROUP BY 1
ORDER BY count_high_ev DESC;
```

Interpretation:
- Clustering in a single bucket indicates isotonic plateau effect.
- This is expected in sparse regions and is not a defect by itself.

Step 5 - Book Dispersion Lens
-----------------------------
Goal: determine if high EV is driven by a single outlier book.

Diagnostic SQL (non-production, bounded):
```sql
-- DIAGNOSTIC / NON-PRODUCTION
WITH odds AS (
    SELECT
        o.*,
        (1.0 / o.odds_decimal) AS implied_prob
    FROM fact_prop_odds o
    WHERE o.market_type IN ('ASSISTS','POINTS')
      AND o.odds_decimal IS NOT NULL
),
grouped AS (
    SELECT
        player_name_raw,
        market_type,
        line,
        side,
        event_id_vendor,
        COUNT(DISTINCT book_name_raw) AS book_count,
        MIN(implied_prob) AS implied_min,
        MAX(implied_prob) AS implied_max,
        median(implied_prob) AS implied_median
    FROM odds
    GROUP BY 1,2,3,4,5
)
SELECT *
FROM grouped
ORDER BY (implied_max - implied_min) DESC
LIMIT 50;
```

Interpretation:
- A large implied spread indicates book divergence.
- If top EV is only on one book while others disagree, treat as outlier risk.

Interpretation Guidance
-----------------------
- High EV is credible only if line alignment is correct and book dispersion is not extreme.
- If Poisson tail recomputation matches, the EV is mathematically consistent.
- Calibration plateaus can create jumpy EV in sparse bins; treat as an explanation, not a defect.

What We Will Not Do
-------------------
- No model changes (distributions, mu, windows, calibration).
- No EV caps or thresholds changes.
- No book reweighting or exclusion policy changes.
- No ROI optimization or tuning.
