# Phase 13 — Player Identity Resolution (Name-Based) for NHL Player Props

## 0. Context and Motivation
Phase 12 proved The-Odds-API returns NHL player prop markets (e.g., player_points, player_goals) for real sportsbooks, but outcomes do not include stable vendor player IDs; only player names are present. Therefore, ROI-grade ingestion requires a deterministic, auditable player-name resolution layer.

This phase introduces:
- Canonical player ID mapping via roster-constrained name resolution
- Persistent alias registry (vendor name -> canonical player)
- Confidence scoring and strict gating for ROI-grade inserts
- A repeatable manual review loop for unresolved names
- Audit artifacts to quantify match quality and drift

Non-goals:
- Perfect identity for all historical edge cases in v1
- Multi-sport/general identity framework (NHL-only first)

## 1. Target Outcomes (Acceptance)
A Phase 13 run on a 1-event sample should produce:
- `fact_prop_odds` populated with ROI-grade rows where `player_resolve_conf >= 0.90`
- Remaining rows go to `stg_prop_odds_unresolved` with explicit reasons
- Deterministic results across reruns (idempotent, reproducible)
- Artifacts:
  - `audit_player_resolution.md` (match rate, confidence distribution, top failures)
  - `audit_unresolved_reasons.md` (counts by failure reason + samples)
- Unit tests verifying:
  - exact match, alias match, fuzzy match (unique), and ambiguous cases
  - deterministic outputs given a fixed roster snapshot

## 2. Data Contract Updates
### 2.1 Normalized row fields (provider output -> canonical)
Add/ensure the normalized odds row includes:
- `event_id_vendor` (already present)
- `home_team_raw`, `away_team_raw`
- `player_name_raw` (from outcome.description; or equivalent)
- `market_type_raw` (provider market key)
- `side` (Over/Under)
- `line` (numeric; provider point)
- `odds_decimal` (provider price)
- `odds_american` (derived; optional but recommended)
- `book_id_vendor` (provider bookmaker key)
- `capture_ts_utc`, `source_vendor`

### 2.2 New canonical identity fields
For resolved rows:
- `player_id_canonical` (FK to dim_players)
- `player_resolve_method` (EXACT, ALIAS, FUZZY, MANUAL)
- `player_resolve_conf` (0.00–1.00)
- `player_resolve_notes` (optional)

## 3. Schema Additions
### 3.1 dim_players (if not already suitable)
Ensure `dim_players` exists with stable canonical identifiers.
Suggested minimum:
- `player_id` (PK, internal)
- `full_name`
- `first_name`
- `last_name`
- `shoots` (optional)
- `position` (optional)
- `active_from_season`, `active_to_season` (optional)

### 3.2 dim_player_alias
Purpose: durable mapping of vendor-presented names to canonical players.
Columns:
- `alias_id` (PK)
- `source_vendor` (e.g., THE_ODDS_API)
- `alias_text_raw`
- `alias_text_norm` (normalized)
- `canonical_player_id` (FK dim_players)
- `team_abbrev` (optional but recommended)
- `season` (optional but recommended)
- `match_method` (EXACT/ALIAS/FUZZY/MANUAL)
- `match_confidence`
- `created_ts_utc`, `updated_ts_utc`

Uniqueness:
- `(source_vendor, alias_text_norm, team_abbrev, season)` unique when populated;
  otherwise `(source_vendor, alias_text_norm)` unique.

### 3.3 stg_player_alias_review_queue
Purpose: queue unresolved/ambiguous names for manual mapping.
Columns:
- `queue_id` (PK)
- `source_vendor`
- `alias_text_raw`, `alias_text_norm`
- `event_id_vendor`, `game_start_ts_utc`
- `home_team_raw`, `away_team_raw`
- `candidate_players_json` (list of candidates + scores)
- `decision_status` (PENDING/RESOLVED/REJECTED)
- `created_ts_utc`, `resolved_ts_utc`
- `resolved_canonical_player_id` (nullable)
- `resolution_notes` (nullable)

## 4. Identity Resolution Algorithm (Deterministic)
### 4.1 Inputs
- `player_name_raw` from vendor outcome (e.g., "Alex Ovechkin")
- Event context:
  - home/away teams and start time
  - event_id_vendor
- Team rosters for the specific game date (or nearest snapshot)

### 4.2 Name normalization (must be deterministic)
`normalize_name(s)`:
- Unicode NFKD, strip diacritics
- lowercase
- remove punctuation (.,'’-), collapse whitespace
- remove suffix tokens: jr, sr, ii, iii, iv
- return normalized string

### 4.3 Candidate set restriction (critical)
**ROI-Grade Requirement:**
Only consider players on:
- home team roster OR away team roster
for that event date.
This reduces false positives dramatically and makes fuzzy matching safe.
If roster snapshot is missing, resolution MUST FAIL (unless exploration flag is set).

### 4.4 Resolution cascade
1) Alias table hit:
   - lookup `(source_vendor, alias_text_norm, team_abbrev?, season?)`
   - if found => resolved
   - conf = stored confidence (usually 1.00 for manual, >=0.90 for prior auto)

2) Exact full-name match within candidate set:
   - if exactly one match => resolved
   - conf = 1.00, method EXACT

3) Exact last-name match within candidate set (only if unique):
   - if last name appears once => resolved
   - conf = 0.90, method EXACT_LAST (optional; keep if safe)

4) Fuzzy match within candidate set:
   - compute similarity (Jaro-Winkler or Levenshtein ratio)
   - take best candidate
   - if score >= 0.93 and margin to 2nd-best >= 0.03 => resolved
   - conf = score (clamped to <= 0.95), method FUZZY

5) Otherwise:
   - unresolved (AMBIGUOUS_PLAYER or PLAYER_NOT_FOUND or MISSING_ROSTER_SNAPSHOT)
   - enqueue in `stg_player_alias_review_queue`

### 4.5 Confidence gating for ROI-grade
- ROI-grade requires `player_resolve_conf >= 0.90`
- Anything below goes to unresolved or (optional) a separate exploration table

## 5. Roster Source Strategy
### 5.1 Recommended approach
Maintain a roster snapshot table keyed by team + date:
- `dim_team_roster_snapshot(team_abbrev, snapshot_date, roster_json)`
where `roster_json` contains canonical player IDs and normalized names.

Populate via:
- existing project data sources (preferred), or
- NHL public endpoints (acceptable) but cache snapshots to avoid rate limits.

### 5.2 Caching and determinism
- Snapshots must be stored and versioned (immutable per date) to ensure reproducibility.
- Resolver must never call live roster APIs during “analysis/backtest runs” without caching.

## 6. Odds Format Normalization (per-book)
The-Odds-API payloads may return decimal odds.
Implement:
- `dim_book_odds_rules(book_id_vendor, odds_format_default, notes)`
During normalization:
- parse `price` into `odds_decimal` if > 1.0
- compute `odds_american` deterministically:
  - if decimal >= 2.0: american = round((decimal - 1) * 100)
  - else: american = round(-100 / (decimal - 1))
Store both; use american for downstream ROI computations.

## 7. Operational Flow
1) Ingest raw payloads (Phase 12)
2) Normalize rows (Phase 12)
3) Resolve players (Phase 13)
4) Split:
   - resolved and ROI-grade => `fact_prop_odds`
   - unresolved => `stg_prop_odds_unresolved` + review queue
5) Emit audits

## 8. Audit Artifacts
### 8.1 audit_player_resolution.md
Include:
- total rows attempted
- % resolved (any confidence)
- % ROI-grade resolved (conf >= 0.90)
- confidence histogram (bins)
- top 20 unresolved names
- top failure reasons (counts)

### 8.2 audit_unresolved_reasons.md
Explode `failure_reasons` and report counts + 10 samples per top reason.
Must include raw fields: book_id_vendor, market_type_raw, player_name_raw, event_id_vendor.

## 9. Tests (Minimum)
Unit tests for resolver:
- exact match within roster
- alias hit overrides fuzzy
- fuzzy match unique passes threshold
- ambiguous last name fails and enqueues
- player not found enqueues with PLAYER_NOT_FOUND
- determinism: same inputs produce identical outputs
- **Strict Mode:** missing roster -> FAIL (MISSING_ROSTER_SNAPSHOT)

Integration test:
- run provider ingestion in mock mode + fixed roster snapshot
- assert non-zero ROI-grade rows
- assert specific known player resolves correctly

## 10. Go / No-Go Gates
GO if on a 1-event real run:
- >= 80% of rows resolve with conf >= 0.90 OR
- >= 60% resolve conf >= 0.90 with clear path to improve via alias queue

NO-GO if:
- roster snapshots cannot be built reliably
- or resolution ambiguity is persistently high (>20% ambiguous) even with roster restriction

## 11. Risks and Mitigations
- Risk: Name collisions (e.g., same last name on team)
  - Mitigation: roster restriction + fuzzy margin rule + manual queue
- Risk: Trades/late call-ups causing roster mismatch
  - Mitigation: snapshot at game date; allow “nearest snapshot within 3 days”
- Risk: Historical roster data gaps
  - Mitigation: store team-season rosters; fallback to season roster if date roster missing
- Risk: Silent drift
  - Mitigation: daily resolution audit thresholds and alerts in run summary
