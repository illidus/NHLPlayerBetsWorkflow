# Security & Hygiene Review

## Secrets / Sensitive Data
- PlayNow client can consume a cookie from `PLAYNOW_COOKIE`; the code logs only presence, not the value (src/nhl_bets/scrapers/playnow_api_client.py L15-L30), mitigating accidental exposure. Raw payloads are written verbatim to `outputs/odds/raw/`, but current responses do not embed credentials.
- No credential material is persisted to DuckDB tables; only odds and metadata are stored.

## Network Hardening
- All new network calls set explicit timeouts: Unabated `(10s connect, 30s read)`, PlayNow `15s`, OddsShark `(10s connect, 30s read)`.
- Retry policy: Unabated and OddsShark use tenacity with max 3 attempts and exponential backoff; PlayNow calls have no retry/backoff beyond requests' single attempt.
- No global rate limiting; repeated ingestion runs could hit vendor throttling.

## Repository Hygiene
- `.gitignore` excludes `data/db/`, `outputs/odds/raw/`, and other generated artifacts (root .gitignore L1-L33), reducing risk of committing payloads or DuckDB files.
- Raw payloads are checksumed (`.sha256`) but not encrypted; treat `outputs/odds/raw/` as non-public and avoid sharing.

## Recommended Safeguards
- Add retries/backoff to PlayNow fetches to mirror other vendors and avoid transient failures.
- Consider redacting or validating payloads before writing to disk if upstream ever includes session identifiers.
- Keep CI/commit hooks to prevent accidental inclusion of `outputs/` or `data/db` despite .gitignore (e.g., via pre-commit rules).
