from pathlib import Path
import sys

import duckdb
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from nhl_bets.analysis.side_integrity import (
    build_odds_side_lookup,
    normalize_side,
    resolve_odds_side,
)


TRACE_CSV = Path("outputs/backtesting/projection_trace_audit_rows_20260108_000058.csv")
DB_PATH = Path("data/db/nhl_backtest.duckdb")


@pytest.mark.parametrize(
    "player,market,line,book,odds_american,bet_side",
    [
        ("Adrian Kempe", "POINTS", 0.5, "PINNACLE LOGO", 158, "UNDER"),
        ("Kevin Fiala", "ASSISTS", 0.5, "SPORTS INTERACTION LOGO", -175, "UNDER"),
        ("Igor Chernyshov", "ASSISTS", 0.5, "PINNACLE LOGO", 240, "OVER"),
        ("Fabian Zetterlund", "GOALS", 0.5, "NOVIG", 418, "OVER"),
        ("Tyler Kleven", "GOALS", 0.5, "PLAYNOW", 1600, "OVER"),
    ],
)
def test_trace_rows_side_match(player, market, line, book, odds_american, bet_side):
    if not TRACE_CSV.exists():
        pytest.skip("Trace CSV missing; side integrity fixtures unavailable.")
    if not DB_PATH.exists():
        pytest.skip("DuckDB missing; side integrity fixtures unavailable.")

    con = duckdb.connect(str(DB_PATH))
    odds_df = con.execute(
        """
        SELECT
            player_name_raw,
            market_type,
            line,
            side,
            book_name_raw,
            odds_american
        FROM fact_prop_odds
        """
    ).df()
    con.close()

    lookup = build_odds_side_lookup(odds_df)
    odds_side, reason = resolve_odds_side(
        lookup,
        player,
        market,
        line,
        book,
        odds_american,
    )
    assert reason == "OK"
    assert normalize_side(bet_side) == odds_side
