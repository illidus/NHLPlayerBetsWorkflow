from datetime import date

from nhl_bets.scrapers.oddsshark_client import build_synthetic_event_id


def test_build_synthetic_event_id_deterministic():
    game_date = date(2026, 1, 7)
    event_id = build_synthetic_event_id(game_date, "ANA", "WSH")
    assert event_id == "ODDSSHARK_20260107_ANA_WSH"


def test_build_synthetic_event_id_normalizes_names():
    game_date = date(2026, 1, 7)
    event_id = build_synthetic_event_id(game_date, "Anaheim Ducks", "Washington Capitals")
    assert event_id == "ODDSSHARK_20260107_ANA_WSH"
