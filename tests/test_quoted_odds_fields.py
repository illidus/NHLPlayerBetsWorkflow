from datetime import datetime

from nhl_bets.scrapers.playnow_adapter import PlayNowAdapter


def test_playnow_preserves_quoted_decimal_odds():
    adapter = PlayNowAdapter()
    payload = {
        "data": {
            "events": [
                {
                    "id": 123,
                    "name": "Away Team @ Home Team",
                    "startTime": "2026-01-07T00:00:00Z",
                    "markets": [
                        {
                            "id": 456,
                            "name": "Player Total Points",
                            "handicapValue": 0.5,
                            "outcomes": [
                                {
                                    "id": 789,
                                    "name": "Over",
                                    "prices": [{"decimal": 1.9}],
                                }
                            ],
                        }
                    ],
                }
            ]
        }
    }

    records = adapter.parse_event_details(
        payload,
        raw_path="outputs/odds/raw/PLAYNOW/sample.json",
        raw_hash="hash",
        capture_ts=datetime.utcnow(),
    )

    assert records
    record = records[0]
    assert record["odds_decimal"] == 1.9
    assert record["odds_quoted_raw"] == "1.9"
    assert record["odds_quoted_format"] == "decimal"
    assert record["odds_decimal_derived"] is False
    assert record["odds_american_derived"] is True
