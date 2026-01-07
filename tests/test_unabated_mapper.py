import pytest
from datetime import datetime, timezone
from nhl_bets.scrapers.unabated_client import UnabatedClient

def test_unabated_parse_snapshot_basic():
    client = UnabatedClient()
    
    # Tiny synthetic Unabated JSON snippet
    data = {
        "people": {
            "12345": {"firstName": "Connor", "lastName": "McDavid"}
        },
        "marketSources": [
            {"id": 73, "name": "Pinnacle", "statusId": 1}
        ],
        "teams": {
            "726": {"abbreviation": "EDM"},
            "753": {"abbreviation": "VAN"}
        },
        "odds": {
            "lg6:pt1:pregame": [
                {
                    "eventId": "999",
                    "personId": 12345,
                    "betTypeId": 70,  # POINTS
                    "eventName": "EDM @ VAN",
                    "eventStart": "2026-01-07T00:00:00Z",
                    "statusId": 1,
                    "eventTeams": {
                        "1": {"id": 726},
                        "0": {"id": 753}
                    },
                    "sides": {
                        "si1:pid12345": {
                            "ms73": {"points": 1.5, "americanPrice": -110}
                        },
                        "si0:pid12345": {
                            "ms73": {"points": 1.5, "americanPrice": -120}
                        }
                    }
                }
            ]
        }
    }
    
    capture_ts = datetime(2026, 1, 6, 12, 0, 0, tzinfo=timezone.utc)
    parsed = client.parse_snapshot(data, "raw/path", "hash123", capture_ts)
    records = parsed["odds"]
    
    assert len(records) == 2
    
    # Check Over
    over = next(r for r in records if r["side"] == "OVER")
    assert over["player_name_raw"] == "Connor McDavid"
    assert over["market_type"] == "POINTS"
    assert over["line"] == 1.5
    assert over["odds_american"] == -120
    assert over["book_name_raw"] == "Pinnacle"
    assert over["vendor_bet_type_id"] == 70
    assert over["vendor_outcome_key"] == "si0:pid12345"
    assert over["vendor_market_source_id"] == "73"
    assert round(over["odds_decimal"], 4) == 1.8333
    
    # Check Under
    under = next(r for r in records if r["side"] == "UNDER")
    assert under["odds_american"] == -110
    assert under["vendor_outcome_key"] == "si1:pid12345"
    assert round(under["odds_decimal"], 4) == 1.9091

def test_unabated_goals_filtering():
    client = UnabatedClient()
    # GOALS market (129) with only one side should be skipped
    data = {
        "people": {"123": {"firstName": "A", "lastName": "B"}},
        "marketSources": [{"id": 1, "name": "BK"}],
        "teams": {},
        "odds": {
            "lg6:pt1:pregame": [
                {
                    "eventId": "1",
                    "personId": 123,
                    "betTypeId": 129,
                    "sides": {
                        "si1:pid123": {"ms1": {"points": 0.5, "americanPrice": 150}}
                    }
                }
            ]
        }
    }
    parsed = client.parse_snapshot(data, "p", "h", datetime.now())
    assert len(parsed["odds"]) == 0
