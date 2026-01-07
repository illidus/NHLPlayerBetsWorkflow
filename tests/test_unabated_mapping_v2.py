import pytest
import json
from datetime import datetime, timezone
from src.nhl_bets.scrapers.unabated_client import UnabatedClient

def test_unabated_parse_snapshot_v2():
    client = UnabatedClient()
    
    # Mock data based on typical Unabated structure
    mock_data = {
        "people": {
            "12345": {"firstName": "Auston", "lastName": "Matthews"}
        },
        "marketSources": [
            {"id": 73, "name": "DraftKings", "statusId": 1}
        ],
        "teams": {
            "726": {"abbreviation": "TOR"},
            "753": {"abbreviation": "MTL"}
        },
        "odds": {
            "lg6:pt1:pregame": [
                {
                    "betTypeId": 86, # SOG
                    "personId": 12345,
                    "eventId": 98765,
                    "eventStart": "2026-01-06T19:00:00Z",
                    "eventName": "Maple Leafs @ Canadiens",
                    "statusId": 1,
                    "eventTeams": {
                        "1": {"id": 726},
                        "0": {"id": 753}
                    },
                    "sides": {
                        "si1:pid12345": {
                            "ms73": {"points": 3.5, "americanPrice": -110}
                        },
                        "si0:pid12345": {
                            "ms73": {"points": 3.5, "americanPrice": -110}
                        }
                    },
                    "live": False
                }
            ]
        }
    }
    
    capture_ts = datetime.now(timezone.utc)
    raw_path = "mock/path.json"
    raw_hash = "mockhash"
    
    parsed = client.parse_snapshot(mock_data, raw_path, raw_hash, capture_ts)
    
    assert "odds" in parsed
    assert "events" in parsed
    assert "players" in parsed
    
    odds = parsed["odds"]
    assert len(odds) == 2 # Over and Under
    
    for row in odds:
        assert row["vendor_event_id"] == "98765"
        assert row["vendor_person_id"] == "12345"
        assert row["event_id_vendor"] == "98765"
        assert row["player_id_vendor"] == "12345"
        assert row["home_team"] == "TOR"
        assert row["away_team"] == "MTL"
        assert row["player_name_raw"] == "Auston Matthews"
        
    events = parsed["events"]
    assert len(events) == 1
    assert events[0]["vendor_event_id"] == "98765"
    assert events[0]["home_team"] == "TOR"
    assert events[0]["away_team"] == "MTL"
    
    players = parsed["players"]
    assert len(players) == 1
    assert players[0]["vendor_person_id"] == "12345"
    assert players[0]["player_name"] == "Auston Matthews"

if __name__ == "__main__":
    test_unabated_parse_snapshot_v2()
    print("Test passed!")
