import pytest
import pandas as pd
from datetime import datetime, timezone
from src.nhl_bets.ingestion.unabated import UnabatedIngestor

def test_unabated_parsing():
    # Mock Data
    mock_payload = {
        "people": {
            "p1": {"firstName": "Connor", "lastName": "McDavid"},
            "p2": {"firstName": "Auston", "lastName": "Matthews"}
        },
        "marketSources": [
            {"id": 1, "name": "DraftKings"},
            {"id": 2, "name": "FanDuel"}
        ],
        "teams": {
            "t1": {"abbreviation": "EDM"},
            "t2": {"abbreviation": "TOR"}
        },
        "odds": {
            "lg6:pt1:pregame": [
                {
                    "eventId": "e1",
                    "betTypeId": 70, # POINTS
                    "personId": "p1",
                    "eventStart": "2026-01-07T20:00:00Z",
                    "eventTeams": {"1": {"id": "t1"}, "0": {"id": "t2"}},
                    "sides": {
                        "si1:p1": { # Over
                            "ms1": {"points": 1.5, "americanPrice": -120},
                            "ms2": {"points": 1.5, "americanPrice": -115}
                        },
                        "si0:p1": { # Under
                            "ms1": {"points": 1.5, "americanPrice": 100}
                        }
                    }
                },
                {
                    "eventId": "e1",
                    "betTypeId": 129, # GOALS
                    "personId": "p2",
                    "eventStart": "2026-01-07T20:00:00Z",
                     "eventTeams": {"1": {"id": "t1"}, "0": {"id": "t2"}},
                    "sides": {
                        "si1:p2": { # Over
                            "ms1": {"points": 0.5, "americanPrice": 150}
                        }
                    }
                }
            ]
        }
    }
    
    ingestor = UnabatedIngestor()
    ts = datetime.now(timezone.utc)
    
    df = ingestor.parse_snapshot(mock_payload, "mock/path.json", "hash123", ts)
    
    assert len(df) == 4
    
    mcdavid = df[df['player_name_raw'] == 'Connor McDavid']
    assert len(mcdavid) == 3
    
    matthews = df[df['player_name_raw'] == 'Auston Matthews']
    assert matthews.iloc[0]['market_type'] == 'GOALS'
    
    # Check teams
    assert mcdavid.iloc[0]['home_team'] == 'EDM'
    assert mcdavid.iloc[0]['away_team'] == 'TOR'
