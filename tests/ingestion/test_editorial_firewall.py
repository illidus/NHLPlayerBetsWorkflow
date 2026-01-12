import pytest
from odds_archive import parsers, config

def test_is_nhl_content_positive():
    # Team names
    assert parsers.is_nhl_content("The Maple Leafs are favored tonight.") is True
    assert parsers.is_nhl_content("Bruins game is huge.") is True
    assert parsers.is_nhl_content("Utah HC is the new team.") is True
    
def test_is_nhl_content_negative():
    # Negative keywords
    assert parsers.is_nhl_content("Bet on the MLS game tonight.") is False
    assert parsers.is_nhl_content("NBA finals are approaching.") is False
    assert parsers.is_nhl_content("Touchdown scorers for Sunday.") is False
    
def test_is_nhl_content_mixed():
    # Negative overrides positive
    assert parsers.is_nhl_content("The Maple Leafs play in the MLS stadium.") is False

def test_is_nhl_content_neutral():
    # No keywords -> False (Ambiguous/Strict Phase 11)
    assert parsers.is_nhl_content("The game is tonight at 7pm.") is False

def test_status_determination():
    # Test internal helper if accessible or via parser
    # Accessing private helper for unit test is okay-ish or duplicate logic
    from odds_archive.parsers import _determine_status
    
    assert _determine_status(None, "DraftKings") == ("MISSING_ODDS", "No odds found in snippet")
    assert _determine_status(100, None) == ("MISSING_BOOK", "No bookmaker attributed")
    assert _determine_status(100, "FanDuel") == ("CANDIDATE_READY", None)
