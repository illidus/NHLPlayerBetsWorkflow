import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from nhl_bets.analysis.runner_duckdb import normalize_team


def test_normalize_team_abbr():
    assert normalize_team("Boston Bruins") == "BOS"
    assert normalize_team("bos") == "BOS"
