import os

from nhl_bets.projections.config import get_production_prob_column


def _label_from_col(prob_col):
    return "Calibrated" if "calibrated" in prob_col else "Raw"


def test_debug_mode_forces_raw(monkeypatch):
    available = {
        "p_A_1plus": 0.2,
        "p_A_1plus_calibrated": 0.25,
        "p_PTS_1plus": 0.3,
        "p_PTS_1plus_calibrated": 0.35,
    }
    monkeypatch.setenv("DISABLE_CALIBRATION", "1")

    col_assists = get_production_prob_column("assists", 0.5, available.keys())
    col_points = get_production_prob_column("points", 0.5, available.keys())

    assert col_assists == "p_A_1plus"
    assert col_points == "p_PTS_1plus"
    assert _label_from_col(col_assists) == "Raw"
    assert _label_from_col(col_points) == "Raw"


def test_production_mode_respects_policy(monkeypatch):
    available = {
        "p_A_1plus": 0.2,
        "p_A_1plus_calibrated": 0.25,
        "p_PTS_1plus": 0.3,
        "p_PTS_1plus_calibrated": 0.35,
        "p_G_1plus": 0.1,
    }
    monkeypatch.setenv("DISABLE_CALIBRATION", "0")

    col_assists = get_production_prob_column("assists", 0.5, available.keys())
    col_points = get_production_prob_column("points", 0.5, available.keys())
    col_goals = get_production_prob_column("goals", 0.5, available.keys())

    assert col_assists == "p_A_1plus_calibrated"
    assert col_points == "p_PTS_1plus_calibrated"
    assert col_goals == "p_G_1plus"
    assert _label_from_col(col_assists) == "Calibrated"
    assert _label_from_col(col_points) == "Calibrated"
    assert _label_from_col(col_goals) == "Raw"
