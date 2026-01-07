from datetime import datetime, timezone

from scripts.analysis.unabated_ui_reconcile import format_db_timestamp_utc


def test_format_db_timestamp_utc_naive_is_utc():
    naive = datetime(2026, 1, 7, 0, 0, 0)
    assert format_db_timestamp_utc(naive) == "2026-01-07T00:00:00+00:00"


def test_format_db_timestamp_utc_aware_is_utc():
    aware = datetime(2026, 1, 7, 0, 0, 0, tzinfo=timezone.utc)
    assert format_db_timestamp_utc(aware) == "2026-01-07T00:00:00+00:00"
