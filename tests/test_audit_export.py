import pytest
import pandas as pd
import os
from datetime import datetime

EXPORT_PATH = 'outputs/ev_analysis/MultiBookBestBets.xlsx'

def test_export_columns_exist():
    """Verify that provenance columns exist in the export."""
    if not os.path.exists(EXPORT_PATH):
        pytest.skip("Export file not found")
        
    df = pd.read_excel(EXPORT_PATH)
    required_cols = [
        'source_vendor', 
        'capture_ts_utc', 
        'raw_payload_hash', 
        'mu', 
        'distribution', 
        'alpha'
    ]
    for col in required_cols:
        assert col in df.columns, f"Missing column: {col}"

def test_deduplication():
    """Verify that rows are unique by the reporting key."""
    if not os.path.exists(EXPORT_PATH):
        pytest.skip("Export file not found")
        
    df = pd.read_excel(EXPORT_PATH)
    dedup_key = ['Player', 'Market', 'Line', 'Side', 'Book']
    
    duplicates = df[df.duplicated(subset=dedup_key, keep=False)]
    assert duplicates.empty, f"Found {len(duplicates)} duplicates in export"

def test_sog_alpha_populated():
    """Verify that SOG markets have an alpha value."""
    if not os.path.exists(EXPORT_PATH):
        pytest.skip("Export file not found")
        
    df = pd.read_excel(EXPORT_PATH)
    sog_bets = df[df['Market'] == 'SOG']
    
    if not sog_bets.empty:
        # Check if alpha is not null/empty for SOG
        assert sog_bets['alpha'].notna().all(), "Some SOG bets missing alpha"
        # Check value is roughly 0.35
        assert (sog_bets['alpha'] == 0.35).all(), "SOG alpha should be 0.35"
