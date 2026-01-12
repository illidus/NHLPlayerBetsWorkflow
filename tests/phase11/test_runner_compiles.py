import py_compile
import pytest
import os

def test_runner_compiles():
    """
    Ensures that the Phase 11 runner script is syntactically valid.
    This prevents accidental IndentationError regressions.
    """
    script_path = os.path.join("pipelines", "phase11_historical_odds", "run_phase11_historical_odds.py")
    
    # Check existence
    assert os.path.exists(script_path), f"Runner script not found at {script_path}"
    
    # Compile with raise exception
    try:
        py_compile.compile(script_path, doraise=True)
    except py_compile.PyCompileError as e:
        pytest.fail(f"Compilation failed: {e}")
