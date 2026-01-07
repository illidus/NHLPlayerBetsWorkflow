from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Iterable, List

import pandas as pd

from . import config
from .schema import URL_LAKE_COLUMNS, PAGES_COLUMNS
from .utils import ensure_dirs


def read_parquet(path: Path, columns: List[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns or [])
    return pd.read_parquet(path, columns=columns)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    ensure_dirs()
    df.to_parquet(path, index=False)


def append_parquet(df: pd.DataFrame, path: Path, key_columns: List[str]) -> pd.DataFrame:
    existing = read_parquet(path)
    combined = pd.concat([existing, df], ignore_index=True)
    combined = combined.drop_duplicates(subset=key_columns, keep="last")
    write_parquet(combined, path)
    return combined


def load_url_lake() -> pd.DataFrame:
    return read_parquet(config.URL_LAKE_PATH, columns=URL_LAKE_COLUMNS)


def save_url_lake(df: pd.DataFrame) -> None:
    ordered = df.reindex(columns=URL_LAKE_COLUMNS)
    write_parquet(ordered, config.URL_LAKE_PATH)


def load_pages() -> pd.DataFrame:
    return read_parquet(config.PAGES_PATH, columns=PAGES_COLUMNS)


def save_pages(df: pd.DataFrame) -> None:
    ordered = df.reindex(columns=PAGES_COLUMNS)
    write_parquet(ordered, config.PAGES_PATH)


def append_jsonl(records: Iterable[dict], path: Path) -> None:
    ensure_dirs()
    with path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, default=str))
            handle.write("\n")


def load_jsonl_hashes(path: Path, key: str = "record_hash") -> set[str]:
    if not path.exists():
        return set()
    hashes: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            value = payload.get(key)
            if value:
                hashes.add(str(value))
    return hashes


def write_duckdb_table(table_name: str, df: pd.DataFrame) -> None:
    if importlib.util.find_spec("duckdb") is None:
        return
    import duckdb

    if df.empty:
        return

    ensure_dirs()
    con = duckdb.connect(str(config.ODDS_ARCHIVE_DB_PATH))
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0")
    con.execute(f"DELETE FROM {table_name}")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
    con.close()
