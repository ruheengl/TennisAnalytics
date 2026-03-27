from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Tuple

from fastapi import HTTPException

from api.config import SQLITE_PATH


def connect() -> sqlite3.Connection:
    if not SQLITE_PATH.exists():
        raise HTTPException(status_code=500, detail=f"SQLite file not found at {SQLITE_PATH}")
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def table_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(player_features)").fetchall()
    return {row[1] for row in rows}


def build_where(filters: Dict[str, Any], valid_columns: set[str]) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []

    for key, value in filters.items():
        if key not in valid_columns:
            continue
        if isinstance(value, dict):
            min_val = value.get("min")
            max_val = value.get("max")
            if min_val is not None:
                clauses.append(f'"{key}" >= ?')
                params.append(min_val)
            if max_val is not None:
                clauses.append(f'"{key}" <= ?')
                params.append(max_val)
        else:
            clauses.append(f'"{key}" = ?')
            params.append(value)

    return (" WHERE " + " AND ".join(clauses)) if clauses else "", params
