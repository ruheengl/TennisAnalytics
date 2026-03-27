from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException

from api.config import METRIC_COLUMN_MAP
from api.services.db_service import connect


def metric_column(metric: str) -> str:
    col = METRIC_COLUMN_MAP.get(metric)
    if not col:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported metric '{metric}'. Supported: {sorted(METRIC_COLUMN_MAP)}",
        )
    return col


def load_metric_points(
    player_id: str,
    metric: str,
    start_date: Optional[str],
    end_date: Optional[str],
    limit: int,
) -> Tuple[str, List[Dict[str, Any]]]:
    column = metric_column(metric)
    clauses = ["player_id = ?", f'"{column}" IS NOT NULL']
    params: List[Any] = [player_id]
    if start_date:
        clauses.append("match_date >= ?")
        params.append(start_date)
    if end_date:
        clauses.append("match_date <= ?")
        params.append(end_date)

    where = " AND ".join(clauses)
    sql = f"""
        SELECT match_date, "{column}" AS metric_value
        FROM player_features
        WHERE {where}
        ORDER BY match_date ASC
        LIMIT ?
    """
    params.append(limit)
    with connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    points = [{"match_date": row["match_date"], "value": float(row["metric_value"])} for row in rows]
    if not points:
        raise HTTPException(status_code=404, detail=f"No metric points found for player_id={player_id}, metric={metric}")
    return column, points
