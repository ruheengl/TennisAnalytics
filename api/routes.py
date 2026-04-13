from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import numpy as np
from fastapi import APIRouter, HTTPException, Query

from api.config import DEFAULT_FEATURES, MODEL_ARTIFACT_PATH, SQLITE_PATH
from api.schemas import ClusterRequest, PlayerQueryRequest, PredictRequest, TrendQueryParams
from api.services.clustering_service import load_cluster, nn_lookup
from api.services.db_service import connect, table_columns
from api.services.metrics_service import load_metric_points
from api.services.prediction_service import load_predictor, prediction_explanation
from api.state import cluster_cache
from pipeline.trends import DegradationCriteria, TrendOptions, annotate_series, evaluate_degradation

router = APIRouter()


def _format_player_name(first_name: Optional[str], last_name: Optional[str], player_id: str) -> str:
    first = (first_name or "").strip()
    last = (last_name or "").strip()
    full_name = " ".join(part for part in (first, last) if part)
    return full_name if full_name else player_id


def _normalize_player_id(player_id: Any) -> str:
    text = str(player_id or "").strip()
    if not text:
        return ""
    if text.endswith(".0"):
        integer_part = text[:-2]
        if integer_part.isdigit():
            return integer_part
    return text


@lru_cache(maxsize=1)
def _player_name_lookup() -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    files = sorted(Path("data/processed").glob("atp_*_clean.csv"))
    for file_path in files:
        with file_path.open() as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                for team_num in (1, 2):
                    raw_player_id = (row.get(f"PlayerTeam{team_num}.PlayerId") or "").strip()
                    player_id = _normalize_player_id(raw_player_id)
                    if not player_id:
                        continue
                    first_name = row.get(f"PlayerTeam{team_num}.PlayerFirstName")
                    last_name = row.get(f"PlayerTeam{team_num}.PlayerLastName")
                    display_name = _format_player_name(first_name, last_name, player_id)
                    lookup[player_id] = display_name
                    if raw_player_id and raw_player_id != player_id:
                        lookup[raw_player_id] = display_name
    return lookup


def _player_display_name(player_id: str) -> str:
    normalized = _normalize_player_id(player_id)
    lookup = _player_name_lookup()
    return lookup.get(normalized, lookup.get(player_id, normalized or str(player_id)))


@router.post("/cluster")
def create_cluster(req: ClusterRequest) -> Dict[str, Any]:
    entry = load_cluster(req)
    conf = entry.quality["confidence_by_player"]

    cluster_members: Dict[int, List[str]] = {}
    for pid, label in zip(entry.player_ids, entry.labels.tolist()):
        cluster_members.setdefault(int(label), []).append(pid)

    centroid_rows = []
    unique_labels = sorted(set(int(v) for v in entry.labels.tolist() if int(v) >= 0))
    if entry.prototypes is not None:
        for idx, centroid in enumerate(entry.prototypes):
            centroid_stats = {name: float(value) for name, value in zip(entry.feature_columns, centroid.tolist())}
            centroid_rows.append(
                {
                    "cluster_id": idx,
                    "centroid": centroid_stats,
                    "size": len(cluster_members.get(idx, [])),
                    "player_labels": cluster_members.get(idx, [])[:100],
                }
            )
    else:
        for idx in unique_labels:
            centroid_rows.append(
                {
                    "cluster_id": idx,
                    "centroid": None,
                    "size": len(cluster_members.get(idx, [])),
                    "player_labels": cluster_members.get(idx, [])[:100],
                }
            )

    return {
        "cluster_request_id": entry.request_id,
        "cached": True,
        "player_count": len(entry.player_ids),
        "attributes": entry.feature_columns,
        "algorithm": entry.algorithm,
        "distance_metric": entry.normalized_payload["distance_metric"],
        "scaling": entry.normalized_payload["scaling"],
        "metadata": {
            "projection": {
                "explained_variance_ratio": entry.projection["explained_variance_ratio"],
                "component_loadings": entry.projection["component_loadings"],
                "top_absolute_loadings": entry.projection["top_absolute_loadings"],
            },
            "quality": {
                "inertia": entry.quality["inertia"],
                "mean_confidence": entry.quality["mean_confidence"],
                "clusters": entry.quality["clusters"],
                "method_stats": entry.quality.get("method_stats", {}),
            },
            "centroids": centroid_rows,
            "confidence_sample": [
                {
                    "player_id": pid,
                    "player_name": _player_display_name(pid),
                    "cluster_id": int(label),
                    "confidence": float(score),
                }
                for pid, label, score in zip(entry.player_ids[:1000], entry.labels[:1000], conf[:1000])
            ],
        },
        "projection": {
            "points": [
                {
                    "player_id": pid,
                    "player_name": _player_display_name(pid),
                    "pc1": float(coords[0]),
                    "pc2": float(coords[1]),
                }
                for pid, coords in zip(entry.player_ids, entry.projection["coordinates"])
            ],
            "explained_variance_ratio": entry.projection["explained_variance_ratio"],
            "component_loadings": entry.projection["component_loadings"],
            "top_absolute_loadings": entry.projection["top_absolute_loadings"],
        },
    }


@router.get("/clusters/{cluster_request_id}/projection")
def cluster_projection(cluster_request_id: str) -> Dict[str, Any]:
    entry = cluster_cache.get(cluster_request_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Unknown cluster_request_id")

    return {
        "cluster_request_id": cluster_request_id,
        "projection": {
            "points": [
                {
                    "player_id": pid,
                    "player_name": _player_display_name(pid),
                    "pc1": float(coords[0]),
                    "pc2": float(coords[1]),
                }
                for pid, coords in zip(entry.player_ids, entry.projection["coordinates"])
            ],
            "explained_variance_ratio": entry.projection["explained_variance_ratio"],
            "component_loadings": entry.projection["component_loadings"],
            "top_absolute_loadings": entry.projection["top_absolute_loadings"],
        },
    }


@router.get("/clusters/{cluster_request_id}/players")
def cluster_players(
    cluster_request_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    cluster_id: Optional[int] = Query(None),
    min_confidence: Optional[float] = Query(None, ge=0.0, le=1.0),
    similar_to: Optional[str] = Query(None),
    similar_limit: int = Query(10, ge=1, le=200),
) -> Dict[str, Any]:
    entry = cluster_cache.get(cluster_request_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Unknown cluster_request_id")

    conf = entry.quality["confidence_by_player"]
    rows = []
    for i, (pid, label) in enumerate(zip(entry.player_ids, entry.labels.tolist())):
        c = float(conf[i])
        if cluster_id is not None and int(label) != cluster_id:
            continue
        if min_confidence is not None and c < min_confidence:
            continue
        rows.append(
            {
                "player_id": pid,
                "player_name": _player_display_name(pid),
                "cluster_id": int(label),
                "confidence": c,
            }
        )

    total = len(rows)
    start = (page - 1) * page_size
    end = start + page_size

    response: Dict[str, Any] = {
        "cluster_request_id": cluster_request_id,
        "page": page,
        "page_size": page_size,
        "total": total,
        "players": rows[start:end],
    }
    if similar_to:
        response["similar_players"] = nn_lookup(entry, similar_to, similar_limit)
    return response


@router.get("/players/search")
def search_players(
    q: str = Query(..., min_length=1),
    cluster_request_id: Optional[str] = None,
    cluster_id: Optional[int] = None,
    limit: int = Query(25, ge=1, le=200),
) -> Dict[str, Any]:
    query_text = q.strip().lower()
    if not query_text:
        return {"query": q, "count": 0, "players": []}

    player_ids = [pid for pid, name in _player_name_lookup().items() if query_text in name.lower()]
    if cluster_request_id:
        entry = cluster_cache.get(cluster_request_id)
        if not entry:
            raise HTTPException(status_code=404, detail="Unknown cluster_request_id")
        mapping = entry.label_map
        player_ids = [pid for pid in player_ids if pid in mapping]
        if cluster_id is not None:
            player_ids = [pid for pid in player_ids if mapping[pid] == cluster_id]

    player_ids = sorted(player_ids)[:limit]
    players = [{"player_id": pid, "player_name": _player_display_name(pid)} for pid in player_ids]
    return {"query": q, "count": len(players), "players": players}


@router.post("/players/query")
def query_players(req: PlayerQueryRequest) -> Dict[str, Any]:
    with connect() as conn:
        valid_cols = table_columns(conn)
        for filt in req.filters:
            if filt.attribute not in valid_cols:
                raise HTTPException(status_code=400, detail=f"Unknown attribute: {filt.attribute}")

        clauses: List[str] = []
        params: List[Any] = []
        for filt in req.filters:
            col = f'"{filt.attribute}"'
            if filt.op == "gt":
                clauses.append(f"{col} > ?")
                params.append(filt.value)
            elif filt.op == "gte":
                clauses.append(f"{col} >= ?")
                params.append(filt.value)
            elif filt.op == "lt":
                clauses.append(f"{col} < ?")
                params.append(filt.value)
            elif filt.op == "lte":
                clauses.append(f"{col} <= ?")
                params.append(filt.value)
            elif filt.op == "eq":
                clauses.append(f"{col} = ?")
                params.append(filt.value)
            else:
                assert isinstance(filt.value, list)
                clauses.append(f"{col} BETWEEN ? AND ?")
                params.extend(filt.value)

        if req.cluster_request_id:
            entry = cluster_cache.get(req.cluster_request_id)
            if not entry:
                raise HTTPException(status_code=404, detail="Unknown cluster_request_id")
            candidates = [
                pid
                for pid, label in zip(entry.player_ids, entry.labels.tolist())
                if req.cluster_label is None or int(label) == req.cluster_label
            ]
            if not candidates:
                return {"total": 0, "players": []}
            placeholders = ",".join(["?"] * len(candidates))
            clauses.append(f"player_id IN ({placeholders})")
            params.extend(candidates)

        where_clause = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        order_clause = ""
        if req.sort_by:
            if req.sort_by not in valid_cols:
                raise HTTPException(status_code=400, detail=f"Unknown sort field: {req.sort_by}")
            order_clause = f' ORDER BY "{req.sort_by}" {req.sort_order.upper()}'

        sql = f"""
            SELECT * FROM player_features
            {where_clause}
            {order_clause}
            LIMIT ? OFFSET ?
        """
        rows = conn.execute(sql, (*params, req.limit, req.offset)).fetchall()

        count_sql = f"SELECT COUNT(*) AS n FROM player_features {where_clause}"
        total = int(conn.execute(count_sql, params).fetchone()["n"])

    rows_with_names = []
    for row in rows:
        record = dict(row)
        record["player_id"] = _normalize_player_id(record.get("player_id", ""))
        record["opponent_id"] = _normalize_player_id(record.get("opponent_id", ""))
        record["player_name"] = _player_display_name(record["player_id"])
        record["opponent_name"] = _player_display_name(record["opponent_id"])
        rows_with_names.append(record)

    return {
        "total": total,
        "offset": req.offset,
        "limit": req.limit,
        "players": rows_with_names,
    }


@router.get("/players/{player_id}/metrics/timeseries")
def player_metric_timeseries(
    player_id: str,
    metric: Literal["elo", "ace_pct", "break_points_won_pct", "win_pct"] = Query(...),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(500, ge=10, le=5000),
    smoothing: Literal["none", "ema", "moving_average"] = Query("ema"),
    smoothing_window: int = Query(5, ge=1, le=200),
    ema_alpha: float = Query(0.35, gt=0.0, le=1.0),
    regression: Literal["ols", "theil_sen"] = Query("theil_sen"),
    min_points: int = Query(6, ge=2, le=1000),
    bootstrap_samples: int = Query(300, ge=50, le=2000),
    change_point_z: float = Query(2.0, ge=0.5, le=10.0),
) -> Dict[str, Any]:
    source_column, points = load_metric_points(player_id, metric, start_date, end_date, limit)
    params = TrendQueryParams(
        smoothing=smoothing,
        smoothing_window=smoothing_window,
        ema_alpha=ema_alpha,
        regression=regression,
        min_points=min_points,
        bootstrap_samples=bootstrap_samples,
        change_point_z=change_point_z,
    )
    trend_payload = annotate_series(
        points=points,
        value_key="value",
        options=TrendOptions(
            smoothing=params.smoothing,
            smoothing_window=params.smoothing_window,
            ema_alpha=params.ema_alpha,
            regression=params.regression,
            min_points=params.min_points,
            bootstrap_samples=params.bootstrap_samples,
            change_point_z=params.change_point_z,
        ),
    )
    return {
        "player_id": player_id,
        "metric": metric,
        "source_column": source_column,
        "count": len(points),
        "points": trend_payload["points"],
        "trend": trend_payload["trend"],
    }


@router.get("/players/{player_id}/metrics/degradation")
def player_metric_degradation(
    player_id: str,
    metric: Literal["elo", "ace_pct", "break_points_won_pct", "win_pct"] = Query(...),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(500, ge=10, le=5000),
    smoothing: Literal["none", "ema", "moving_average"] = Query("ema"),
    smoothing_window: int = Query(5, ge=1, le=200),
    ema_alpha: float = Query(0.35, gt=0.0, le=1.0),
    regression: Literal["ols", "theil_sen"] = Query("theil_sen"),
    min_points: int = Query(6, ge=2, le=1000),
    bootstrap_samples: int = Query(300, ge=50, le=2000),
    change_point_z: float = Query(2.0, ge=0.5, le=10.0),
    min_negative_slope: float = Query(0.0, ge=0.0),
    drawdown_threshold: float = Query(0.08, ge=0.0),
    sustained_decline_window: int = Query(5, ge=2, le=200),
    sustained_decline_min_drop: float = Query(0.02, ge=0.0),
) -> Dict[str, Any]:
    source_column, points = load_metric_points(player_id, metric, start_date, end_date, limit)
    options = TrendOptions(
        smoothing=smoothing,
        smoothing_window=smoothing_window,
        ema_alpha=ema_alpha,
        regression=regression,
        min_points=min_points,
        bootstrap_samples=bootstrap_samples,
        change_point_z=change_point_z,
    )
    annotated = annotate_series(points=points, value_key="value", options=options)
    criteria = DegradationCriteria(
        min_negative_slope=min_negative_slope,
        drawdown_threshold=drawdown_threshold,
        sustained_decline_window=sustained_decline_window,
        sustained_decline_min_drop=sustained_decline_min_drop,
    )
    degradation = evaluate_degradation(
        [p.get("smoothed_value") for p in annotated["points"]],
        trend=annotated["trend"],
        criteria=criteria,
    )
    return {
        "player_id": player_id,
        "metric": metric,
        "source_column": source_column,
        "count": len(points),
        "criteria": {
            "min_negative_slope": min_negative_slope,
            "drawdown_threshold": drawdown_threshold,
            "sustained_decline_window": sustained_decline_window,
            "sustained_decline_min_drop": sustained_decline_min_drop,
        },
        "trend": annotated["trend"],
        "degradation": degradation,
        "points": annotated["points"],
    }


@router.post("/predict")
def predict_match_outcome(req: PredictRequest) -> Dict[str, Any]:
    predictor = load_predictor()
    model = predictor["model"]
    feature_columns = list(predictor["feature_columns"])
    medians = np.asarray(predictor["imputer_medians"], dtype=np.float64)
    threshold = float(predictor.get("threshold", 0.5))

    x = np.asarray(
        [
            [
                float(req.features.get(col))
                if req.features.get(col) is not None
                else float(medians[idx])
                for idx, col in enumerate(feature_columns)
            ]
        ],
        dtype=np.float64,
    )

    prob = float(model.predict_proba(x)[0, 1])
    label = int(prob >= threshold)
    explanation = prediction_explanation(
        model,
        x,
        feature_columns,
        req.top_k_features,
        include_tree_structure=req.include_tree_structure,
    )

    return {
        "predicted_outcome": label,
        "win_probability": prob,
        "threshold": threshold,
        "model_type": predictor.get("model_type", "tree_model"),
        "trained_at": predictor.get("trained_at"),
        "used_default_medians_for": [
            col for col in feature_columns if req.features.get(col) is None
        ],
        "explanation": explanation,
    }


@router.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": SQLITE_PATH.exists(),
        "sqlite_path": str(SQLITE_PATH),
        "model_artifact_path": str(MODEL_ARTIFACT_PATH),
        "model_artifact_exists": MODEL_ARTIFACT_PATH.exists(),
        "cache_size": len(cluster_cache),
        "default_attributes": DEFAULT_FEATURES,
    }
