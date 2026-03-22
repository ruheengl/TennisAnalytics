from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Any, Dict, List, Literal, Optional, Tuple

import numpy as np
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, model_validator

SQLITE_PATH = Path("data/features/player_features.sqlite")
DEFAULT_FEATURES = [
    "career_win_pct",
    "service_points_won_pct",
    "return_points_won_pct",
    "aces_per_service_game",
    "double_faults_per_service_game",
    "break_points_saved_pct",
    "elo_pre",
    "win_pct_last_20_matches",
    "surface_win_pct_last_20_matches",
]


class ClusterRequest(BaseModel):
    attributes: List[str] = Field(..., min_length=2)
    k: int = Field(6, ge=2, le=30)
    distance_metric: Literal["euclidean", "manhattan", "cosine"] = "euclidean"
    scaling: Literal["none", "zscore", "minmax"] = "zscore"
    max_iter: int = Field(40, ge=5, le=200)
    seed: int = 42
    filters: Dict[str, Any] = Field(default_factory=dict)


class ThresholdFilter(BaseModel):
    attribute: str
    op: Literal["gt", "gte", "lt", "lte", "eq", "between"]
    value: float | List[float]

    @model_validator(mode="after")
    def validate_range(self) -> "ThresholdFilter":
        if self.op == "between":
            if not isinstance(self.value, list) or len(self.value) != 2:
                raise ValueError("between requires [min, max]")
        elif isinstance(self.value, list):
            raise ValueError("value must be scalar for non-between operators")
        return self


class PlayerQueryRequest(BaseModel):
    filters: List[ThresholdFilter]
    limit: int = Field(100, ge=1, le=2000)
    offset: int = Field(0, ge=0)
    sort_by: Optional[str] = None
    sort_order: Literal["asc", "desc"] = "desc"
    cluster_request_id: Optional[str] = None
    cluster_label: Optional[int] = None


@dataclass
class ClusterCacheEntry:
    request_id: str
    normalized_payload: Dict[str, Any]
    feature_columns: List[str]
    player_ids: List[str]
    labels: np.ndarray
    centroids: np.ndarray
    vectors: np.ndarray
    quality: Dict[str, Any]
    created_at: float

    @property
    def label_map(self) -> Dict[str, int]:
        return {pid: int(label) for pid, label in zip(self.player_ids, self.labels.tolist())}


app = FastAPI(title="Player Clustering API", version="0.1.0")
_cache_lock = threading.Lock()
_cluster_cache: Dict[str, ClusterCacheEntry] = {}


def _connect() -> sqlite3.Connection:
    if not SQLITE_PATH.exists():
        raise HTTPException(status_code=500, detail=f"SQLite file not found at {SQLITE_PATH}")
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(player_features)").fetchall()
    return {row[1] for row in rows}


def _normalize_payload(payload: ClusterRequest) -> Dict[str, Any]:
    return {
        "attributes": sorted(set(payload.attributes)),
        "k": int(payload.k),
        "distance_metric": payload.distance_metric,
        "scaling": payload.scaling,
        "max_iter": int(payload.max_iter),
        "seed": int(payload.seed),
        "filters": {k: payload.filters[k] for k in sorted(payload.filters)},
    }


def _payload_hash(normalized: Dict[str, Any]) -> str:
    body = json.dumps(normalized, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()[:20]


def _build_where(filters: Dict[str, Any], valid_columns: set[str]) -> Tuple[str, List[Any]]:
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


def _scale_matrix(matrix: np.ndarray, method: str) -> np.ndarray:
    if method == "none":
        return matrix
    if method == "zscore":
        mean = np.nanmean(matrix, axis=0)
        std = np.nanstd(matrix, axis=0)
        std[std == 0] = 1.0
        return (matrix - mean) / std
    if method == "minmax":
        mins = np.nanmin(matrix, axis=0)
        maxs = np.nanmax(matrix, axis=0)
        span = maxs - mins
        span[span == 0] = 1.0
        return (matrix - mins) / span
    raise ValueError(f"Unsupported scaling: {method}")


def _distance(a: np.ndarray, b: np.ndarray, metric: str) -> np.ndarray:
    if metric == "euclidean":
        return np.sqrt(np.sum((a - b) ** 2, axis=1))
    if metric == "manhattan":
        return np.sum(np.abs(a - b), axis=1)
    if metric == "cosine":
        an = np.linalg.norm(a, axis=1)
        bn = np.linalg.norm(b)
        denom = an * (bn if bn != 0 else 1.0)
        denom[denom == 0] = 1.0
        sims = np.sum(a * b, axis=1) / denom
        return 1.0 - np.clip(sims, -1.0, 1.0)
    raise ValueError(f"Unsupported distance metric: {metric}")


def _kmeans(matrix: np.ndarray, k: int, metric: str, seed: int, max_iter: int) -> Tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    n = matrix.shape[0]
    if n < k:
        raise HTTPException(status_code=400, detail=f"k={k} cannot exceed player count={n}")

    centroids = matrix[rng.choice(n, size=k, replace=False)]
    labels = np.zeros(n, dtype=int)

    for _ in range(max_iter):
        dists = np.column_stack([_distance(matrix, c, metric) for c in centroids])
        new_labels = np.argmin(dists, axis=1)
        if np.array_equal(new_labels, labels):
            break
        labels = new_labels

        for idx in range(k):
            members = matrix[labels == idx]
            if len(members) == 0:
                centroids[idx] = matrix[rng.integers(0, n)]
            else:
                centroids[idx] = np.mean(members, axis=0)

    return labels, centroids


def _cluster_quality(matrix: np.ndarray, labels: np.ndarray, centroids: np.ndarray, metric: str) -> Dict[str, Any]:
    distances = np.column_stack([_distance(matrix, c, metric) for c in centroids])
    assigned = distances[np.arange(len(matrix)), labels]
    sorted_d = np.sort(distances, axis=1)
    sep = np.maximum(sorted_d[:, 1], 1e-9) if distances.shape[1] > 1 else np.ones(len(matrix))
    confidence = np.clip(1.0 - (assigned / sep), 0.0, 1.0)

    per_cluster = []
    for idx in range(len(centroids)):
        members = assigned[labels == idx]
        per_cluster.append(
            {
                "cluster_id": idx,
                "size": int(len(members)),
                "avg_distance_to_centroid": float(np.mean(members)) if len(members) else None,
                "distance_stddev": float(np.std(members)) if len(members) else None,
            }
        )

    return {
        "inertia": float(np.sum(assigned**2)),
        "mean_confidence": float(np.mean(confidence)),
        "confidence_by_player": confidence,
        "clusters": per_cluster,
    }


def _rows_to_matrix(rows: List[sqlite3.Row], columns: List[str]) -> Tuple[List[str], np.ndarray]:
    player_ids: List[str] = []
    vectors: List[List[float]] = []
    for row in rows:
        pid = row["player_id"]
        vals = []
        valid = True
        for col in columns:
            val = row[col]
            if val is None:
                valid = False
                break
            vals.append(float(val))
        if valid:
            player_ids.append(str(pid))
            vectors.append(vals)

    if not vectors:
        raise HTTPException(status_code=400, detail="No rows with complete feature vectors for selected attributes")
    return player_ids, np.asarray(vectors, dtype=np.float64)


def _load_cluster(req: ClusterRequest) -> ClusterCacheEntry:
    normalized = _normalize_payload(req)
    request_id = _payload_hash(normalized)

    with _cache_lock:
        cached = _cluster_cache.get(request_id)
    if cached:
        return cached

    with _connect() as conn:
        valid_cols = _table_columns(conn)
        invalid = [c for c in normalized["attributes"] if c not in valid_cols]
        if invalid:
            raise HTTPException(status_code=400, detail=f"Unknown attributes: {invalid}")

        where_sql, params = _build_where(normalized["filters"], valid_cols)
        query = f"SELECT player_id, {', '.join(f'\"{c}\"' for c in normalized['attributes'])} FROM player_features{where_sql}"
        rows = conn.execute(query, params).fetchall()

    player_ids, matrix = _rows_to_matrix(rows, normalized["attributes"])
    scaled = _scale_matrix(matrix, normalized["scaling"])
    labels, centroids = _kmeans(
        scaled,
        normalized["k"],
        normalized["distance_metric"],
        normalized["seed"],
        normalized["max_iter"],
    )
    quality = _cluster_quality(scaled, labels, centroids, normalized["distance_metric"])

    entry = ClusterCacheEntry(
        request_id=request_id,
        normalized_payload=normalized,
        feature_columns=normalized["attributes"],
        player_ids=player_ids,
        labels=labels,
        centroids=centroids,
        vectors=scaled,
        quality=quality,
        created_at=time(),
    )
    with _cache_lock:
        _cluster_cache[request_id] = entry
    return entry


def _nn_lookup(entry: ClusterCacheEntry, player_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    try:
        src_idx = entry.player_ids.index(player_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=f"Player {player_id} not found in cluster set") from exc

    src = entry.vectors[src_idx]
    dists = _distance(entry.vectors, src, entry.normalized_payload["distance_metric"])
    order = np.argsort(dists)

    result = []
    for idx in order:
        if idx == src_idx:
            continue
        result.append(
            {
                "player_id": entry.player_ids[idx],
                "cluster_id": int(entry.labels[idx]),
                "distance": float(dists[idx]),
            }
        )
        if len(result) >= limit:
            break
    return result


@app.post("/cluster")
def create_cluster(req: ClusterRequest) -> Dict[str, Any]:
    entry = _load_cluster(req)
    conf = entry.quality["confidence_by_player"]

    cluster_members: Dict[int, List[str]] = {}
    for pid, label in zip(entry.player_ids, entry.labels.tolist()):
        cluster_members.setdefault(int(label), []).append(pid)

    centroids = []
    for idx, centroid in enumerate(entry.centroids):
        centroid_stats = {name: float(value) for name, value in zip(entry.feature_columns, centroid.tolist())}
        centroids.append(
            {
                "cluster_id": idx,
                "centroid": centroid_stats,
                "size": len(cluster_members.get(idx, [])),
                "player_labels": cluster_members.get(idx, [])[:100],
            }
        )

    return {
        "cluster_request_id": entry.request_id,
        "cached": True,
        "player_count": len(entry.player_ids),
        "attributes": entry.feature_columns,
        "distance_metric": entry.normalized_payload["distance_metric"],
        "scaling": entry.normalized_payload["scaling"],
        "metadata": {
            "quality": {
                "inertia": entry.quality["inertia"],
                "mean_confidence": entry.quality["mean_confidence"],
                "clusters": entry.quality["clusters"],
            },
            "centroids": centroids,
            "confidence_sample": [
                {
                    "player_id": pid,
                    "cluster_id": int(label),
                    "confidence": float(score),
                }
                for pid, label, score in zip(entry.player_ids[:1000], entry.labels[:1000], conf[:1000])
            ],
        },
    }


@app.get("/clusters/{cluster_request_id}/players")
def cluster_players(
    cluster_request_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    cluster_id: Optional[int] = Query(None),
    min_confidence: Optional[float] = Query(None, ge=0.0, le=1.0),
    similar_to: Optional[str] = Query(None),
    similar_limit: int = Query(10, ge=1, le=200),
) -> Dict[str, Any]:
    entry = _cluster_cache.get(cluster_request_id)
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
        rows.append({"player_id": pid, "cluster_id": int(label), "confidence": c})

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
        response["similar_players"] = _nn_lookup(entry, similar_to, similar_limit)
    return response


@app.get("/players/search")
def search_players(
    q: str = Query(..., min_length=1),
    cluster_request_id: Optional[str] = None,
    cluster_id: Optional[int] = None,
    limit: int = Query(25, ge=1, le=200),
) -> Dict[str, Any]:
    like = f"%{q.lower()}%"
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT player_id FROM player_features
            WHERE LOWER(player_id) LIKE ?
            ORDER BY player_id
            LIMIT ?
            """,
            (like, limit * 10),
        ).fetchall()

    player_ids = [r["player_id"] for r in rows]
    if cluster_request_id:
        entry = _cluster_cache.get(cluster_request_id)
        if not entry:
            raise HTTPException(status_code=404, detail="Unknown cluster_request_id")
        mapping = entry.label_map
        player_ids = [pid for pid in player_ids if pid in mapping]
        if cluster_id is not None:
            player_ids = [pid for pid in player_ids if mapping[pid] == cluster_id]

    return {"query": q, "count": min(len(player_ids), limit), "players": player_ids[:limit]}


@app.post("/players/query")
def query_players(req: PlayerQueryRequest) -> Dict[str, Any]:
    with _connect() as conn:
        valid_cols = _table_columns(conn)
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
            entry = _cluster_cache.get(req.cluster_request_id)
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

    return {
        "total": total,
        "offset": req.offset,
        "limit": req.limit,
        "players": [dict(row) for row in rows],
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": SQLITE_PATH.exists(),
        "sqlite_path": str(SQLITE_PATH),
        "cache_size": len(_cluster_cache),
        "default_attributes": DEFAULT_FEATURES,
    }
