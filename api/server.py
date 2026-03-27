from __future__ import annotations

import hashlib
import json
import pickle
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Any, Dict, List, Literal, Optional, Tuple

import numpy as np
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, model_validator
from pipeline.trends import DegradationCriteria, TrendOptions, annotate_series, evaluate_degradation

SQLITE_PATH = Path("data/features/player_features.sqlite")
MODEL_ARTIFACT_PATH = Path("data/models/match_outcome_tree.pkl")
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
    algorithm: Literal["kmeans", "gmm", "dbscan", "hierarchical"] = "kmeans"
    params: Dict[str, Any] = Field(default_factory=dict)
    k: int = Field(6, ge=2, le=30)
    distance_metric: Literal["euclidean", "manhattan", "cosine"] = "euclidean"
    scaling: Literal["none", "zscore", "minmax"] = "zscore"
    max_iter: int = Field(40, ge=5, le=200)
    seed: int = 42
    filters: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_algorithm_params(self) -> "ClusterRequest":
        if self.algorithm in {"kmeans", "gmm", "hierarchical"}:
            raw_k = self.params.get("k", self.k)
            if not isinstance(raw_k, int) or raw_k < 2 or raw_k > 30:
                raise ValueError("params.k must be an integer in [2, 30]")
        if self.algorithm == "dbscan":
            eps = self.params.get("eps", 0.7)
            min_samples = self.params.get("min_samples", 5)
            if not isinstance(eps, (int, float)) or float(eps) <= 0:
                raise ValueError("params.eps must be > 0 for dbscan")
            if not isinstance(min_samples, int) or min_samples < 1:
                raise ValueError("params.min_samples must be >= 1 for dbscan")
        return self


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


class TrendQueryParams(BaseModel):
    smoothing: Literal["none", "ema", "moving_average"] = "ema"
    smoothing_window: int = Field(5, ge=1, le=200)
    ema_alpha: float = Field(0.35, gt=0.0, le=1.0)
    regression: Literal["ols", "theil_sen"] = "theil_sen"
    min_points: int = Field(6, ge=2, le=1000)
    bootstrap_samples: int = Field(300, ge=50, le=2000)
    change_point_z: float = Field(2.0, ge=0.5, le=10.0)


class PredictRequest(BaseModel):
    features: Dict[str, float] = Field(
        ..., description="Feature vector aligned to training feature names."
    )
    top_k_features: int = Field(5, ge=1, le=20)


@dataclass
class ClusterCacheEntry:
    request_id: str
    normalized_payload: Dict[str, Any]
    feature_columns: List[str]
    player_ids: List[str]
    labels: np.ndarray
    algorithm: str
    prototypes: Optional[np.ndarray]
    vectors: np.ndarray
    quality: Dict[str, Any]
    created_at: float

    @property
    def label_map(self) -> Dict[str, int]:
        return {pid: int(label) for pid, label in zip(self.player_ids, self.labels.tolist())}


app = FastAPI(title="Player Clustering API", version="0.1.0")
_cache_lock = threading.Lock()
_cluster_cache: Dict[str, ClusterCacheEntry] = {}
_predictor_lock = threading.Lock()
_predictor_cache: Optional[Dict[str, Any]] = None
METRIC_COLUMN_MAP: Dict[str, str] = {
    "elo": "elo_pre",
    "ace_pct": "aces_per_service_game",
    "break_points_won_pct": "break_points_saved_pct",
    "win_pct": "career_win_pct",
}


def _metric_column(metric: str) -> str:
    col = METRIC_COLUMN_MAP.get(metric)
    if not col:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported metric '{metric}'. Supported: {sorted(METRIC_COLUMN_MAP)}",
        )
    return col


def _load_predictor() -> Dict[str, Any]:
    global _predictor_cache
    with _predictor_lock:
        if _predictor_cache is not None:
            return _predictor_cache
        if not MODEL_ARTIFACT_PATH.exists():
            raise HTTPException(
                status_code=500,
                detail=f"Model artifact not found at {MODEL_ARTIFACT_PATH}. "
                "Run pipeline/modeling.py first.",
            )
        with MODEL_ARTIFACT_PATH.open("rb") as fh:
            payload = pickle.load(fh)
        required = {"model", "feature_columns", "imputer_medians", "threshold"}
        if not required.issubset(set(payload.keys())):
            raise HTTPException(status_code=500, detail="Model artifact payload is invalid.")
        _predictor_cache = payload
        return payload


def _prediction_explanation(
    model: Any,
    x: np.ndarray,
    feature_columns: List[str],
    top_k: int,
) -> Dict[str, Any]:
    if not hasattr(model, "tree_"):
        return {"top_contributing_features": [], "path_summary": {"rules": [], "leaf_id": None}}

    tree = model.tree_
    node_indicator = model.decision_path(x)
    leaf_id = int(model.apply(x)[0])
    node_ids = node_indicator.indices[
        node_indicator.indptr[0] : node_indicator.indptr[1]
    ].tolist()

    rules: List[Dict[str, Any]] = []
    contributions: Dict[str, float] = {}
    for node_id in node_ids:
        left_id = int(tree.children_left[node_id])
        right_id = int(tree.children_right[node_id])
        if left_id == right_id:
            continue
        feat_idx = int(tree.feature[node_id])
        threshold = float(tree.threshold[node_id])
        value = float(x[0, feat_idx])
        feature_name = feature_columns[feat_idx]
        direction = "<=" if value <= threshold else ">"
        margin = abs(value - threshold)
        rules.append(
            {
                "feature": feature_name,
                "operator": direction,
                "threshold": threshold,
                "value": value,
                "margin": margin,
            }
        )
        contributions[feature_name] = contributions.get(feature_name, 0.0) + margin

    top = sorted(
        [{"feature": k, "score": float(v)} for k, v in contributions.items()],
        key=lambda item: item["score"],
        reverse=True,
    )[:top_k]

    leaf_counts = tree.value[leaf_id][0]
    total = float(np.sum(leaf_counts))
    win_prob = float(leaf_counts[1] / total) if total else None
    return {
        "top_contributing_features": top,
        "path_summary": {
            "leaf_id": leaf_id,
            "sample_count": int(tree.n_node_samples[leaf_id]),
            "leaf_win_probability": win_prob,
            "rules": rules,
        },
    }


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
        "algorithm": payload.algorithm,
        "params": payload.params,
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


def _resolve_algorithm_params(req: Dict[str, Any]) -> Dict[str, Any]:
    params = dict(req.get("params") or {})
    params.setdefault("k", int(req["k"]))
    params.setdefault("max_iter", int(req["max_iter"]))
    params.setdefault("seed", int(req["seed"]))
    params.setdefault("distance_metric", req["distance_metric"])
    return params


def _run_kmeans(matrix: np.ndarray, req: Dict[str, Any]) -> Dict[str, Any]:
    params = _resolve_algorithm_params(req)
    labels, centroids = _kmeans(
        matrix, int(params["k"]), str(params["distance_metric"]), int(params["seed"]), int(params["max_iter"])
    )
    quality = _cluster_quality(matrix, labels, centroids, str(params["distance_metric"]))
    quality["method_stats"] = {"algorithm": "kmeans", "k": int(params["k"])}
    return {"labels": labels, "prototypes": centroids, "quality": quality}


def _run_gmm(matrix: np.ndarray, req: Dict[str, Any]) -> Dict[str, Any]:
    params = _resolve_algorithm_params(req)
    k = int(params["k"])
    max_iter = int(params["max_iter"])
    seed = int(params["seed"])
    n, d = matrix.shape
    km_labels, km_centroids = _kmeans(matrix, k, str(params["distance_metric"]), seed, max_iter=max(10, min(max_iter, 30)))
    means = km_centroids.copy()
    variances = np.ones((k, d), dtype=np.float64)
    weights = np.ones(k, dtype=np.float64) / k
    eye = 1e-6

    for _ in range(max_iter):
        log_probs = np.zeros((n, k), dtype=np.float64)
        for j in range(k):
            diff = matrix - means[j]
            var = np.maximum(variances[j], eye)
            log_det = float(np.sum(np.log(var)))
            quad = np.sum((diff * diff) / var, axis=1)
            log_probs[:, j] = -0.5 * (d * np.log(2.0 * np.pi) + log_det + quad) + np.log(max(weights[j], eye))
        max_log = np.max(log_probs, axis=1, keepdims=True)
        probs = np.exp(log_probs - max_log)
        responsibilities = probs / np.maximum(np.sum(probs, axis=1, keepdims=True), eye)
        nk = np.sum(responsibilities, axis=0)
        weights = nk / n
        means = (responsibilities.T @ matrix) / np.maximum(nk[:, None], eye)
        for j in range(k):
            diff = matrix - means[j]
            variances[j] = np.sum(responsibilities[:, [j]] * (diff * diff), axis=0) / max(nk[j], eye)

    labels = np.argmax(responsibilities, axis=1).astype(int)
    confidence = responsibilities[np.arange(n), labels]
    quality = _cluster_quality(matrix, labels, means, str(params["distance_metric"]))
    quality["confidence_by_player"] = confidence
    quality["method_stats"] = {
        "algorithm": "gmm",
        "k": k,
        "avg_assignment_probability": float(np.mean(confidence)),
    }
    return {"labels": labels, "prototypes": means, "quality": quality}


def _run_dbscan(matrix: np.ndarray, req: Dict[str, Any]) -> Dict[str, Any]:
    params = _resolve_algorithm_params(req)
    eps = float(params.get("eps", 0.7))
    min_samples = int(params.get("min_samples", 5))
    metric = str(params["distance_metric"])
    n = matrix.shape[0]
    labels = np.full(n, -1, dtype=int)
    visited = np.zeros(n, dtype=bool)
    cluster_id = 0

    def neighborhood(i: int) -> np.ndarray:
        d = _distance(matrix, matrix[i], metric)
        return np.where(d <= eps)[0]

    for i in range(n):
        if visited[i]:
            continue
        visited[i] = True
        neighbors = neighborhood(i)
        if len(neighbors) < min_samples:
            labels[i] = -1
            continue
        labels[i] = cluster_id
        seeds = list(neighbors.tolist())
        idx = 0
        while idx < len(seeds):
            j = seeds[idx]
            if not visited[j]:
                visited[j] = True
                nbs = neighborhood(j)
                if len(nbs) >= min_samples:
                    for c in nbs.tolist():
                        if c not in seeds:
                            seeds.append(c)
            if labels[j] == -1:
                labels[j] = cluster_id
            if labels[j] < 0:
                labels[j] = cluster_id
            idx += 1
        cluster_id += 1

    non_noise = sorted(int(v) for v in np.unique(labels) if v >= 0)
    remap = {old: new for new, old in enumerate(non_noise)}
    labels = np.asarray([remap.get(int(v), -1) for v in labels], dtype=int)
    k = len(non_noise)
    if k > 0:
        centroids = np.vstack([np.mean(matrix[labels == idx], axis=0) for idx in range(k)])
        quality = _cluster_quality(matrix[labels >= 0], labels[labels >= 0], centroids, metric)
        full_conf = np.zeros(n, dtype=np.float64)
        full_conf[labels >= 0] = quality["confidence_by_player"]
    else:
        centroids = None
        quality = {"inertia": None, "mean_confidence": 0.0, "clusters": [], "confidence_by_player": np.zeros(n)}
        full_conf = quality["confidence_by_player"]
    quality["confidence_by_player"] = full_conf
    quality["method_stats"] = {
        "algorithm": "dbscan",
        "eps": eps,
        "min_samples": min_samples,
        "noise_count": int(np.sum(labels < 0)),
        "cluster_count": k,
    }
    return {"labels": labels, "prototypes": centroids, "quality": quality}


def _run_hierarchical(matrix: np.ndarray, req: Dict[str, Any]) -> Dict[str, Any]:
    params = _resolve_algorithm_params(req)
    target_k = int(params["k"])
    n = matrix.shape[0]
    if n < target_k:
        raise HTTPException(status_code=400, detail=f"k={target_k} cannot exceed player count={n}")
    clusters: Dict[int, List[int]] = {i: [i] for i in range(n)}
    next_id = n
    while len(clusters) > target_k:
        keys = list(clusters)
        best_pair: Optional[Tuple[int, int]] = None
        best_dist = float("inf")
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                ai = np.mean(matrix[clusters[keys[i]]], axis=0)
                bj = np.mean(matrix[clusters[keys[j]]], axis=0)
                dist = float(np.linalg.norm(ai - bj))
                if dist < best_dist:
                    best_dist = dist
                    best_pair = (keys[i], keys[j])
        assert best_pair is not None
        a, b = best_pair
        clusters[next_id] = clusters[a] + clusters[b]
        del clusters[a]
        del clusters[b]
        next_id += 1
    labels = np.zeros(n, dtype=int)
    for cid, members in enumerate(clusters.values()):
        labels[members] = cid
    centroids = np.vstack([np.mean(matrix[labels == idx], axis=0) for idx in range(target_k)])
    quality = _cluster_quality(matrix, labels, centroids, str(params["distance_metric"]))
    quality["method_stats"] = {"algorithm": "hierarchical", "k": target_k, "linkage": "average"}
    return {"labels": labels, "prototypes": centroids, "quality": quality}


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


def _load_metric_points(
    player_id: str,
    metric: str,
    start_date: Optional[str],
    end_date: Optional[str],
    limit: int,
) -> Tuple[str, List[Dict[str, Any]]]:
    column = _metric_column(metric)
    clauses = ['player_id = ?', f'"{column}" IS NOT NULL']
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
    with _connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    points = [{"match_date": row["match_date"], "value": float(row["metric_value"])} for row in rows]
    if not points:
        raise HTTPException(status_code=404, detail=f"No metric points found for player_id={player_id}, metric={metric}")
    return column, points


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
    algorithm = normalized["algorithm"]
    runners = {
        "kmeans": _run_kmeans,
        "gmm": _run_gmm,
        "dbscan": _run_dbscan,
        "hierarchical": _run_hierarchical,
    }
    runner = runners.get(algorithm)
    if runner is None:
        raise HTTPException(status_code=400, detail=f"Unsupported algorithm: {algorithm}")
    result = runner(scaled, normalized)
    labels = result["labels"]
    prototypes = result["prototypes"]
    quality = result["quality"]

    entry = ClusterCacheEntry(
        request_id=request_id,
        normalized_payload=normalized,
        feature_columns=normalized["attributes"],
        player_ids=player_ids,
        labels=labels,
        algorithm=algorithm,
        prototypes=prototypes,
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


@app.get("/players/{player_id}/metrics/timeseries")
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
    source_column, points = _load_metric_points(player_id, metric, start_date, end_date, limit)
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


@app.get("/players/{player_id}/metrics/degradation")
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
    source_column, points = _load_metric_points(player_id, metric, start_date, end_date, limit)
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


@app.post("/predict")
def predict_match_outcome(req: PredictRequest) -> Dict[str, Any]:
    predictor = _load_predictor()
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
    explanation = _prediction_explanation(model, x, feature_columns, req.top_k_features)

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


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": SQLITE_PATH.exists(),
        "sqlite_path": str(SQLITE_PATH),
        "model_artifact_path": str(MODEL_ARTIFACT_PATH),
        "model_artifact_exists": MODEL_ARTIFACT_PATH.exists(),
        "cache_size": len(_cluster_cache),
        "default_attributes": DEFAULT_FEATURES,
    }
