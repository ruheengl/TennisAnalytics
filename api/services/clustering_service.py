from __future__ import annotations

import hashlib
import json
import sqlite3
from time import time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from fastapi import HTTPException

from api.schemas import ClusterCacheEntry, ClusterRequest
from api.services.db_service import build_where, connect, table_columns
from api.state import cache_lock, cluster_cache


def normalize_payload(payload: ClusterRequest) -> Dict[str, Any]:
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


def payload_hash(normalized: Dict[str, Any]) -> str:
    body = json.dumps(normalized, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()[:20]


def scale_matrix(matrix: np.ndarray, method: str) -> np.ndarray:
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


def distance(a: np.ndarray, b: np.ndarray, metric: str) -> np.ndarray:
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


def kmeans(matrix: np.ndarray, k: int, metric: str, seed: int, max_iter: int) -> Tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    n = matrix.shape[0]
    if n < k:
        raise HTTPException(status_code=400, detail=f"k={k} cannot exceed player count={n}")

    centroids = matrix[rng.choice(n, size=k, replace=False)]
    labels = np.zeros(n, dtype=int)

    for _ in range(max_iter):
        dists = np.column_stack([distance(matrix, c, metric) for c in centroids])
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


def cluster_quality(matrix: np.ndarray, labels: np.ndarray, centroids: np.ndarray, metric: str) -> Dict[str, Any]:
    distances = np.column_stack([distance(matrix, c, metric) for c in centroids])
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


def resolve_algorithm_params(req: Dict[str, Any]) -> Dict[str, Any]:
    params = dict(req.get("params") or {})
    params.setdefault("k", int(req["k"]))
    params.setdefault("max_iter", int(req["max_iter"]))
    params.setdefault("seed", int(req["seed"]))
    params.setdefault("distance_metric", req["distance_metric"])
    return params


def run_kmeans(matrix: np.ndarray, req: Dict[str, Any]) -> Dict[str, Any]:
    params = resolve_algorithm_params(req)
    labels, centroids = kmeans(
        matrix, int(params["k"]), str(params["distance_metric"]), int(params["seed"]), int(params["max_iter"])
    )
    quality = cluster_quality(matrix, labels, centroids, str(params["distance_metric"]))
    quality["method_stats"] = {"algorithm": "kmeans", "k": int(params["k"])}
    return {"labels": labels, "prototypes": centroids, "quality": quality}


def run_gmm(matrix: np.ndarray, req: Dict[str, Any]) -> Dict[str, Any]:
    params = resolve_algorithm_params(req)
    k = int(params["k"])
    max_iter = int(params["max_iter"])
    seed = int(params["seed"])
    n, d = matrix.shape
    _, km_centroids = kmeans(matrix, k, str(params["distance_metric"]), seed, max_iter=max(10, min(max_iter, 30)))
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
    quality = cluster_quality(matrix, labels, means, str(params["distance_metric"]))
    quality["confidence_by_player"] = confidence
    quality["method_stats"] = {
        "algorithm": "gmm",
        "k": k,
        "avg_assignment_probability": float(np.mean(confidence)),
    }
    return {"labels": labels, "prototypes": means, "quality": quality}


def run_dbscan(matrix: np.ndarray, req: Dict[str, Any]) -> Dict[str, Any]:
    params = resolve_algorithm_params(req)
    eps = float(params.get("eps", 0.7))
    min_samples = int(params.get("min_samples", 5))
    metric = str(params["distance_metric"])
    n = matrix.shape[0]
    labels = np.full(n, -1, dtype=int)
    visited = np.zeros(n, dtype=bool)
    cluster_id = 0

    def neighborhood(i: int) -> np.ndarray:
        d = distance(matrix, matrix[i], metric)
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
        quality = cluster_quality(matrix[labels >= 0], labels[labels >= 0], centroids, metric)
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


def run_hierarchical(matrix: np.ndarray, req: Dict[str, Any]) -> Dict[str, Any]:
    params = resolve_algorithm_params(req)
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
    quality = cluster_quality(matrix, labels, centroids, str(params["distance_metric"]))
    quality["method_stats"] = {"algorithm": "hierarchical", "k": target_k, "linkage": "average"}
    return {"labels": labels, "prototypes": centroids, "quality": quality}


def rows_to_matrix(rows: List[sqlite3.Row], columns: List[str]) -> Tuple[List[str], np.ndarray]:
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


def compute_pca_projection(matrix: np.ndarray, feature_columns: List[str]) -> Dict[str, Any]:
    n, d = matrix.shape
    k = min(2, d)
    centered = matrix - np.mean(matrix, axis=0, keepdims=True)

    if n <= 1:
        components = np.zeros((k, d), dtype=np.float64)
        scores = np.zeros((n, k), dtype=np.float64)
        explained = np.zeros(k, dtype=np.float64)
    else:
        _, singular_values, vh = np.linalg.svd(centered, full_matrices=False)
        components = vh[:k]
        scores = centered @ components.T
        variance = (singular_values**2) / (n - 1)
        total_variance = float(np.sum(variance))
        explained = variance[:k] / total_variance if total_variance > 0 else np.zeros(k, dtype=np.float64)

    if k < 2:
        components = np.pad(components, ((0, 2 - k), (0, 0)))
        scores = np.pad(scores, ((0, 0), (0, 2 - k)))
        explained = np.pad(explained, (0, 2 - k))

    component_loadings: List[Dict[str, float]] = []
    top_loadings: List[List[Dict[str, Any]]] = []
    for comp in range(2):
        loading_map = {feature: float(weight) for feature, weight in zip(feature_columns, components[comp].tolist())}
        component_loadings.append(loading_map)
        ranked = sorted(loading_map.items(), key=lambda item: abs(item[1]), reverse=True)[:3]
        top_loadings.append([{"attribute": attr, "loading": float(weight)} for attr, weight in ranked])

    return {
        "explained_variance_ratio": [float(explained[0]), float(explained[1])],
        "component_loadings": component_loadings,
        "top_absolute_loadings": top_loadings,
        "coordinates": scores[:, :2],
    }


def load_cluster(req: ClusterRequest) -> ClusterCacheEntry:
    normalized = normalize_payload(req)
    request_id = payload_hash(normalized)

    with cache_lock:
        cached = cluster_cache.get(request_id)
    if cached:
        return cached

    with connect() as conn:
        valid_cols = table_columns(conn)
        invalid = [c for c in normalized["attributes"] if c not in valid_cols]
        if invalid:
            raise HTTPException(status_code=400, detail=f"Unknown attributes: {invalid}")

        where_sql, params = build_where(normalized["filters"], valid_cols)
        query = f"SELECT player_id, {', '.join(f'\"{c}\"' for c in normalized['attributes'])} FROM player_features{where_sql}"
        rows = conn.execute(query, params).fetchall()

    player_ids, matrix = rows_to_matrix(rows, normalized["attributes"])
    scaled = scale_matrix(matrix, normalized["scaling"])
    algorithm = normalized["algorithm"]
    runners = {
        "kmeans": run_kmeans,
        "gmm": run_gmm,
        "dbscan": run_dbscan,
        "hierarchical": run_hierarchical,
    }
    runner = runners.get(algorithm)
    if runner is None:
        raise HTTPException(status_code=400, detail=f"Unsupported algorithm: {algorithm}")

    result = runner(scaled, normalized)
    entry = ClusterCacheEntry(
        request_id=request_id,
        normalized_payload=normalized,
        feature_columns=normalized["attributes"],
        player_ids=player_ids,
        labels=result["labels"],
        algorithm=algorithm,
        prototypes=result["prototypes"],
        vectors=scaled,
        projection=compute_pca_projection(scaled, normalized["attributes"]),
        quality=result["quality"],
        created_at=time(),
    )
    with cache_lock:
        cluster_cache[request_id] = entry
    return entry


def nn_lookup(entry: ClusterCacheEntry, player_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    try:
        src_idx = entry.player_ids.index(player_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=f"Player {player_id} not found in cluster set") from exc

    src = entry.vectors[src_idx]
    dists = distance(entry.vectors, src, entry.normalized_payload["distance_metric"])
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
