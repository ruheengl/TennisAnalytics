from __future__ import annotations

import pickle
from typing import Any, Dict, List

import numpy as np
from fastapi import HTTPException

from api.config import MODEL_ARTIFACT_PATH
from api import state


def load_predictor() -> Dict[str, Any]:
    with state.predictor_lock:
        if state.predictor_cache is not None:
            return state.predictor_cache
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
        state.predictor_cache = payload
        return payload


def prediction_explanation(
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
