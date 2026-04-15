from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

import numpy as np
from pydantic import BaseModel, Field, model_validator


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
    player_limit: Optional[int] = Field(None, ge=2, le=20000)

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
    limit: int = Field(100, ge=1)
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
    row_id: int = Field(..., ge=1, description="Unique SQLite row id for player_features.")
    top_k_features: int = Field(5, ge=1, le=20)
    include_tree_structure: bool = Field(
        True,
        description="Include full serialized tree structure in explanation payload.",
    )


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
    projection: Dict[str, Any]
    quality: Dict[str, Any]
    created_at: float

    @property
    def label_map(self) -> Dict[str, int]:
        return {pid: int(label) for pid, label in zip(self.player_ids, self.labels.tolist())}
