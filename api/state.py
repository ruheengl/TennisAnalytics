import threading
from typing import Any, Dict, Optional

from api.schemas import ClusterCacheEntry

cache_lock = threading.Lock()
cluster_cache: Dict[str, ClusterCacheEntry] = {}
predictor_lock = threading.Lock()
predictor_cache: Optional[Dict[str, Any]] = None
