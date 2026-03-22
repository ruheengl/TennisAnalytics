# Architecture overview

This monorepo is split into domain-focused packages that share contract artifacts.

## Top-level packages
- `pipeline/`: Processes raw ATP CSV files, normalizes rows, engineers metrics, and writes canonical artifacts.
- `api/`: Provides a query layer over normalized data and ML endpoints (clustering/prediction).
- `frontend/`: D3.js visual analytics UI consuming API responses.
- `shared/`: Cross-package schemas and metric definitions used for validation and consistent interpretation.

## Data flow
1. **Raw ingestion**
   - Source files are loaded from `data/*.csv`.
2. **Normalization + feature engineering (`pipeline/`)**
   - Raw records are transformed into shared normalized match/player schemas.
   - Derived metrics (ace %, break points won %, win %, Elo trend) are computed from atomic stats.
3. **Artifact publication**
   - Cleaned datasets are emitted as **Parquet** (analytics/storage efficiency) and **JSON** (API portability).
4. **API query layer (`api/`)**
   - API reads normalized parquet/json artifacts.
   - Endpoints expose filtered records plus clustering/prediction services using shared payload contracts.
5. **Visual analytics (`frontend/`)**
   - D3 components request API data and render tables, trend lines, and cluster views.

## Shared contracts
- `shared/schemas/normalized_match_record.schema.json`
- `shared/schemas/normalized_player_record.schema.json`
- `shared/schemas/clustering_payload.schema.json`
- `shared/metrics/derived_metric_definitions.yaml`

These contracts are intended to be versioned alongside code so every package can validate inputs/outputs consistently.
