# Pipeline package

`pipeline/` owns ingestion, normalization, and feature engineering for ATP raw CSV files.

## Responsibilities
- Read raw yearly CSV files from `data/`.
- Normalize player and match fields into canonical records.
- Compute derived performance metrics used downstream.
- Persist normalized outputs as Parquet for analytics and JSON for API portability.

## Suggested layout
- `pipeline/ingest/`: CSV readers + validation.
- `pipeline/normalize/`: canonical schema mappers.
- `pipeline/features/`: derived metric transformations.
- `pipeline/export/`: Parquet/JSON writers.

## Stats cleaning utility

Use `pipeline/clean_stats.py` to aggregate per-set stat columns into `Sets[0]`,
clear `Sets[1..]` stat columns, and produce versioned cleaned outputs under
`data/processed/` plus a data-quality report.

Example:

```bash
python pipeline/clean_stats.py --years 2024 2025 2026
```

Outputs:
- `data/processed/atp_YYYY_clean.csv`
- `data/processed/cleaning_report.json`

## Player feature generation

Use `pipeline/features.py` to build player-level feature vectors from cleaned
match files, persist a parquet analytics artifact, and upsert an indexed SQLite
serving table.

Example:

```bash
python pipeline/features.py --match-windows 5 10 20 --day-windows 30 90 365
```

Outputs:
- `data/features/player_features.parquet`
- `data/features/player_features.sqlite` (with retrieval indexes)
- `data/features/feature_state.json` (incremental refresh fingerprints)
- `docs/player_feature_columns.md` (column dictionary)

## Match outcome modeling

Use `pipeline/modeling.py` to train and evaluate a tree model (`DecisionTreeClassifier`)
for match outcome prediction on top of the cleaned feature SQLite artifact.

Example:

```bash
python pipeline/modeling.py --max-depth 6 --min-samples-leaf 120
```

Outputs:
- `data/models/match_outcome_tree.pkl` (serialized model + feature metadata)
- `data/models/feature_importance.json`
- `data/models/top_decision_paths.json`
- `data/models/evaluation_metrics.json` (overall + season/surface segment metrics)
