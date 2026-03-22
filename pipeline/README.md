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
