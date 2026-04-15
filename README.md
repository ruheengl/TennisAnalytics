# CSCE679 Project

This repository includes three helper scripts so a new collaborator can get started quickly.

## Quickstart

1. Clone the repository.
2. Setup the environment:

```bash
./scripts/setup_environment.sh
```

3. Run the data pipeline:

```bash
./scripts/run_pipeline.sh
```

4. Start the local API + frontend servers:

```bash
./scripts/start_local.sh
```

The frontend runs on `http://localhost:5173` and proxies API requests to `http://localhost:8000`.

## Notes

- `run_pipeline.sh` accepts optional years. Example: `./scripts/run_pipeline.sh 2024 2025 2026`.
- You can override ports when starting services:

```bash
API_PORT=9000 FRONTEND_PORT=5174 ./scripts/start_local.sh
```
