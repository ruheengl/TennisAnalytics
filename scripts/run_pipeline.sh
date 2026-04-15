#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"

cd "${ROOT_DIR}"

if [[ ! -d "${VENV_DIR}" ]]; then
  echo "Error: virtual environment not found at ${VENV_DIR}." >&2
  echo "Run ./scripts/setup_environment.sh first." >&2
  exit 1
fi

# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

if [[ $# -gt 0 ]]; then
  YEARS=("$@")
else
  mapfile -t YEARS < <(find data -maxdepth 1 -type f -name 'atp_*.csv' -printf '%f\n' \
    | sed -E 's/atp_([0-9]{4})\.csv/\1/' \
    | sort -n)
fi

if [[ ${#YEARS[@]} -eq 0 ]]; then
  echo "Error: no yearly ATP files were found in data/." >&2
  exit 1
fi

echo "Running clean_stats for years: ${YEARS[*]}"
python pipeline/clean_stats.py --years "${YEARS[@]}"

echo "Running feature generation..."
python pipeline/features.py --match-windows 5 10 20 --day-windows 30 90 365

echo "Training evaluation model artifact..."
python pipeline/modeling.py --max-depth 6 --min-samples-leaf 120

echo
echo "Pipeline run complete."
