#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"

cd "${ROOT_DIR}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "Error: python3 is required but was not found in PATH." >&2
  exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "Error: npm is required but was not found in PATH." >&2
  exit 1
fi

if [[ ! -d "${VENV_DIR}" ]]; then
  echo "Creating Python virtual environment at ${VENV_DIR}..."
  python3 -m venv "${VENV_DIR}"
fi

# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

echo "Installing Python dependencies..."
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo "Installing frontend dependencies..."
cd "${ROOT_DIR}/frontend"
if [[ -f package-lock.json ]]; then
  npm ci
else
  npm install
fi

echo

echo "Setup complete."
echo "Use ./scripts/run_pipeline.sh to generate pipeline artifacts."
echo "Use ./scripts/start_local.sh to launch API + frontend on localhost."
