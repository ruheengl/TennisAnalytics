#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
API_PORT="${API_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"

if [[ ! -d "${VENV_DIR}" ]]; then
  echo "Error: virtual environment not found at ${VENV_DIR}." >&2
  echo "Run ./scripts/setup_environment.sh first." >&2
  exit 1
fi

# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

cleanup() {
  local exit_code=$?
  if [[ -n "${API_PID:-}" ]] && kill -0 "${API_PID}" >/dev/null 2>&1; then
    kill "${API_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${FRONTEND_PID:-}" ]] && kill -0 "${FRONTEND_PID}" >/dev/null 2>&1; then
    kill "${FRONTEND_PID}" >/dev/null 2>&1 || true
  fi
  exit "${exit_code}"
}
trap cleanup INT TERM EXIT

cd "${ROOT_DIR}"
echo "Starting API on http://localhost:${API_PORT} ..."
uvicorn api.server:app --reload --host 0.0.0.0 --port "${API_PORT}" &
API_PID=$!

cd "${ROOT_DIR}/frontend"
echo "Starting frontend on http://localhost:${FRONTEND_PORT} ..."
npm run dev -- --host 0.0.0.0 --port "${FRONTEND_PORT}" &
FRONTEND_PID=$!

echo
echo "Services started:"
echo "- API:      http://localhost:${API_PORT}"
echo "- Frontend: http://localhost:${FRONTEND_PORT}"
echo "Press Ctrl+C to stop both services."

wait "${API_PID}" "${FRONTEND_PID}"
