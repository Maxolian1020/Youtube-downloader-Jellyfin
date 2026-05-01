#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -x .venv/bin/python ]; then
  echo "Missing .venv. Create it with: python3 -m venv .venv"
  exit 1
fi

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

exec .venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000
