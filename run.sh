#!/usr/bin/env bash
set -euo pipefail

# Usage: ./run.sh [YYYY-MM-DD]
# Runs the full pipeline for today, a date passed as an argument,
# or RUN_DATE if set in .env.

if [[ ! -f .env ]]; then
    cp .env.example .env
    echo "Created .env from .env.example — set PIPELINE_SECRET_KEY before running again."
    exit 1
fi

set -a; source .env; set +a

export RUN_DATE="${1:-${RUN_DATE:-$(date +%F)}}"

docker compose up --build