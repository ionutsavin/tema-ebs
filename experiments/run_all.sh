#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
uv run --project ../python-network python3 run.py
