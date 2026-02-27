#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/home/illia/youtube_direct_bot/.venv/bin/python}"
COOKIES_FILE="${COOKIES_FILE:-$ROOT_DIR/cookies.txt}"
BROWSER="${1:-${YT_COOKIES_FROM_BROWSER:-chrome}}"
MAX_AGE_HOURS="${COOKIE_MAX_AGE_HOURS:-6}"

export COOKIES_FILE
export YT_COOKIES_FROM_BROWSER="$BROWSER"

echo "[1/2] Refresh cookies from browser profile: $YT_COOKIES_FROM_BROWSER"
"$PYTHON_BIN" "$ROOT_DIR/scripts/refresh_cookies_from_browser.py"

echo "[2/2] Validate refreshed cookies: $COOKIES_FILE"
"$PYTHON_BIN" "$ROOT_DIR/scripts/validate_cookies.py" \
  --cookies-file "$COOKIES_FILE" \
  --max-age-hours "$MAX_AGE_HOURS"

echo "Cookie refresh + validation completed."
