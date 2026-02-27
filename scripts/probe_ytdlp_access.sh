#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/home/illia/youtube_direct_bot/.venv/bin/python}"
COOKIES_FILE="${COOKIES_FILE:-$ROOT_DIR/cookies.txt}"

usage() {
  cat <<EOF
Usage:
  $0 [--browser <name[:profile]>] [--browser-only] [--cookies-file <path>] <youtube-url-or-video-id>

Examples:
  $0 wyGjjST4tFY
  $0 --browser chrome:Default --browser-only wyGjjST4tFY
  $0 --browser firefox --browser-only "https://www.youtube.com/watch?v=wyGjjST4tFY"
EOF
}

RAW_TARGET=""
BROWSER_ARG="${YT_COOKIES_FROM_BROWSER:-}"
BROWSER_ONLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --browser)
      shift
      [[ $# -gt 0 ]] || { echo "Missing value for --browser" >&2; usage; exit 2; }
      BROWSER_ARG="$1"
      ;;
    --browser-only)
      BROWSER_ONLY=1
      ;;
    --cookies-file)
      shift
      [[ $# -gt 0 ]] || { echo "Missing value for --cookies-file" >&2; usage; exit 2; }
      COOKIES_FILE="$1"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -z "$RAW_TARGET" ]]; then
        RAW_TARGET="$1"
      else
        echo "Unexpected argument: $1" >&2
        usage
        exit 2
      fi
      ;;
  esac
  shift
done

if [[ -z "$RAW_TARGET" ]]; then
  usage
  exit 2
fi

if [[ "$RAW_TARGET" =~ ^[A-Za-z0-9_-]{6,20}$ ]]; then
  URL="https://www.youtube.com/watch?v=${RAW_TARGET}"
else
  URL="$RAW_TARGET"
fi

if [[ -n "$BROWSER_ARG" ]]; then
  BROWSER_ONLY=1
fi

run_probe() {
  local label="$1"
  shift
  local -a cmd=(
    "$PYTHON_BIN" -m yt_dlp
    --no-playlist
    --no-warnings
    --js-runtimes node
    --remote-components ejs:github
  )
  if [[ -n "$BROWSER_ARG" ]]; then
    cmd+=(--cookies-from-browser "$BROWSER_ARG")
  fi
  if [[ "$BROWSER_ONLY" -ne 1 ]]; then
    cmd+=(--cookies "$COOKIES_FILE")
  fi
  cmd+=("$@" --print "%(id)s | %(title)s" "$URL")
  if [[ -n "${YTDLP_PROXY:-}" ]]; then
    cmd+=(--proxy "$YTDLP_PROXY")
  fi

  echo "=== ${label} ==="
  echo "URL: $URL"
  if [[ -n "$BROWSER_ARG" ]]; then
    echo "Browser cookies: $BROWSER_ARG"
    echo "Mode: browser-only"
  else
    echo "Cookies file: $COOKIES_FILE"
    echo "Mode: file-based"
  fi
  if output="$("${cmd[@]}" 2>&1)"; then
    echo "$output"
    echo "Probe status: OK"
    return 0
  fi
  echo "$output" | tail -n 40
  if echo "$output" | grep -Eqi "failed to read from keyring|keyring"; then
    echo "Hint: run this from the same GUI desktop session where browser login/keyring is unlocked."
  fi
  echo "Probe status: FAILED (${label})"
  return 1
}

if run_probe "default"; then
  exit 0
fi

echo "Retrying with extractor fallback variants..."
if run_probe "android,ios,web" --extractor-args "youtube:player_client=android,ios,web"; then
  exit 0
fi
if run_probe "tv_embedded,web_safari" --extractor-args "youtube:player_client=tv_embedded,web_safari"; then
  exit 0
fi

echo "All probes failed."
exit 1
