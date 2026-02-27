#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/illia/youtube_direct_bot"
USER_SYSTEMD_DIR="$HOME/.config/systemd/user"
UNITS=("ytdl-bot.service" "ytdl-web.service")
PYTHON_BIN="/home/illia/youtube_direct_bot/.venv/bin/python"
COOKIE_VALIDATOR="$ROOT_DIR/scripts/validate_cookies.py"
COOKIE_REFRESHER="$ROOT_DIR/scripts/refresh_cookies_from_browser.py"
COOKIE_FILE="${COOKIES_FILE:-$ROOT_DIR/cookies.txt}"
FALLBACK_COOKIE_FILE="${FALLBACK_COOKIE_FILE:-/home/illia/youtube_direct_bot/cookies.txt}"

# Keep current GUI session DBus/keyring vars if already present.
# Fallback to standard user bus only when values are missing.
if [[ -z "${XDG_RUNTIME_DIR:-}" ]]; then
  export XDG_RUNTIME_DIR="/run/user/$(id -u)"
fi
if [[ -z "${DBUS_SESSION_BUS_ADDRESS:-}" ]] && [[ -S "$XDG_RUNTIME_DIR/bus" ]]; then
  export DBUS_SESSION_BUS_ADDRESS="unix:path=$XDG_RUNTIME_DIR/bus"
fi

if ! systemctl --user show-environment >/dev/null 2>&1; then
  echo "Could not reach systemd --user manager via current DBus session."
  echo "Run this script from the same logged-in desktop session where browser/keyring is available."
  exit 1
fi

mkdir -p "$USER_SYSTEMD_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

echo "[1/8] Auto-generate OpenAPI (Swagger source)..."
"$PYTHON_BIN" "$ROOT_DIR/scripts/generate_swagger.py" \
  --web-app "$ROOT_DIR/web_app.py" \
  --output "$ROOT_DIR/web/openapi.auto.json"

echo "[2/8] Ensure cookies file exists..."
if [[ ! -s "$COOKIE_FILE" ]]; then
  echo "cookies file missing/empty: $COOKIE_FILE"
  echo "Attempting browser tab refresh + cookies export..."
  export COOKIES_FILE="$COOKIE_FILE"
  export YT_COOKIES_FROM_BROWSER="${YT_COOKIES_FROM_BROWSER:-chrome:Default}"
  export COOKIE_TOUCH_YOUTUBE_BEFORE_EXPORT="${COOKIE_TOUCH_YOUTUBE_BEFORE_EXPORT:-1}"
  export COOKIE_REQUIRE_OPEN_YOUTUBE="${COOKIE_REQUIRE_OPEN_YOUTUBE:-1}"
  export COOKIE_TOUCH_WAIT_SEC="${COOKIE_TOUCH_WAIT_SEC:-8}"
  if ! "$PYTHON_BIN" "$COOKIE_REFRESHER"; then
    echo "Cookie export failed from browser keyring."
    if [[ -s "$FALLBACK_COOKIE_FILE" ]]; then
      echo "Falling back to copy cookies from: $FALLBACK_COOKIE_FILE"
      cp "$FALLBACK_COOKIE_FILE" "$COOKIE_FILE"
      chmod 600 "$COOKIE_FILE" || true
    else
      echo "No fallback cookies file found at: $FALLBACK_COOKIE_FILE"
      echo "Open a logged-in YouTube tab in browser and rerun."
      exit 1
    fi
  fi
  for _ in $(seq 1 15); do
    [[ -s "$COOKIE_FILE" ]] && break
    sleep 1
  done
fi

echo "[3/8] Validate cookies before restart..."
"$PYTHON_BIN" "$COOKIE_VALIDATOR" --cookies-file "$COOKIE_FILE"

echo "[4/8] Sync systemd user units..."
for unit in "${UNITS[@]}"; do
  src="$ROOT_DIR/systemd-user/$unit"
  dst="$USER_SYSTEMD_DIR/$unit"
  if [[ ! -f "$src" ]]; then
    echo "Missing unit template: $src"
    exit 1
  fi
  cp "$src" "$dst"
done

echo "[5/8] Reload systemd user daemon..."
systemctl --user daemon-reload

echo "[6/8] Import GUI session env for browser-keyring cookies..."
for name in DISPLAY DBUS_SESSION_BUS_ADDRESS XAUTHORITY; do
  if [[ -n "${!name:-}" ]]; then
    systemctl --user import-environment "$name" || true
    systemctl --user set-environment "$name=${!name}" || true
  fi
done
if command -v dbus-update-activation-environment >/dev/null 2>&1; then
  dbus-update-activation-environment --systemd DISPLAY DBUS_SESSION_BUS_ADDRESS XAUTHORITY || true
fi
echo "systemd --user env snapshot:"
systemctl --user show-environment | grep -E '^(DISPLAY|DBUS_SESSION_BUS_ADDRESS|XAUTHORITY)=' || true

echo "[7/8] Restart bot + web services..."
systemctl --user reset-failed "${UNITS[@]}" || true
for unit in "${UNITS[@]}"; do
  systemctl --user enable --now "$unit"
  systemctl --user restart "$unit"
done

echo "[8/8] Service status..."
systemctl --user --no-pager --full status "${UNITS[@]}" || true

echo
echo "Web UI: http://127.0.0.1:8088/"
echo "Swagger: http://127.0.0.1:8088/swagger"
