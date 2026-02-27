#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/illia/youtube_direct_bot"
USER_UNIT_SRC="$ROOT_DIR/systemd-user/ytdl-web.service"
USER_UNIT_DST="$HOME/.config/systemd/user/ytdl-web.service"
NGINX_SRC="$ROOT_DIR/nginx/ytdl-with-web-ui.conf"
NGINX_DST="/etc/nginx/sites-available/ytdl"
PYTHON_BIN="/home/illia/youtube_direct_bot/.venv/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

echo "[1/5] Validate cookies before deployment..."
"$PYTHON_BIN" "$ROOT_DIR/scripts/validate_cookies.py" --cookies-file "/home/illia/youtube_direct_bot/cookies.txt"

echo "[2/5] Install user systemd unit..."
mkdir -p "$HOME/.config/systemd/user"
cp "$USER_UNIT_SRC" "$USER_UNIT_DST"

echo "[3/5] Reload and start ytdl-web.service..."
export XDG_RUNTIME_DIR="/run/user/$(id -u)"
export DBUS_SESSION_BUS_ADDRESS="unix:path=$XDG_RUNTIME_DIR/bus"
systemctl --user daemon-reload
systemctl --user enable --now ytdl-web.service
systemctl --user status ytdl-web.service --no-pager || true

echo "[4/5] Install nginx site config (sudo required)..."
sudo cp "$NGINX_SRC" "$NGINX_DST"
sudo nginx -t
sudo systemctl reload nginx

echo "[5/5] Quick checks..."
pgrep -fa "web_app.py" || true
curl -I https://wetwilly.tech/ || true
curl -I https://wetwilly.tech/downloads/ || true
curl -I https://wetwilly.tech/youtube-direct-bot-downloads/ || true

echo "Done. Web UI should be available at: https://wetwilly.tech/"
