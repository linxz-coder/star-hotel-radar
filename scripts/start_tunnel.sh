#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/Users/linxiaozhong/development/star-hotel-deal-app}"
PORT="${PORT:-5013}"
APP_LABEL="${APP_LABEL:-com.linxz.star-hotel-deal.app}"
TUNNEL_LABEL="${TUNNEL_LABEL:-com.linxz.star-hotel-deal.tunnel}"
APP_PLIST="${APP_PLIST:-${APP_DIR}/deploy/launchd/${APP_LABEL}.plist}"
TUNNEL_PLIST="${TUNNEL_PLIST:-${APP_DIR}/deploy/launchd/${TUNNEL_LABEL}.plist}"
CLOUDFLARED="${CLOUDFLARED:-/opt/homebrew/bin/cloudflared}"
LOCAL_HEALTH_URL="http://127.0.0.1:${PORT}/api/health"
LOCAL_SITE_URL="http://127.0.0.1:${PORT}"
LOCAL_ADMIN_URL="${LOCAL_SITE_URL}/admin"
LOG_DIR="${APP_DIR}/.cache"
APP_LOG="${LOG_DIR}/app.out.log"
APP_ERR="${LOG_DIR}/app.err.log"
TUNNEL_OUT_LOG="${LOG_DIR}/cloudflared.log"
TUNNEL_ERR_LOG="${LOG_DIR}/cloudflared.err.log"
LATEST_URL_FILE="${LOG_DIR}/latest_tunnel_url.txt"
LATEST_ADMIN_URL_FILE="${LOG_DIR}/latest_admin_url.txt"
USER_ID="$(id -u)"

file_size() {
  local path="$1"
  if [[ -f "$path" ]]; then
    wc -c < "$path" | tr -d ' '
  else
    printf "0"
  fi
}

extract_tunnel_url() {
  grep -Eo 'https://[a-z0-9-]+\.trycloudflare\.com' | tail -n 1 || true
}

print_tunnel_urls() {
  local url="$1"
  local admin_url="${url}/admin"
  printf "%s\n" "$url" > "$LATEST_URL_FILE"
  printf "%s\n" "$admin_url" > "$LATEST_ADMIN_URL_FILE"
  echo
  echo "前台 Tunnel 地址：$url"
  echo "手机后台地址：$admin_url"
  echo "本地前台地址：$LOCAL_SITE_URL"
  echo "本地后台地址：$LOCAL_ADMIN_URL"
  echo "地址已保存：$LATEST_URL_FILE"
  echo "后台地址已保存：$LATEST_ADMIN_URL_FILE"
}

cd "$APP_DIR"
mkdir -p "$LOG_DIR"

if [[ ! -f "$APP_PLIST" ]]; then
  echo "本地应用 launchd 配置不存在：$APP_PLIST" >&2
  exit 1
fi

if [[ ! -f "$TUNNEL_PLIST" ]]; then
  echo "Tunnel launchd 配置不存在：$TUNNEL_PLIST" >&2
  exit 1
fi

if [[ ! -x "$CLOUDFLARED" ]]; then
  echo "没有找到可执行 cloudflared：$CLOUDFLARED" >&2
  echo "可用 CLOUDFLARED=/path/to/cloudflared scripts/start_tunnel.sh 指定路径。" >&2
  exit 1
fi

echo "注册/启动本地应用服务..."
launchctl bootstrap "gui/${USER_ID}" "$APP_PLIST" 2>/dev/null || true
launchctl kickstart -k "gui/${USER_ID}/${APP_LABEL}"

echo "等待本地服务可用..."
for _ in $(seq 1 30); do
  if curl --noproxy '*' -fsS "$LOCAL_HEALTH_URL" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl --noproxy '*' -fsS "$LOCAL_HEALTH_URL" >/dev/null 2>&1; then
  echo "本地服务没有启动成功，请查看：$APP_ERR" >&2
  exit 1
fi

before_out_size="$(file_size "$TUNNEL_OUT_LOG")"
before_err_size="$(file_size "$TUNNEL_ERR_LOG")"

echo "注册/启动 Cloudflare Quick Tunnel..."
launchctl bootstrap "gui/${USER_ID}" "$TUNNEL_PLIST" 2>/dev/null || true
launchctl kickstart -k "gui/${USER_ID}/${TUNNEL_LABEL}"

echo "等待公网地址..."
for _ in $(seq 1 60); do
  new_log=""
  if [[ -f "$TUNNEL_ERR_LOG" ]]; then
    new_log="${new_log}$(tail -c +"$((before_err_size + 1))" "$TUNNEL_ERR_LOG" 2>/dev/null || true)"
  fi
  if [[ -f "$TUNNEL_OUT_LOG" ]]; then
    new_log="${new_log}
$(tail -c +"$((before_out_size + 1))" "$TUNNEL_OUT_LOG" 2>/dev/null || true)"
  fi
  url="$(printf "%s\n" "$new_log" | extract_tunnel_url)"
  if [[ -n "$url" ]]; then
    print_tunnel_urls "$url"
    exit 0
  fi
  sleep 1
done

url="$(
  {
    tail -n 200 "$TUNNEL_ERR_LOG" 2>/dev/null || true
    tail -n 200 "$TUNNEL_OUT_LOG" 2>/dev/null || true
  } | extract_tunnel_url
)"
if [[ -n "$url" ]]; then
  print_tunnel_urls "$url"
  exit 0
fi

echo "没有自动识别到新地址，请查看日志：" >&2
echo "  $TUNNEL_ERR_LOG" >&2
echo "  $TUNNEL_OUT_LOG" >&2
tail -n 80 "$TUNNEL_ERR_LOG" >&2 || true
tail -n 80 "$TUNNEL_OUT_LOG" >&2 || true
exit 1
