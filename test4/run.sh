#!/usr/bin/env bash
# 服务器一行启动（与 btc5m 采集脚本分开，装到 ~/btc5m-4）:
#   curl -fsSL https://raw.githubusercontent.com/hrf100/btc5m/main/test4/run.sh | bash
# 再执行一次会先更新再重启。私钥放在 ~/.btc5m-4.env 的 POLY_PRIVATE_KEY。
set -euo pipefail

REPO="https://github.com/hrf100/btc5m.git"
ROOT="${BTC5M4_DIR:-$HOME/btc5m-4}"
PORT="${BTC5M4_PORT:-9999}"

if [ -n "${BASH_SOURCE[0]:-}" ] && [ -f "${BASH_SOURCE[0]}" ]; then
  SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  if [ -f "$SRC/btc5m-4.py" ]; then
    ROOT="$(cd "$SRC/.." && pwd)"
  fi
fi

if [ ! -f "$ROOT/test4/btc5m-4.py" ]; then
  git clone "$REPO" "$ROOT"
elif [ -d "$ROOT/.git" ]; then
  git -C "$ROOT" pull --ff-only
fi

cd "$ROOT/test4"

if ! command -v python3 >/dev/null 2>&1; then
  echo "需要 python3"
  exit 1
fi

if [ ! -x .venv/bin/python ]; then
  if ! python3 -m venv .venv; then
    sudo apt-get update
    sudo apt-get install -y python3-venv
    rm -rf .venv
    python3 -m venv .venv
  fi
fi

.venv/bin/python -m pip install -r requirements.txt

if [ -f "$HOME/.btc5m-4.env" ]; then
  set -a
  . "$HOME/.btc5m-4.env"
  set +a
fi

pkill -f '[.]venv/bin/python btc5m-4.py' || true
sleep 1

PUB_IP=$(curl -fsS --max-time 3 https://api.ipify.org || hostname -I | awk '{print $1}')
echo "看板: http://${PUB_IP}:${PORT}/"
exec .venv/bin/python btc5m-4.py --port "$PORT"
