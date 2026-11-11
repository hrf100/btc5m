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

# curl 下载的是当时的脚本。拉完代码后改跑仓库里的这一份，避免管道里的旧脚本继续往下执行。
if [ "${BTC5M4_INNER:-}" != 1 ]; then
  export BTC5M4_INNER=1
  exec bash "$ROOT/test4/run.sh"
fi

cd "$ROOT/test4"

if ! command -v python3 >/dev/null 2>&1; then
  echo "需要 python3"
  exit 1
fi

has_pip() { .venv/bin/python -m pip --version >/dev/null 2>&1; }
if ! has_pip; then
  rm -rf .venv
  python3 -m venv .venv || true
fi
if ! has_pip; then
  echo "正在安装 python3-venv"
  export DEBIAN_FRONTEND=noninteractive
  if [ "$(id -u)" -eq 0 ]; then
    apt-get update
    apt-get install -y python3-venv python3-pip
  else
    sudo apt-get update
    sudo apt-get install -y python3-venv python3-pip
  fi
  rm -rf .venv
  python3 -m venv .venv || true
fi
if ! has_pip; then
  echo "虚拟环境仍没有 pip，改用 get-pip 安装"
  curl -fsSL https://bootstrap.pypa.io/get-pip.py | .venv/bin/python
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
