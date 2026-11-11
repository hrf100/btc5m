#!/usr/bin/env bash
# 服务器一行启动:
#   curl -fsSL https://raw.githubusercontent.com/hrf100/btc5m/main/run.sh | bash
# 已有目录时, 同一条命令会先快进更新再启动。
set -euo pipefail

REPO="https://github.com/hrf100/btc5m.git"
INSTALL_DIR="${BTC5M_DIR:-$HOME/btc5m}"

if [ -n "${BASH_SOURCE[0]:-}" ] && [ -f "${BASH_SOURCE[0]}" ]; then
  SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  if [ -f "$SRC/btc5m.py" ]; then
    INSTALL_DIR="$SRC"
  fi
fi

if [ ! -f "$INSTALL_DIR/btc5m.py" ]; then
  git clone "$REPO" "$INSTALL_DIR"
elif [ -d "$INSTALL_DIR/.git" ]; then
  git -C "$INSTALL_DIR" pull --ff-only
fi

if [ "${BTC5M_INNER:-}" != 1 ]; then
  export BTC5M_INNER=1
  exec bash "$INSTALL_DIR/run.sh"
fi

cd "$INSTALL_DIR"

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
exec .venv/bin/python btc5m.py
