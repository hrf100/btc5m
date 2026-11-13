#!/usr/bin/env bash
# 长久运行服务：一行安装/更新并启动
#   curl -fsSL https://raw.githubusercontent.com/hrf100/btc5m/main/test4/run.sh | bash
# 启动或更新前会检查并关闭已有进程/服务，再拉代码重启。
# 私钥放在 ~/.btc5m-4.env 的 POLY_PRIVATE_KEY。
set -euo pipefail

REPO="https://github.com/hrf100/btc5m.git"
ROOT="${BTC5M4_DIR:-$HOME/btc5m-4}"
PORT="${BTC5M4_PORT:-9999}"
BIND="${BTC5M4_BIND:-0.0.0.0}"
SERVICE="${BTC5M4_SERVICE:-btc5m-4}"
UNIT="/etc/systemd/system/${SERVICE}.service"
ENVF="${BTC5M4_ENV:-$HOME/.btc5m-4.env}"
LOG_DIR="${BTC5M4_LOG:-$ROOT/test4/logs}"

say() { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*"; }

if [ -n "${BASH_SOURCE[0]:-}" ] && [ -f "${BASH_SOURCE[0]}" ]; then
  SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  if [ -f "$SRC/btc5m-4.py" ]; then
    ROOT="$(cd "$SRC/.." && pwd)"
  fi
fi

# ---------- 1. 先停掉已有服务/进程，避免端口占用和双开 ----------
stop_existing() {
  local pids=""
  if command -v systemctl >/dev/null 2>&1 && [ -f "$UNIT" ]; then
    if systemctl is-active --quiet "$SERVICE" 2>/dev/null; then
      say "检测到 systemd 服务 ${SERVICE} 在运行，先停止"
      if [ "$(id -u)" -eq 0 ]; then
        systemctl stop "$SERVICE" || true
      else
        sudo systemctl stop "$SERVICE" || true
      fi
      sleep 1
    fi
  fi

  pids="$(pgrep -f '(^|/)btc5m-4\.py([[:space:]]|$)' 2>/dev/null || true)"
  if [ -z "$pids" ]; then
    pids="$(pgrep -f '[.]venv/bin/python .*btc5m-4\.py' 2>/dev/null || true)"
  fi
  if [ -n "$pids" ]; then
    say "检测到进程仍在运行: $pids ，先关闭"
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
    sleep 2
    # shellcheck disable=SC2086
    kill -9 $pids 2>/dev/null || true
    sleep 1
  fi

  if command -v ss >/dev/null 2>&1; then
    if ss -ltn "sport = :$PORT" 2>/dev/null | grep -q ":$PORT"; then
      warn "端口 ${PORT} 仍被占用，尝试释放"
      if command -v fuser >/dev/null 2>&1; then
        fuser -k "${PORT}/tcp" 2>/dev/null || true
        sleep 1
      fi
    fi
  fi
}

stop_existing

# ---------- 2. 拉取/更新代码 ----------
if [ ! -f "$ROOT/test4/btc5m-4.py" ]; then
  say "克隆仓库到 $ROOT"
  git clone "$REPO" "$ROOT"
elif [ -d "$ROOT/.git" ]; then
  say "更新代码"
  git -C "$ROOT" pull --ff-only
fi

# curl 管道里跑的是旧脚本；拉完后改跑仓库里的最新 run.sh
if [ "${BTC5M4_INNER:-}" != 1 ]; then
  export BTC5M4_INNER=1
  exec bash "$ROOT/test4/run.sh"
fi

cd "$ROOT/test4"
mkdir -p "$LOG_DIR" data

# ---------- 3. Python 环境 ----------
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
  say "正在安装 python3-venv"
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
  warn "虚拟环境仍没有 pip，改用 get-pip 安装"
  curl -fsSL https://bootstrap.pypa.io/get-pip.py | .venv/bin/python
fi

.venv/bin/python -m pip install -r requirements.txt

if [ ! -f "$ENVF" ]; then
  warn "未找到 $ENVF ，可稍后写入 POLY_PRIVATE_KEY"
  touch "$ENVF"
  chmod 600 "$ENVF"
fi

# ---------- 4. 注册长久运行服务（systemd）并启动 ----------
install_systemd() {
  local py="$ROOT/test4/.venv/bin/python"
  local app="$ROOT/test4/btc5m-4.py"
  local tmp
  tmp="$(mktemp)"
  cat > "$tmp" <<EOF
[Unit]
Description=btc5m-4 Polymarket BTC 5m dashboard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$ROOT/test4
EnvironmentFile=-$ENVF
Environment=BTC5M4_PORT=$PORT
Environment=BTC5M4_BIND=$BIND
ExecStart=$py $app --host $BIND --port $PORT
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=20
MemoryMax=800M
Nice=5
StandardOutput=append:$LOG_DIR/out.log
StandardError=append:$LOG_DIR/err.log

[Install]
WantedBy=multi-user.target
EOF
  if [ "$(id -u)" -eq 0 ]; then
    mv "$tmp" "$UNIT"
    systemctl daemon-reload
    systemctl enable "$SERVICE" >/dev/null 2>&1 || true
    systemctl restart "$SERVICE"
  else
    sudo mv "$tmp" "$UNIT"
    sudo systemctl daemon-reload
    sudo systemctl enable "$SERVICE" >/dev/null 2>&1 || true
    sudo systemctl restart "$SERVICE"
  fi
}

start_background() {
  warn "没有可用的 systemd，改为 nohup 后台运行"
  nohup .venv/bin/python btc5m-4.py --host "$BIND" --port "$PORT" \
    >>"$LOG_DIR/out.log" 2>>"$LOG_DIR/err.log" &
  echo $! > "$LOG_DIR/btc5m-4.pid"
  sleep 1
}

if command -v systemctl >/dev/null 2>&1 && { [ "$(id -u)" -eq 0 ] || command -v sudo >/dev/null 2>&1; }; then
  say "安装并启动 systemd 服务: $SERVICE"
  install_systemd
  sleep 2
  if systemctl is-active --quiet "$SERVICE" 2>/dev/null || sudo systemctl is-active --quiet "$SERVICE" 2>/dev/null; then
    say "服务已启动并设为开机自启"
  else
    warn "systemd 启动异常，回退到后台进程"
    start_background
  fi
else
  stop_existing
  start_background
fi

PUB_IP=$(curl -fsS --max-time 3 https://api.ipify.org 2>/dev/null || hostname -I 2>/dev/null | awk '{print $1}')
echo
say "看板: http://${PUB_IP:-127.0.0.1}:${PORT}/"
echo "  状态: systemctl status ${SERVICE}"
echo "  日志: journalctl -u ${SERVICE} -f   或   tail -f ${LOG_DIR}/out.log"
echo "  环境: ${ENVF}"
echo "  再次执行同一条 curl 命令即可更新并重启"
