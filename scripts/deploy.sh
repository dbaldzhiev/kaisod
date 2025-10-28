#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "This script must be run as root" >&2
  exit 1
fi

BRANCH=${BRANCH:-main}
APP_USER=${APP_USER:-kais-monitor}
INSTALL_DIR=${INSTALL_DIR:-/opt/kais-monitor}
SERVICE_NAME=${SERVICE_NAME:-kais-monitor}
PYTHON_BIN=${PYTHON_BIN:-python3}

OVERRIDE_DATA_DIR="${DATA_DIR:-}"
POSITIONAL_BRANCH=""
while (($#)); do
  case "$1" in
    BRANCH=*)
      BRANCH="${1#BRANCH=}"
      ;;
    REPO_URL=*)
      REPO_URL="${1#REPO_URL=}"
      ;;
    APP_USER=*)
      APP_USER="${1#APP_USER=}"
      ;;
    INSTALL_DIR=*)
      INSTALL_DIR="${1#INSTALL_DIR=}"
      ;;
    SERVICE_NAME=*)
      SERVICE_NAME="${1#SERVICE_NAME=}"
      ;;
    PYTHON_BIN=*)
      PYTHON_BIN="${1#PYTHON_BIN=}"
      ;;
    DATA_DIR=*)
      OVERRIDE_DATA_DIR="${1#DATA_DIR=}"
      ;;
    -* )
      echo "Unknown option: $1" >&2
      exit 1
      ;;
    * )
      if [[ -z "$POSITIONAL_BRANCH" ]]; then
        POSITIONAL_BRANCH="$1"
      else
        echo "Unexpected argument: $1" >&2
        exit 1
      fi
      ;;
  esac
  shift
done

if [[ -n "$POSITIONAL_BRANCH" ]]; then
  BRANCH="$POSITIONAL_BRANCH"
fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)
DEFAULT_REPO_URL=$(git -C "$REPO_ROOT" config --get remote.origin.url 2>/dev/null || true)
REPO_URL=${REPO_URL:-$DEFAULT_REPO_URL}
if [[ -z "$REPO_URL" ]]; then
  echo "Unable to determine repository URL. Set the REPO_URL environment variable." >&2
  exit 1
fi

if [[ -n "$OVERRIDE_DATA_DIR" ]]; then
  DATA_DIR="$OVERRIDE_DATA_DIR"
else
  DATA_DIR=$(realpath -m "${INSTALL_DIR}/data")
fi

SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

apt-get update
apt-get install -y git python3 python3-venv

if ! id -u "$APP_USER" >/dev/null 2>&1; then
  useradd --system --create-home --shell /usr/sbin/nologin "$APP_USER"
fi

if [[ -d "$INSTALL_DIR/.git" ]]; then
  git -C "$INSTALL_DIR" fetch origin
else
  rm -rf "$INSTALL_DIR"
  git clone "$REPO_URL" "$INSTALL_DIR"
fi

git -C "$INSTALL_DIR" checkout "$BRANCH"
git -C "$INSTALL_DIR" pull --ff-only origin "$BRANCH"

"$PYTHON_BIN" -m venv "$INSTALL_DIR/.venv"
source "$INSTALL_DIR/.venv/bin/activate"
pip install --upgrade pip
pip install -r "$INSTALL_DIR/requirements.txt"
deactivate

mkdir -p "$DATA_DIR"
chown -R "$APP_USER":"$APP_USER" "$DATA_DIR"
chown -R "$APP_USER":"$APP_USER" "$INSTALL_DIR"

cat <<SERVICE > "$SERVICE_FILE"
[Unit]
Description=KAIS Monitor service
After=network.target

[Service]
Type=simple
User=${APP_USER}
Group=${APP_USER}
WorkingDirectory=${INSTALL_DIR}
Environment="PYTHONUNBUFFERED=1"
Environment="KAIS_MONITOR_BASE=${DATA_DIR}"
ExecStart=${INSTALL_DIR}/.venv/bin/python ${INSTALL_DIR}/server/app.py
Restart=on-failure

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable --now "$SERVICE_NAME"

echo "Deployment completed. Service '${SERVICE_NAME}' is running." >&2
