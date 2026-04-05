#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  source "${ROOT_DIR}/.env"
  set +a
fi

: "${WB_DEPLOY_HOST:?WB_DEPLOY_HOST is required}"
WB_DEPLOY_USER="${WB_DEPLOY_USER:-root}"
WB_DEPLOY_PATH="${WB_DEPLOY_PATH:-/opt/wb-sdr-air-grabberer}"

ssh "${WB_DEPLOY_USER}@${WB_DEPLOY_HOST}" "mkdir -p '${WB_DEPLOY_PATH}'"
rsync -az --delete \
  --exclude '.git' \
  --exclude '.env' \
  --exclude 'data' \
  "${ROOT_DIR}/" "${WB_DEPLOY_USER}@${WB_DEPLOY_HOST}:${WB_DEPLOY_PATH}/"
scp "${ROOT_DIR}/.env" "${WB_DEPLOY_USER}@${WB_DEPLOY_HOST}:${WB_DEPLOY_PATH}/.env"
ssh "${WB_DEPLOY_USER}@${WB_DEPLOY_HOST}" "cd '${WB_DEPLOY_PATH}' && docker compose up -d --build"
