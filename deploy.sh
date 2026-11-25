#!/usr/bin/env bash
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DEPLOY_DIR"

echo "=== Deploying VisionX WebInput Service ==="
echo "Working directory: $DEPLOY_DIR"

git fetch origin main || true
git reset --hard origin/main || true

docker compose down --remove-orphans || true
docker compose up -d --build --remove-orphans

echo "Waiting for webinput service to become healthy..."
docker compose ps
