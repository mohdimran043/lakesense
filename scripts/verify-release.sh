#!/usr/bin/env bash
# verify-release.sh — clean-machine simulation. Exports ONLY the committed tree
# (git archive HEAD → no node_modules, no build artifacts, no uncommitted work),
# then follows the README quickstart literally: bring up the compose stack, seed
# demo data, and assert the dashboard and API respond. Run before every tag.
#
# Requires docker. Ports are auto-picked to avoid host collisions.

set -uo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
# shellcheck source=scripts/lib.sh
source scripts/lib.sh

command -v docker >/dev/null || { echo "docker required" >&2; exit 2; }

TMP="$(mktemp -d)"
PROJECT="lsrelease$$"
cleanup() {
  (cd "$TMP/deploy" 2>/dev/null && docker compose -p "$PROJECT" down -v >/dev/null 2>&1)
  rm -rf "$TMP"
}
trap cleanup EXIT

pick_port() { python3 -c 'import socket;s=socket.socket();s.bind(("",0));print(s.getsockname()[1]);s.close()'; }
API_PORT="$(pick_port)"
UI_PORT="$(pick_port)"

section "Clean-machine release check (committed tree only)"
check "export committed tree (git archive HEAD)" bash -c "git -C '$ROOT' archive HEAD | tar -x -C '$TMP'"
assert "exported tree has no node_modules" "$([ ! -d "$TMP/frontend/node_modules" ] && echo ok)"
assert "exported tree has deploy/docker-compose.yml" "$([ -f "$TMP/deploy/docker-compose.yml" ] && echo ok)"

cd "$TMP/deploy"
cp .env.example .env
export LAKESENSE_PORT="$API_PORT" LAKESENSE_UI_PORT="$UI_PORT"

section "Follow the README quickstart"
printf "  building images (first run, no cache) …\n"
check "docker compose up --build" bash -c "timeout 600 docker compose -p '$PROJECT' up -d --build >/tmp/lsrel.log 2>&1"

printf "  waiting for backend health …\n"
healthy=""
for _ in $(seq 1 50); do
  if [ "$(docker compose -p "$PROJECT" ps backend --format '{{.Health}}' 2>/dev/null)" = "healthy" ]; then
    healthy=ok; break
  fi
  sleep 3
done
assert "backend reports healthy (doctor healthcheck)" "$healthy"

check "seed demo data" bash -c "docker compose -p '$PROJECT' run --rm -T backend seed --days 7 >/dev/null 2>&1"

# Dashboard (served by nginx) and API (proxied through it).
assert "dashboard responds (:$UI_PORT)" "$(curl -fsS "localhost:$UI_PORT" 2>/dev/null | grep -q LakeSense && echo ok)"
np="$(curl -fsS "localhost:$UI_PORT/api/v1/pipelines" 2>/dev/null | jq -r 'length' 2>/dev/null)"
assert "API returns seeded pipelines through nginx (${np:-0})" "$([ "${np:-0}" -ge 1 ] && echo ok)"

summary
