#!/usr/bin/env bash
# verify-features.sh — the whole-product smoke proof, exercised through the
# public API exactly as a user (or the dashboard) would. Asserts the flagship
# surfaces that exist today: seed → pipelines with green diff badges → analytics
# cost model → column lineage → incidents endpoint → self-diagnostic.
#
# Targets a running stack. Point it with LAKESENSE_URL (the API base, default
# http://localhost:8080). Write-path assertions (create pipeline, inject
# failure, escalate, ack) are marked TODO until those endpoints ship — this
# proves the read/observability surface honestly, not a fiction.
#
# Usage:
#   LAKESENSE_URL=http://localhost:8080 scripts/verify-features.sh

set -uo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib.sh
source scripts/lib.sh

URL="${LAKESENSE_URL:-http://localhost:8080}"
API="$URL/api/v1"

section "LakeSense feature verification → $URL"

# --- reachability ---
check "control plane is up (/healthz 200)" bash -c "curl -fsS $URL/healthz -o /dev/null"

pipes="$(curl -fsS "$API/pipelines" 2>/dev/null)"
np="$(jqget "$pipes" 'length')"
assert "pipelines endpoint returns data (found ${np:-0})" "$([ "${np:-0}" -ge 1 ] && echo ok)"

# --- data-diff: every seeded pipeline verifies on its latest sync ---
section "Data-diff / correctness"
verified="$(jqget "$pipes" '[.[] | select(.diff_verified)] | length')"
assert "every pipeline's latest sync is verified (${verified:-0}/${np:-0})" \
  "$([ "${verified:-0}" -eq "${np:-0}" ] && [ "${np:-0}" -ge 1 ] && echo ok)"
rows="$(jqget "$pipes" '[.[].verified_rows] | add')"
assert "verified rows are counted (${rows:-0})" "$([ "${rows:-0}" -gt 0 ] && echo ok)"

# per-pipeline diff history exists
pid="$(jqget "$pipes" '.[0].id')"
diffs="$(curl -fsS "$API/pipelines/$pid/diffs" 2>/dev/null)"
nd="$(jqget "$diffs" 'length')"
assert "pipeline #$pid has diff history (${nd:-0} runs)" "$([ "${nd:-0}" -ge 1 ] && echo ok)"

# --- health scoring ---
section "Health & incidents"
health="$(jqget "$pipes" '.[0].health_score')"
assert "health score is 0..100 (got ${health})" \
  "$([ -n "$health" ] && [ "$health" -ge 0 ] && [ "$health" -le 100 ] && echo ok)"
check "incidents endpoint responds" bash -c "curl -fsS $API/incidents -o /dev/null"

# --- analytics / transparent cost model ---
section "Analytics & cost"
an="$(curl -fsS "$API/analytics" 2>/dev/null)"
cost="$(jqget "$an" '.total_est_cost_usd')"
assert "analytics returns a cost estimate (\$${cost})" "$([ -n "$cost" ] && echo ok)"
cpg="$(jqget "$an" '.cost_per_gb')"
assert "cost model is transparent (\$${cpg}/GB exposed)" "$([ -n "$cpg" ] && echo ok)"

# --- lineage ---
section "Column lineage"
lin="$(curl -fsS "$API/pipelines/$pid/lineage" 2>/dev/null)"
nl="$(jqget "$lin" 'length')"
assert "column lineage recorded (${nl:-0} edges on #$pid)" "$([ "${nl:-0}" -ge 1 ] && echo ok)"

# --- audit endpoint ---
check "audit log endpoint responds" bash -c "curl -fsS $API/audit -o /dev/null"

printf "\n  ${C_DIM}TODO (needs write endpoints): create-pipeline, inject-failure→alert→escalate→ack,${C_RESET}\n"
printf "  ${C_DIM}config-version rollback, environment promotion, backfill window.${C_RESET}\n"

summary
