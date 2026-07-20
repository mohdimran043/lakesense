#!/usr/bin/env bash
# verify-migration.sh — the migration-correctness proof, end to end, for a
# locally-testable source. Seeds a source with a known dataset, runs a full sync
# through lsengine, and asserts the engine's own source/destination checksums
# match and the row count is exact. This is the credibility check: a wrong
# "verified" is worse than no badge, so it gets a real end-to-end test.
#
# Usage:
#   scripts/verify-migration.sh sqlite      # the self-contained default
#   scripts/verify-migration.sh all         # every locally-testable source
#
# sqlite needs no container. postgres/mysql/etc. require their throwaway
# containers (deploy/test-compose.yml, when present) and are marked pending here.

set -uo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib.sh
source scripts/lib.sh

SOURCE="${1:-sqlite}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

build_engine() {
  (cd engine && go build -o "$WORK/lsengine" ./cmd/lsengine)
}

verify_sqlite() {
  section "Migration proof: sqlite → ndjson"
  check "lsengine builds" build_engine

  # Deterministic dataset: 500 rows, known content.
  python3 - "$WORK/demo.db" <<'PY'
import sqlite3, sys
db = sqlite3.connect(sys.argv[1])
db.execute("CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT, price REAL, updated_at TEXT)")
db.executemany("INSERT INTO widgets VALUES(?,?,?,?)",
               [(i, f"w{i}", i + 0.5, f"2026-01-{(i % 28) + 1:02d}T00:00:00Z") for i in range(1, 501)])
db.commit(); db.close()
PY
  assert "source seeded with 500 known rows" "$([ -f "$WORK/demo.db" ] && echo ok)"

  echo "{\"type\":\"sqlite\",\"path\":\"$WORK/demo.db\",\"chunk_rows\":128}" > "$WORK/src.json"
  echo "{\"type\":\"ndjson\",\"path\":\"$WORK/out\"}" > "$WORK/dst.json"

  # Build the catalog from discover, select full_load.
  "$WORK/lsengine" discover --config "$WORK/src.json" \
    | python3 -c 'import json,sys;c=json.load(sys.stdin);c["selected_streams"]=[{"namespace":"main","name":"widgets","mode":"full_load"}];open("'"$WORK"'/cat.json","w").write(json.dumps(c))'
  assert "discover produced a catalog" "$([ -f "$WORK/cat.json" ] && echo ok)"

  # Run the sync, capture the JSONL event stream.
  "$WORK/lsengine" sync --config "$WORK/src.json" --destination "$WORK/dst.json" \
    --catalog "$WORK/cat.json" --state "$WORK/state.json" > "$WORK/events.jsonl" 2>/dev/null

  # Extract source/destination checksums and counts from the events.
  read -r src_rows src_sum dst_rows dst_sum written < <(python3 - "$WORK/events.jsonl" <<'PY'
import json, sys
src_rows = src_sum = dst_rows = dst_sum = written = ""
for line in open(sys.argv[1]):
    e = json.loads(line); p = e.get("payload", {})
    if e["event"] == "checksum_computed":
        if p["side"] == "source": src_rows, src_sum = p["rows"], p["checksum"]
        else: dst_rows, dst_sum = p["rows"], p["checksum"]
    if e["event"] == "sync_finished": written = p["rows_written"]
print(src_rows, src_sum, dst_rows, dst_sum, written)
PY
)

  assert "source read 500 rows (got ${src_rows})" "$([ "${src_rows:-0}" = "500" ] && echo ok)"
  assert "destination wrote 500 rows (got ${dst_rows})" "$([ "${dst_rows:-0}" = "500" ] && echo ok)"
  assert "sync_finished reports 500 rows (got ${written})" "$([ "${written:-0}" = "500" ] && echo ok)"
  assert "source & destination checksums MATCH" "$([ -n "$src_sum" ] && [ "$src_sum" = "$dst_sum" ] && echo ok)"

  # Independently confirm the output file has exactly 500 lines with metadata.
  lines="$(wc -l < "$WORK/out/main.widgets.ndjson" 2>/dev/null | tr -d ' ')"
  assert "output file has 500 rows" "$([ "${lines:-0}" = "500" ] && echo ok)"
  meta="$(head -1 "$WORK/out/main.widgets.ndjson" 2>/dev/null | grep -c _ls_id)"
  assert "engine metadata injected (_ls_id present)" "$([ "${meta:-0}" = "1" ] && echo ok)"
}

case "$SOURCE" in
  sqlite) verify_sqlite ;;
  all)
    verify_sqlite
    section "Other sources"
    printf "  ${C_DIM}postgres/mysql/mongodb/… require throwaway containers (deploy/test-compose.yml);${C_RESET}\n"
    printf "  ${C_DIM}run their env-gated suites (e.g. LAKESENSE_PG_IT=1 go test ./engine/...).${C_RESET}\n"
    ;;
  *)
    printf "unknown source %q — try: sqlite | all\n" "$SOURCE" >&2
    exit 2
    ;;
esac

summary
