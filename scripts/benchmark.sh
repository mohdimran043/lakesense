#!/usr/bin/env bash
# benchmark.sh — measure REAL migration throughput for LakeSense's shipping
# connectors. Seeds a source with a realistic dataset, runs a full lsengine
# sync, and reports rows/sec and MB/sec from the engine's own sync_finished
# accounting (and wall clock). Numbers are yours, measured on your hardware —
# NEVER borrow anyone else's.
#
#   scripts/benchmark.sh                    # postgres (if docker) + sqlite, 1M rows
#   ROWS=5000000 scripts/benchmark.sh postgres
#   scripts/benchmark.sh sqlite
#
# Writes docs/BENCHMARKS.md with methodology + the measured table.

set -uo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
# shellcheck source=scripts/lib.sh
source scripts/lib.sh

ROWS="${ROWS:-1000000}"
WHICH="${1:-all}"
WORK="$(mktemp -d)"
ENGINE="$WORK/lsengine"
PGC="lsbench-pg-$$"
RESULTS="$WORK/results.tsv" # source \t rows \t bytes \t engine_s \t wall_s
: > "$RESULTS"

cleanup() {
  docker rm -f "$PGC" >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT

section "Building engine (optimized)"
(cd engine && go build -ldflags "-s -w" -o "$ENGINE" ./cmd/lsengine) || { echo "build failed" >&2; exit 1; }
echo "  ok"

# run_sync <source-label> <src.json> <catalog.json> — times a full sync to NDJSON.
run_sync() {
  local label="$1" src="$2" cat="$3"
  echo "{\"type\":\"ndjson\",\"path\":\"$WORK/out-$label\"}" > "$WORK/dst-$label.json"
  local ev="$WORK/ev-$label.jsonl"
  local start end
  start="$(date +%s.%N)"
  "$ENGINE" sync --config "$src" --destination "$WORK/dst-$label.json" \
    --catalog "$cat" --state "$WORK/state-$label.json" > "$ev" 2>/dev/null
  end="$(date +%s.%N)"
  local wall; wall="$(python3 -c "print(f'{$end-$start:.2f}')")"
  read -r rows bytes esecs < <(python3 - "$ev" <<'PY'
import json, sys
rows = bytes_ = secs = 0
for line in open(sys.argv[1]):
    e = json.loads(line)
    if e["event"] == "sync_finished":
        p = e["payload"]; rows, bytes_, secs = p["rows_written"], p["bytes_written"], p["duration_seconds"]
print(rows, bytes_, secs)
PY
)
  printf "%s\t%s\t%s\t%s\t%s\n" "$label" "$rows" "$bytes" "$esecs" "$wall" >> "$RESULTS"
  local rps mbps
  rps="$(python3 -c "print(f'{$rows/$esecs:,.0f}')" 2>/dev/null)"
  mbps="$(python3 -c "print(f'{$bytes/1e6/$esecs:,.1f}')" 2>/dev/null)"
  printf "  ${C_GREEN}%s${C_RESET}: %s rows in %.2fs  →  ${C_BOLD}%s rows/s · %s MB/s${C_RESET}  (wall %.2fs)\n" \
    "$label" "$(printf "%'d" "$rows")" "$esecs" "$rps" "$mbps" "$wall"
}

bench_sqlite() {
  section "SQLite full load — $ROWS rows"
  python3 - "$WORK/bench.db" "$ROWS" <<'PY'
import sqlite3, sys
db = sqlite3.connect(sys.argv[1]); n = int(sys.argv[2])
db.execute("PRAGMA journal_mode=OFF"); db.execute("PRAGMA synchronous=OFF")
db.execute("CREATE TABLE events(id INTEGER PRIMARY KEY, user_id INTEGER, kind TEXT, amount REAL, note TEXT, created_at TEXT)")
note = "x" * 180
db.executemany("INSERT INTO events VALUES(?,?,?,?,?,?)",
  ((i, i % 100000, "click" if i%2 else "view", (i%1000)+0.5, f"{note}-{i}", "2026-01-01T00:00:00Z") for i in range(1, n+1)))
db.commit(); db.close()
PY
  echo "{\"type\":\"sqlite\",\"path\":\"$WORK/bench.db\",\"chunk_rows\":200000}" > "$WORK/src-sqlite.json"
  "$ENGINE" discover --config "$WORK/src-sqlite.json" \
    | python3 -c 'import json,sys;c=json.load(sys.stdin);c["selected_streams"]=[{"namespace":"main","name":"events","mode":"full_load"}];open("'"$WORK"'/cat-sqlite.json","w").write(json.dumps(c))'
  run_sync sqlite "$WORK/src-sqlite.json" "$WORK/cat-sqlite.json"
}

bench_postgres() {
  command -v docker >/dev/null || { echo "  (docker unavailable — skipping postgres)"; return; }
  section "PostgreSQL full load (keyset-chunked) — $ROWS rows"
  docker rm -f "$PGC" >/dev/null 2>&1
  docker run -d --name "$PGC" -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=bench -p 55450:5432 \
    postgres:16-alpine -c fsync=off -c full_page_writes=off >/dev/null 2>&1
  for _ in $(seq 1 30); do docker exec "$PGC" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
  docker exec "$PGC" psql -U postgres -d bench -q -c "
    CREATE TABLE events(id bigint primary key, user_id bigint, kind text, amount numeric, note text, created_at timestamptz);
    INSERT INTO events SELECT g, g % 100000, CASE WHEN g%2=0 THEN 'view' ELSE 'click' END,
      (g%1000)+0.5, repeat('x',180)||'-'||g, '2026-01-01'::timestamptz
    FROM generate_series(1, $ROWS) g;
    ANALYZE events;" >/dev/null 2>&1
  cat > "$WORK/src-pg.json" <<JSON
{ "type":"postgres","host":"localhost","port":55450,"database":"bench","user":"postgres","password":"bench","sslmode":"disable","max_connections":8,"chunk_strategy":"keyset" }
JSON
  "$ENGINE" discover --config "$WORK/src-pg.json" \
    | python3 -c 'import json,sys;c=json.load(sys.stdin);c["selected_streams"]=[{"namespace":"public","name":"events","mode":"full_load"}];open("'"$WORK"'/cat-pg.json","w").write(json.dumps(c))'
  run_sync postgres "$WORK/src-pg.json" "$WORK/cat-pg.json"
}

case "$WHICH" in
  sqlite) bench_sqlite ;;
  postgres) bench_postgres ;;
  all) bench_postgres; bench_sqlite ;;
  *) echo "usage: benchmark.sh [postgres|sqlite|all]" >&2; exit 2 ;;
esac

# ── Write BENCHMARKS.md ──────────────────────────────────────────────────────
section "Writing docs/BENCHMARKS.md"
cores="$(nproc 2>/dev/null || echo '?')"
mem="$(free -h 2>/dev/null | awk '/Mem:/{print $2}' || echo '?')"
gover="$(cd engine && go version | awk '{print $3}')"
os="$(uname -s -r -m)"
{
  echo "# LakeSense Benchmarks"
  echo
  echo "**Honest, reproducible, and your own.** Every number here is produced by"
  echo "\`scripts/benchmark.sh\` on the hardware named below — never borrowed from"
  echo "another project. Re-run it on your machine and you'll get your machine's"
  echo "numbers."
  echo
  echo "## Method"
  echo
  echo "- **Workload:** full-load replication of a \`$(printf "%'d" "$ROWS")\`-row table"
  echo "  (\`id, user_id, kind, amount, note (~200 B), created_at\`) to the shipping"
  echo "  NDJSON writer, which JSON-encodes and fsyncs every row to disk."
  echo "- **Throughput** is from the engine's own \`sync_finished\` accounting"
  echo "  (rows & bytes written / engine duration); wall clock is also shown."
  echo "- **Postgres** uses keyset chunking; the v0.1 orchestrator reads chunks"
  echo "  **sequentially** (parallel chunk readers are on the roadmap and will lift"
  echo "  Postgres throughput well above this floor). **SQLite** is a single-reader,"
  echo "  server-less file. In v0.1 both are bounded by the NDJSON writer."
  echo "- Measured on: **${cores} cores, ${mem} RAM**, ${os}, ${gover}."
  echo
  echo "## Results"
  echo
  echo "| Source | Rows | Data | Engine time | Rows/s | MB/s |"
  echo "|---|--:|--:|--:|--:|--:|"
  while IFS=$'\t' read -r label rows bytes esecs _wall; do
    python3 - "$label" "$rows" "$bytes" "$esecs" <<'PY'
import sys
label, rows, bytes_, esecs = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
mb = bytes_/1e6
print(f"| {label} | {rows:,} | {mb:,.0f} MB | {esecs:.2f}s | **{rows/esecs:,.0f}** | **{mb/esecs:,.1f}** |")
PY
  done < "$RESULTS"
  echo
  echo "> NDJSON is the v0.1 writer (rock-solid, dependency-free). Parquet + Iceberg"
  echo "> land in v0.2 and will raise these numbers — this is the conservative floor."
  echo
  echo "Reproduce: \`ROWS=$ROWS scripts/benchmark.sh\`"
} > "$ROOT/docs/BENCHMARKS.md"
echo "  wrote docs/BENCHMARKS.md"

summary
