#!/usr/bin/env bash
# backup-metadata.sh — pg_dump the control-plane metadata database to a
# timestamped, compressed file, with retention rotation. The metadata DB is the
# only stateful piece (pipelines, config versions, incidents, audit); the lake
# itself lives in open formats you already own.
#
#   scripts/backup-metadata.sh                 # back up now
#   BACKUP_DIR=/data/backups RETENTION=14 scripts/backup-metadata.sh
#
# Restore (documented, tested procedure):
#   gunzip -c lakesense-YYYYMMDD-HHMMSS.sql.gz | psql "$LAKESENSE_DATABASE_URL"
#
# Schedule it via cron or a compose sidecar (see deploy/ notes). Reads the DSN
# from LAKESENSE_DATABASE_URL.

set -euo pipefail

DSN="${LAKESENSE_DATABASE_URL:?LAKESENSE_DATABASE_URL is required}"
BACKUP_DIR="${BACKUP_DIR:-./backups}"
RETENTION="${RETENTION:-7}" # keep this many most-recent backups

mkdir -p "$BACKUP_DIR"
stamp="$(date -u +%Y%m%d-%H%M%S)"
out="$BACKUP_DIR/lakesense-$stamp.sql.gz"

echo "→ dumping metadata DB to $out"
pg_dump "$DSN" --no-owner --no-privileges | gzip > "$out"
size="$(du -h "$out" | cut -f1)"
echo "✓ backup complete ($size)"

# Retention: delete all but the newest $RETENTION backups.
mapfile -t backups < <(ls -1t "$BACKUP_DIR"/lakesense-*.sql.gz 2>/dev/null)
if [ "${#backups[@]}" -gt "$RETENTION" ]; then
  for old in "${backups[@]:$RETENTION}"; do
    echo "  rotating out $old"
    rm -f "$old"
  done
fi
echo "✓ retention: keeping newest $RETENTION (have ${#backups[@]})"
