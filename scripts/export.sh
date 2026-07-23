#!/usr/bin/env bash
#
# Export a completed capture run.
#
# Builds indices on the fies table (if not already present), then creates
# a tar.gz archive containing fies.db and all event files. The archive is
# written to RUN_DIR.tar.gz alongside the run directory.
#
#   ./export.sh <run_directory>

set -uo pipefail

die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

[[ $# -eq 1 ]] || die "usage: $0 <run_directory>"

RUN_DIR="$1"
[[ -d "$RUN_DIR" ]] || die "run directory not found: $RUN_DIR"

DB_FILE="${RUN_DIR}/fies.db"
[[ -f "$DB_FILE" ]] || die "fies.db not found: $DB_FILE"

[[ -d "${RUN_DIR}/events" ]] || die "events directory not found: ${RUN_DIR}/events"

command -v sqlite3 >/dev/null 2>&1 || die "sqlite3 not found"
command -v tar >/dev/null 2>&1 || die "tar not found"

echo "[export] building indices..."
sqlite3 "$DB_FILE" <<'SQL' || die "index creation failed"
CREATE INDEX IF NOT EXISTS idx_fies_time
ON fies(production_timestamp);

CREATE INDEX IF NOT EXISTS idx_fies_pdid_seq
ON fies(probing_directive_id, sequence_number);
SQL

echo "[export] checkpointing WAL..."
sqlite3 "$DB_FILE" "PRAGMA wal_checkpoint(TRUNCATE);" 2>/dev/null || true

echo "[export] computing statistics..."
rows=$(sqlite3 "$DB_FILE" "SELECT count(*) FROM fies;" 2>/dev/null || echo "?")
echo "[export] fies table: ${rows} rows"

ARCHIVE="$(dirname "$RUN_DIR")/$(basename "$RUN_DIR").tar.gz"
echo "[export] creating archive: ${ARCHIVE}"

tar -czf "$ARCHIVE" \
	-C "$(dirname "$RUN_DIR")" \
	--exclude='capture.log' \
	"$(basename "$RUN_DIR")/fies.db" \
	"$(basename "$RUN_DIR")/events" ||
	die "tar failed"

ARCHIVE_SIZE=$(du -h "$ARCHIVE" | cut -f1)
echo "[export] done: ${ARCHIVE_SIZE}"
echo "[export] archive: ${ARCHIVE}"
