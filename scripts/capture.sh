#!/usr/bin/env bash
#
# Retina stream capture.
#
# Connects to the FIE stream and the event stream, writing FIEs into a SQLite
# database and events into hourly rolling JSONL segments.
#
# Neither stream reconnects. If either one ends, for any reason, the whole
# capture shuts down. A run therefore covers one uninterrupted connection to
# both endpoints, and its end is always explicit rather than papered over.
#
#   ./capture.sh [output_root]
#
# Environment:
#   RETINA_SERVER_URL   base URL              (default: http://localhost:8080)
#   BATCH_SIZE          rows per FIE flush    (default: 1000)
#   FLUSH_INTERVAL      max seconds unflushed (default: 30)
#
# Layout:
#   YYYYMMDD__HHMMSS/
#     events/YYYY-MM-DD__HH.jsonl
#     fies.db
#     capture.log

set -uo pipefail

SERVER_URL="${RETINA_SERVER_URL:-http://localhost:8080}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
FLUSH_INTERVAL="${FLUSH_INTERVAL:-30}"

OUTPUT_ROOT="${1:-./data/streams}"
RUN_DIR="${OUTPUT_ROOT}/$(date -u +'%Y%m%d__%H%M%S')"
EVENTS_DIR="${RUN_DIR}/events"
DB_FILE="${RUN_DIR}/fies.db"
LOG_FILE="${RUN_DIR}/capture.log"

FIE_ENDPOINT="${SERVER_URL}/api/v1/stream"
SSE_ENDPOINT="${SERVER_URL}/api/v1/sse"

log() {
	printf '%s [capture] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" |
		tee -a "$LOG_FILE" >&2
}

die() {
	printf '%s [capture] error: %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
	exit 1
}

for tool in curl jq sqlite3; do
	command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done

mkdir -p "$EVENTS_DIR" || die "cannot create ${EVENTS_DIR}"
: >"$LOG_FILE"

# ---------------------------------------------------------------------------
# Schema
#
# Flat, one row per FIE, near and far denormalised. WAL so a reader can attach
# while the capture is running; NORMAL is durable across a process crash, which
# is the failure this run is actually exposed to.
# ---------------------------------------------------------------------------

sqlite3 "$DB_FILE" >/dev/null <<'SQL' || die "cannot initialise ${DB_FILE}"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -200000;
PRAGMA mmap_size = 1073741824;

CREATE TABLE IF NOT EXISTS fies (
    agent_id                TEXT        NOT NULL,
    probing_directive_id    INTEGER     NOT NULL,
    sequence_number         INTEGER     NOT NULL,
    ip_version              INTEGER     NOT NULL,
    protocol                INTEGER     NOT NULL,
    source_address          TEXT        NOT NULL,
    destination_address     TEXT        NOT NULL,
    near_probe_ttl          INTEGER,
    near_reply_address      TEXT,
    near_sent_timestamp     TEXT,
    near_received_timestamp TEXT,
    far_probe_ttl           INTEGER,
    far_reply_address       TEXT,
    far_sent_timestamp      TEXT,
    far_received_timestamp  TEXT,
    production_timestamp    TEXT        NOT NULL
);
SQL

log "run directory ${RUN_DIR}"
log "fie stream    ${FIE_ENDPOINT}"
log "event stream  ${SSE_ENDPOINT}"
log "batch ${BATCH_SIZE} rows or ${FLUSH_INTERVAL}s"

# ---------------------------------------------------------------------------
# FIE stream
#
# jq flattens each FIE to a CSV row. Rows accumulate in a staging file and are
# loaded with .import, which handles quoting correctly rather than building
# INSERT statements by hand.
# ---------------------------------------------------------------------------

readonly FIE_FILTER='
[
    (.agent.agent_id // ""),
    (.probing_directive_id // 0),
    (.sequence_number // 0),

    (.ip_version // 0),
    (.protocol // 0),

    (.source_address // ""),
    (.destination_address // ""),

    (.near_info.probe_ttl // 0),
    (.near_info.reply_address // ""),
    (.near_info.sent_timestamp // ""),
    (.near_info.received_timestamp // ""),

    (.far_info.probe_ttl // 0),
    (.far_info.reply_address // ""),
    (.far_info.sent_timestamp // ""),
    (.far_info.received_timestamp // ""),

    (.production_timestamp // "")
] | @csv'

capture_fies() {
	local staging="${RUN_DIR}/.fies.staging.csv"
	local batch=0
	local last_flush
	last_flush=$(date -u +%s)

	: >"$staging"

	# Claim the staged rows by renaming them aside before importing. Signal
	# handlers can re-enter this while an import is in flight, and rename is
	# atomic, so only one caller can ever own a given batch. Truncating after
	# the import instead would let a second caller import the same rows twice.
	flush_fies() {
		local pending="${staging}.pending"

		[[ "$batch" -gt 0 ]] || return 0
		mv "$staging" "$pending" 2>/dev/null || return 0
		: >"$staging"
		batch=0
		last_flush=$(date -u +%s)

		sqlite3 "$DB_FILE" >/dev/null <<SQL
PRAGMA busy_timeout = 30000;
.mode csv
.import '${pending}' fies
SQL
		rm -f "$pending"
	}

	# Flush whatever is staged when this stream is torn down.
	trap 'flush_fies; exit 0' TERM INT
	trap 'flush_fies' EXIT

	log "fie stream connecting"

	# One connection, no retry. jq --unbuffered keeps latency bounded; without
	# it jq buffers and rows arrive in clumps.
	while IFS= read -r row; do
		[[ -n "$row" ]] || continue

		printf '%s\n' "$row" >>"$staging"
		batch=$((batch + 1))

		local now
		now=$(date -u +%s)
		if [[ "$batch" -ge "$BATCH_SIZE" ]] ||
			[[ $((now - last_flush)) -ge "$FLUSH_INTERVAL" ]]; then
			flush_fies
		fi
	done < <(curl -sN --no-buffer "$FIE_ENDPOINT" 2>/dev/null |
		jq -r --unbuffered "$FIE_FILTER" 2>/dev/null)

	flush_fies
	log "fie stream ended"
}

# ---------------------------------------------------------------------------
# Event stream
#
# Lines are appended verbatim. The current hour is recomputed per line, so
# rotation needs no external helper and a quiet hour simply produces no file.
# ---------------------------------------------------------------------------

capture_events() {
	trap 'exit 0' TERM INT

	log "event stream connecting"

	# One connection, no retry. Lines are appended verbatim; the hour is
	# recomputed per line so rotation needs no external helper.
	while IFS= read -r line; do
		[[ -n "$line" ]] || continue
		printf '%s\n' "$line" \
			>>"${EVENTS_DIR}/$(date -u +'%Y-%m-%d__%H').jsonl"
	done < <(curl -sN --no-buffer "$SSE_ENDPOINT" 2>/dev/null)

	log "event stream ended"
}

# ---------------------------------------------------------------------------
# Shutdown
#
# Reached either from a signal or from one of the streams ending. Both paths
# tear down both streams. Indexes are deliberately not built here; add them in
# post-processing so stopping stays cheap.
# ---------------------------------------------------------------------------

FIE_PID=""
EVENT_PID=""
SHUTTING_DOWN=0

shutdown() {
	# Signals can arrive while this is already running, and the supervisor loop
	# also calls it directly.
	[[ "$SHUTTING_DOWN" -eq 0 ]] || return 0
	SHUTTING_DOWN=1
	trap - INT TERM HUP EXIT

	log "stopping: ${1:-signal}"

	for pid in "$FIE_PID" "$EVENT_PID"; do
		[[ -n "$pid" ]] || continue
		kill -TERM "$pid" 2>/dev/null || true
	done

	# kill returns as soon as the signal is queued, so poll until the children
	# are actually gone and their final flush has released the write lock.
	local waited=0
	while [[ "$waited" -lt 200 ]]; do
		local alive=0
		for pid in "$FIE_PID" "$EVENT_PID"; do
			[[ -n "$pid" ]] || continue
			if kill -0 "$pid" 2>/dev/null; then alive=1; fi
		done
		[[ "$alive" -eq 1 ]] || break
		sleep 0.1
		waited=$((waited + 1))
	done
	[[ "$waited" -lt 200 ]] || log "streams did not exit within 20s, continuing"

	# curl and jq are grandchildren via process substitution and outlive their
	# parent, so clear them out too.
	for pid in "$FIE_PID" "$EVENT_PID"; do
		[[ -n "$pid" ]] || continue
		pkill -TERM -P "$pid" 2>/dev/null || true
	done

	rm -f "${RUN_DIR}/.fies.staging.csv" "${RUN_DIR}/.fies.staging.csv.pending"

	local rows
	rows=$(sqlite3 "$DB_FILE" "SELECT count(*) FROM fies;" 2>/dev/null || echo "?")
	log "captured ${rows} fies into ${DB_FILE}"
	log "events in ${EVENTS_DIR}"
	exit 0
}

# Note on stopping a backgrounded run: bash sets SIGINT to ignored for commands
# launched with &, and a signal ignored on entry to a non-interactive shell
# cannot be trapped, so the INT trap below is a no-op in that case. Ctrl-C on a
# foreground run works; a backgrounded run must be stopped with TERM or HUP.
trap shutdown INT TERM HUP

capture_fies &
FIE_PID=$!

capture_events &
EVENT_PID=$!

log "capturing, interrupt to stop"

# Either stream ending ends the run. Polling rather than a bare `wait`, because
# bash defers trap handling until a wait returns, which would make an interrupt
# aimed at this process alone hang instead of shutting down.
while true; do
	if ! kill -0 "$FIE_PID" 2>/dev/null; then
		shutdown "fie stream ended"
	fi
	if ! kill -0 "$EVENT_PID" 2>/dev/null; then
		shutdown "event stream ended"
	fi
	sleep 1
done
