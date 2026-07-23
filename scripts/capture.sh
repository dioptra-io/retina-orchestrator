#!/usr/bin/env bash
#
# Retina stream capture.
#
# Connects to the FIE stream and the event stream. FIEs are written into a
# SQLite database (single file, no indices — add those in post-processing).
# Events are appended verbatim into JSONL, rotated every ROTATE_EVERY records.
#
# Neither stream reconnects. If either one ends, for any reason, the whole
# capture shuts down. A run therefore covers one uninterrupted connection to
# both endpoints, and its end is always explicit rather than papered over.
#
#   ./capture.sh [output_root]
#
# Environment:
#   RETINA_SERVER_URL   base URL              (default: http://localhost:8080)
#   BATCH_SIZE          rows per FIE flush    (default: 5000)
#   FLUSH_INTERVAL      max seconds unflushed (default: 30)
#   ROTATE_EVERY        event records/file    (default: 1000000)
#
# Layout:
#   YYYYMMDD__HHMMSS/
#     fies.db
#     events/events_000001.jsonl
#     capture.log

set -uo pipefail

SERVER_URL="${RETINA_SERVER_URL:-http://localhost:8080}"
BATCH_SIZE="${BATCH_SIZE:-5000}"
FLUSH_INTERVAL="${FLUSH_INTERVAL:-30}"
ROTATE_EVERY="${ROTATE_EVERY:-1000000}"

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
# Flat, one row per FIE, near and far denormalised. No indices here on
# purpose — building them on a 100M+ row table costs real time, so that
# happens once in post-processing rather than on every flush. WAL lets a
# reader attach while the capture is running; NORMAL is durable across a
# process crash, which is the failure this run is actually exposed to.
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
log "fie batch ${BATCH_SIZE} rows or ${FLUSH_INTERVAL}s"
log "event rotate every ${ROTATE_EVERY} records"

# ---------------------------------------------------------------------------
# FIE stream
#
# jq flattens each FIE to a CSV row. Rows accumulate in a bash string buffer
# and are piped straight to sqlite3's stdin via .import /dev/stdin, so nothing
# is ever written to disk except the database itself.
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
	local buffer=""
	local batch=0
	local last_flush
	last_flush=$(date -u +%s)

	# Claim the buffered rows by reassigning the variable before sqlite3 is
	# invoked. Signal handlers can re-enter this while sqlite3 is running, and
	# the reassignment is a single bash operation with no external process in
	# between, so a re-entrant call sees batch=0 and returns immediately
	# instead of reimporting the same rows. No file ever touches disk: the CSV
	# lives in the buffer variable and is piped straight to sqlite3's stdin.
	flush_fies() {
		[[ "$batch" -gt 0 ]] || return 0
		local snapshot="$buffer"
		buffer=""
		batch=0
		last_flush=$(date -u +%s)

		printf '%s' "$snapshot" |
			sqlite3 -csv -cmd "PRAGMA busy_timeout = 30000;" \
				-cmd ".import /dev/stdin fies" "$DB_FILE" "" >/dev/null
	}

	# Flush whatever is staged when this stream is torn down.
	trap 'flush_fies; exit 0' TERM INT
	trap 'flush_fies' EXIT

	log "fie stream connecting"

	# One connection, no retry. jq --unbuffered keeps latency bounded; without
	# it jq buffers and rows arrive in clumps.
	while IFS= read -r row; do
		[[ -n "$row" ]] || continue

		printf -v buffer '%s%s\n' "$buffer" "$row"
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
# Lines are appended verbatim, rotated on record count.
# ---------------------------------------------------------------------------

capture_events() {
	trap 'exit 0' TERM INT

	local index=1
	local count=0
	local file
	file=$(printf '%s/events_%06d.jsonl' "$EVENTS_DIR" "$index")

	log "event stream connecting"

	while IFS= read -r line; do
		[[ -n "$line" ]] || continue

		printf '%s\n' "$line" >>"$file"
		count=$((count + 1))

		if [[ "$count" -ge "$ROTATE_EVERY" ]]; then
			index=$((index + 1))
			count=0
			file=$(printf '%s/events_%06d.jsonl' "$EVENTS_DIR" "$index")
		fi
	done < <(curl -sN --no-buffer "$SSE_ENDPOINT" 2>/dev/null)

	log "event stream ended"
}

# ---------------------------------------------------------------------------
# Shutdown
#
# Reached either from a signal or from one of the streams ending. Both paths
# tear down both streams. No indices are built here — add them in
# post-processing so stopping stays fast regardless of table size.
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

	local rows event_rows
	rows=$(sqlite3 "$DB_FILE" "SELECT count(*) FROM fies;" 2>/dev/null || echo "?")
	event_rows=$(cat "$EVENTS_DIR"/*.jsonl 2>/dev/null | wc -l)

	log "captured ${rows} fies into ${DB_FILE}"
	log "captured ${event_rows} events in $(find "$EVENTS_DIR" -name '*.jsonl' | wc -l) file(s)"
	log "run directory ${RUN_DIR}"
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
