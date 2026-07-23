#!/usr/bin/env bash
#
# Retina stream capture.
#
# Connects to the FIE stream and the event stream, writing FIEs as CSV and
# events as JSONL, both rotated every ROTATE_EVERY records.
#
# Neither stream reconnects. If either one ends, for any reason, the whole
# capture shuts down. A run therefore covers one uninterrupted connection to
# both endpoints, and its end is always explicit rather than papered over.
#
#   ./capture.sh [output_root]
#
# Environment:
#   RETINA_SERVER_URL   base URL                (default: http://localhost:8080)
#   ROTATE_EVERY        records per file        (default: 1000000)
#
# Layout:
#   YYYYMMDD__HHMMSS/
#     fies/fies_000001.csv
#     events/events_000001.jsonl
#     capture.log

set -uo pipefail

SERVER_URL="${RETINA_SERVER_URL:-http://localhost:8080}"
ROTATE_EVERY="${ROTATE_EVERY:-1000000}"

OUTPUT_ROOT="${1:-./data/streams}"
RUN_DIR="${OUTPUT_ROOT}/$(date -u +'%Y%m%d__%H%M%S')"
FIES_DIR="${RUN_DIR}/fies"
EVENTS_DIR="${RUN_DIR}/events"
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

for tool in curl jq; do
	command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done

mkdir -p "$FIES_DIR" "$EVENTS_DIR" || die "cannot create ${RUN_DIR}"
: >"$LOG_FILE"

# ---------------------------------------------------------------------------
# Column order for the FIE CSV. No header is written, so this comment is the
# schema of record:
#
#   agent_id, probing_directive_id, sequence_number, ip_version, protocol,
#   source_address, destination_address,
#   near_probe_ttl, near_reply_address, near_sent_timestamp,
#   near_received_timestamp,
#   far_probe_ttl, far_reply_address, far_sent_timestamp,
#   far_received_timestamp,
#   production_timestamp
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

# ---------------------------------------------------------------------------
# Streams
#
# Both are the same loop: read a line, append it, rotate on count. The write
# is a plain append per line, so a kill can at worst truncate the final line
# rather than lose a buffered batch.
# ---------------------------------------------------------------------------

capture_fies() {
	trap 'exit 0' TERM INT

	local index=1
	local count=0
	local file
	file=$(printf '%s/fies_%06d.csv' "$FIES_DIR" "$index")

	log "fie stream connecting"

	# jq --unbuffered keeps latency bounded; without it jq buffers and rows
	# arrive in clumps.
	while IFS= read -r row; do
		[[ -n "$row" ]] || continue

		printf '%s\n' "$row" >>"$file"
		count=$((count + 1))

		if [[ "$count" -ge "$ROTATE_EVERY" ]]; then
			index=$((index + 1))
			count=0
			file=$(printf '%s/fies_%06d.csv' "$FIES_DIR" "$index")
		fi
	done < <(curl -sN --no-buffer "$FIE_ENDPOINT" 2>/dev/null |
		jq -r --unbuffered "$FIE_FILTER" 2>/dev/null)

	log "fie stream ended"
}

capture_events() {
	trap 'exit 0' TERM INT

	local index=1
	local count=0
	local file
	file=$(printf '%s/events_%06d.jsonl' "$EVENTS_DIR" "$index")

	log "event stream connecting"

	# Lines are appended verbatim, no parsing.
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
# tear down both streams.
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
	# are actually gone.
	local waited=0
	while [[ "$waited" -lt 100 ]]; do
		local alive=0
		for pid in "$FIE_PID" "$EVENT_PID"; do
			[[ -n "$pid" ]] || continue
			if kill -0 "$pid" 2>/dev/null; then alive=1; fi
		done
		[[ "$alive" -eq 1 ]] || break
		sleep 0.1
		waited=$((waited + 1))
	done

	# curl and jq are grandchildren via process substitution and outlive their
	# parent, so clear them out too.
	for pid in "$FIE_PID" "$EVENT_PID"; do
		[[ -n "$pid" ]] || continue
		pkill -TERM -P "$pid" 2>/dev/null || true
	done

	local fie_rows event_rows
	fie_rows=$(cat "$FIES_DIR"/*.csv 2>/dev/null | wc -l)
	event_rows=$(cat "$EVENTS_DIR"/*.jsonl 2>/dev/null | wc -l)

	log "captured ${fie_rows} fies in $(find "$FIES_DIR" -name '*.csv' | wc -l) file(s)"
	log "captured ${event_rows} events in $(find "$EVENTS_DIR" -name '*.jsonl' | wc -l) file(s)"
	log "run directory ${RUN_DIR}"
	exit 0
}

log "run directory ${RUN_DIR}"
log "fie stream    ${FIE_ENDPOINT}"
log "event stream  ${SSE_ENDPOINT}"
log "rotating every ${ROTATE_EVERY} records"

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
