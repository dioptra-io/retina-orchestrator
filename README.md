# retina-orchestrator

`retina-orchestrator` schedules Probing Directives (PDs) to connected agents, collects the resulting Forwarding Info Elements (FIEs), and forwards them to [retina-api](https://github.com/dioptra-io/retina-api).

**Retina Architecture:**
- **PD source**: Produces probing directives (PDs) as JSONL files: the
  [Generator](https://github.com/dioptra-io/retina-generator) or the
  [retina-tools](https://github.com/dioptra-io/retina-tools) pipeline for
  Iris-derived directives
- **[Orchestrator](https://github.com/dioptra-io/retina-orchestrator)** (this component): Loads PD files,
  distributes directives to agents, collects forwarding info elements (FIEs), forwards them to the API
- **[Agents](https://github.com/dioptra-io/retina-agent)**: Execute network probes and
  return measurements
- **[API](https://github.com/dioptra-io/retina-api)**: Receives FIEs from the orchestrator and streams them to clients

Orchestrator-agent communication uses Protobuf messages over length-prefixed
TCP streams (see the `framing` package in
[retina-commons](https://github.com/dioptra-io/retina-commons)). PD files are
JSONL, one protojson-encoded directive per line.

```
┌───────────┐
│ PD source │
└─────┬─────┘
      │ PD files (JSONL), diffs on SIGHUP
      ▼
┌─────────────┐         ProbingDirective         ┌───────┐
│Orchestrator │────────────────────────────────▶ │ Agent │
└──┬───▲──────┘                                  └───┬───┘
   │   │            ForwardingInfoElement            │
   │   └─────────────────────────────────────────────┘
   │ FIEs
   ▼
┌─────┐
│ API │
└─────┘
```

## Build

```bash
make build
```

To clean:
```bash
make clean
```

## Test

```bash
make test
```

## Usage

```bash
./retina-orchestrator [flags]
```

### Example

```bash
RETINA_SECRET=mysecret ./retina-orchestrator \
  --agent-addr=localhost:50050 \
  --api-addr=retina-api.example.org:8123 \
  --pd-path-v4=pds_v4.jsonl \
  --pd-path-v6=pds_v6.jsonl \
  --pd-diff-path=pds_diff.jsonl \
  --issuance-rate=1.0 \
  --impact-threshold=1.0 \
  --active-set-size=10000 \
  --consecutive-misses-threshold=3 \
  --max-evictions=9 \
  --log-level=info
```

## Flags

| Flag                               | Default           | Description                                                                 |
| ---------------------------------- | ----------------- | --------------------------------------------------------------------------- |
| `--agent-addr`                     | `localhost:50050` | TCP address for agent connections (host:port)                               |
| `--api-addr`                       | *required*        | retina-api ingest listener address (host:port)                              |
| `--api-buffer-size`                | `10000`           | Capacity of the outbound FIE buffer toward retina-api                       |
| `--api-reconnect-delay`            | `5s`              | Delay before retrying a dropped retina-api connection                       |
| `--api-send-timeout`               | `5s`              | Deadline for sending one FIE to retina-api                                  |
| `--pd-queue-size`                  | `100`             | Size of the per-agent PD queue buffer                                       |
| `--pd-path-v4`                     | `""`              | Path to the JSONL file containing IPv4 Probing Directives                   |
| `--pd-path-v6`                     | `""`              | Path to the JSONL file containing IPv6 Probing Directives                   |
| `--pd-diff-path`                   | `""`              | Path to the PD diff file (insert/remove ops), applied on `SIGHUP`           |
| `--issuance-rate`                  | `1.0`             | Target PD issuance rate in PDs per second                                   |
| `--impact-threshold`               | `1.0`             | Maximum allowed probe rate (probes/second) on any single address            |
| `--seed`                           | `42`              | Seed for the random scheduler                                               |
| `--metrics-addr`                   | `:9312`           | Address to expose Prometheus metrics on                                     |
| `--log-level`                      | `info`            | Log level (`debug`, `info`, `warn`, `error`)                                |
| `--fie-filter-policy`              | `both`            | FIE filtering policy: `any`, `one`, or `both`                               |
| `--active-set-size`                | `10000`           | Number of PDs in the active probing set (split 50/50 between IPv4 and IPv6) |
| `--consecutive-misses-threshold`   | `3`               | Consecutive cycles without a reply before a PD is replaced                  |
| `--max-evictions`                  | `9`               | Times a PD can be replaced before permanent eviction                        |

At least one of `--pd-path-v4` or `--pd-path-v6` must be provided. If only one is provided, all active set slots go to that protocol.

## Environment Variables

All flags can be configured via environment variables. These act as defaults and are overridden by CLI flags.

Precedence:

```
CLI flags > environment variables > hardcoded defaults
```

| Variable                                | Default           | Description                                                      |
| --------------------------------------- | ----------------- | ---------------------------------------------------------------- |
| `RETINA_SECRET`                         | *                 | Shared secret for agent authentication, required                 |
| `RETINA_AGENT_ADDR`                     | `localhost:50050` | TCP address for agent connections                                |
| `RETINA_API_ADDR`                       | *required*        | retina-api ingest listener address                               |
| `RETINA_API_BUFFER_SIZE`                | `10000`           | Capacity of the outbound FIE buffer toward retina-api            |
| `RETINA_API_RECONNECT_DELAY`            | `5s`              | Delay before retrying a dropped retina-api connection            |
| `RETINA_API_SEND_TIMEOUT`               | `5s`              | Deadline for sending one FIE to retina-api                       |
| `RETINA_PD_QUEUE_SIZE`                  | `100`             | Size of the per-agent PD queue buffer                            |
| `RETINA_PD_PATH_V4`                     | `""`              | Path to the JSONL file containing IPv4 Probing Directives        |
| `RETINA_PD_PATH_V6`                     | `""`              | Path to the JSONL file containing IPv6 Probing Directives        |
| `RETINA_PD_DIFF_PATH`                   | `""`              | Path to the PD diff file, applied on `SIGHUP`                    |
| `RETINA_ISSUANCE_RATE`                  | `1.0`             | Target PD issuance rate in PDs per second                        |
| `RETINA_IMPACT_THRESHOLD`               | `1.0`             | Maximum allowed probe rate per address (probes/second)           |
| `RETINA_SEED`                           | `42`              | Seed for the random scheduler                                    |
| `RETINA_METRICS_ADDR`                   | `:9312`           | Address to expose Prometheus metrics on                          |
| `RETINA_LOG_LEVEL`                      | `info`            | Log level (`debug`, `info`, `warn`, `error`)                     |
| `RETINA_FIE_FILTER_POLICY`              | `both`            | Filtering policy for FIEs (`any`, `one`, `both`)                 |
| `RETINA_ACTIVE_SET_SIZE`                | `10000`           | Number of PDs in the active probing set                          |
| `RETINA_CONSECUTIVE_MISSES_THRESHOLD`   | `3`               | Consecutive cycles without a reply before a PD is replaced       |
| `RETINA_MAX_EVICTIONS`                  | `9`               | Times a PD can be replaced before permanent eviction             |

## Behavior

- The orchestrator talks to agents over TCP using length-prefixed Protobuf messages (see the `framing` package in retina-commons).
- Agents authenticate using the `RETINA_SECRET` environment variable before receiving directives.
- PDs are loaded from separate IPv4 and IPv6 files at startup. The active set is filled 50/50 from each file; if only one file is provided, all active set slots go to that protocol.
- PDs are scheduled using a responsible probing algorithm that limits the aggregate probe rate on any single address via a Bernoulli experiment.
- When a PD fails the Bernoulli experiment or does not yield replies (both near and far) for `--consecutive-misses-threshold` cycles, it is replaced with a candidate from the unused pool **for the same protocol**, maintaining a stable IPv4/IPv6 distribution in the active set over time.
- A PD that has been replaced `--max-evictions` times without yielding is permanently evicted from the unused pool.
- FIEs received from agents are forwarded to retina-api. If the connection drops, the orchestrator reconnects after `--api-reconnect-delay`.
- Logs are written to stdout in JSON format, compatible with Loki/Grafana pipelines.
- `SIGINT` and `SIGTERM` trigger a graceful shutdown. `SIGHUP` reloads the PD diff (see below).

## Reloading PDs without a restart

PDs can be added or removed while the orchestrator runs by writing a diff file and sending `SIGHUP`. This requires `--pd-diff-path` (or `RETINA_PD_DIFF_PATH`).

The diff is JSONL, one operation per line:

```jsonl
{"op":"insert","probing_directive_id":57762981243948,"ip_version":4,"protocol":1,"agent_id":"retina-southamerica-east1-dev","destination_address":"124.206.254.4","near_ttl":14,"next_header":{"icmp_next_header":{"first_half_word":31916,"second_half_word":0}}}
{"op":"remove","probing_directive_id":18446741891296895000}
```

Apply it with:

```bash
docker compose kill -s HUP <service>
```

Or, outside Docker, `kill -HUP <pid>`.

Notes:

- Write the diff atomically: write a temporary file in the same directory, then `mv` it over `--pd-diff-path`. A diff read while still being written is truncated.
- Malformed lines are skipped and logged; valid lines are still applied.
- Inserted PDs join the unused pool, and removed PDs stop being probed. The active set keeps its size and 50/50 IPv4/IPv6 split where supply allows.

## Observability

Metrics are exposed at `--metrics-addr` (default `:9312`) in Prometheus format, covering:

- **Agent connectivity**: agents currently connected, authentication failures, disconnections by agent ID
- **Pipeline throughput**: probing directives sent and FIEs received, queue size per agent, labeled by agent ID
- **PD scheduling**: total directives loaded, active set size, unused pool size labeled by IP version (`4` or `6`), cycle duration, cycles completed, directives replaced by responsible probing or consecutive misses, permanent evictions — labeled by agent ID where applicable
- **Streaming to retina-api**: FIEs sent and dropped, and whether the connection is up

See `internal/orchestrator/metrics.go` for the full list.

## License

MIT License - see [LICENSE](LICENSE) for details