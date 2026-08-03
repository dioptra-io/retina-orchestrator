# retina-orchestrator

`retina-orchestrator` schedules Probing Directives (PDs) to connected agents, collects the resulting Forwarding Info Elements (FIEs), and streams them to HTTP clients.

**Part of the Retina system:**
- **Generator**: Creates probing directives
- **Orchestrator**: Distributes directives to agents, collects FIEs (this component)
- **Agent**: Executes network probes

## Build

```bash
make build
```

To generate Swagger documentation:
```bash
make docs
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
  --agent-addr=0.0.0.0:9100 \
  --api-addr=0.0.0.0:8080 \
  --pd-path-v4=pds_v4.jsonl \
  --pd-path-v6=pds_v6.jsonl \
  --issuance-rate=1000 \
  --impact-threshold=2.0 \
  --active-set-size=10000 \
  --consecutive-misses-threshold=3 \
  --max-evictions=9 \
  --log-level=info
```

## Flags

| Flag                               | Default          | Description                                                                 |
| ---------------------------------- | ---------------- | --------------------------------------------------------------------------- |
| `--api-addr`                       | `localhost:8080` | TCP address for the HTTP API server (host:port)                             |
| `--agent-addr`                     | `localhost:50050`| TCP address for agent connections (host:port)                               |
| `--pd-queue-size`                  | `100`            | Size of the per-agent PD queue buffer                                       |
| `--ring-buffer-size`               | `100`            | Size of the ring buffer                                                     |
| `--pd-path-v4`                     | `""`             | Path to the JSONL file containing IPv4 Probing Directives                   |
| `--pd-path-v6`                     | `""`             | Path to the JSONL file containing IPv6 Probing Directives                   |
| `--issuance-rate`                  | `1.0`            | Target PD issuance rate in PDs per second                                   |
| `--impact-threshold`               | `1.0`            | Maximum allowed probe rate (probes/second) on any single address            |
| `--seed`                           | `42`             | Seed for the random scheduler                                               |
| `--api-read-header-timeout`        | `5s`             | Timeout for reading HTTP request headers                                    |
| `--metrics-addr`                   | `:9312`          | Address to expose Prometheus metrics on                                     |
| `--log-level`                      | `info`           | Log level (`debug`, `info`, `warn`, `error`)                                |
| `--fie-filter-policy`              | `both`           | FIE filtering policy: `any`, `one`, or `both`                               |
| `--active-set-size`                | `10000`          | Number of PDs in the active probing set (split 50/50 between IPv4 and IPv6) |
| `--consecutive-misses-threshold`   | `3`              | Consecutive cycles without a reply before a PD is replaced                 |
| `--max-evictions`                  | `9`              | Times a PD can be replaced before permanent eviction                        |

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
| `RETINA_API_ADDR`                       | `localhost:8080`  | TCP address for the HTTP API server                              |
| `RETINA_AGENT_ADDR`                     | `localhost:50050` | TCP address for agent connections                                |
| `RETINA_PD_QUEUE_SIZE`                  | `100`             | Size of the per-agent PD queue buffer                            |
| `RETINA_RING_BUFFER_SIZE`               | `100`             | Size of the ring buffer used in streaming FIEs                   |
| `RETINA_PD_PATH_V4`                     | `""`              | Path to the JSONL file containing IPv4 Probing Directives        |
| `RETINA_PD_PATH_V6`                     | `""`              | Path to the JSONL file containing IPv6 Probing Directives        |
| `RETINA_ISSUANCE_RATE`                  | `1.0`             | Target PD issuance rate in PDs per second                        |
| `RETINA_IMPACT_THRESHOLD`               | `1.0`             | Maximum allowed probe rate per address (probes/second)           |
| `RETINA_SEED`                           | `42`              | Seed for the random scheduler                                    |
| `RETINA_API_READ_HEADER_TIMEOUT`        | `5s`              | Timeout for reading HTTP request headers                         |
| `RETINA_METRICS_ADDR`                   | `:9312`           | Address to expose Prometheus metrics on                          |
| `RETINA_LOG_LEVEL`                      | `info`            | Log level (`debug`, `info`, `warn`, `error`)                     |
| `RETINA_FIE_FILTER_POLICY`              | `both`            | Filtering policy for FIEs (`any`, `one`, `both`)                 |
| `RETINA_ACTIVE_SET_SIZE`                | `10000`           | Number of PDs in the active probing set                          |
| `RETINA_CONSECUTIVE_MISSES_THRESHOLD`   | `3`               | Consecutive cycles without a reply before a PD is replaced      |
| `RETINA_MAX_EVICTIONS`                  | `9`               | Times a PD can be replaced before permanent eviction             |

## Behavior

- The orchestrator connects to agents over TCP using newline-delimited JSON.
- Agents authenticate using the `RETINA_SECRET` environment variable before receiving directives.
- PDs are loaded from separate IPv4 and IPv6 files at startup. The active set is filled 50/50 from each file; if only one file is provided, all active set slots go to that protocol.
- PDs are scheduled using a responsible probing algorithm that limits the aggregate probe rate on any single address via a Bernoulli experiment.
- When a PD fails the Bernoulli experiment or does not yield replies (both near and far) for `--consecutive-misses-threshold` cycles, it is replaced with a candidate from the unused pool **for the same protocol**, maintaining a stable IPv4/IPv6 distribution in the active set over time.
- A PD that has been replaced `--max-evictions` times without yielding is permanently evicted from the unused pool.
- FIEs received from agents are streamed to HTTP clients via the `/stream` endpoint as NDJSON.
- Swagger UI is available at `/swagger/index.html` when the server is running.
- Logs are written to stdout in JSON format, compatible with Loki/Grafana pipelines.
- The program handles `SIGINT` and `SIGTERM` for graceful shutdown.

## Observability

Metrics are exposed at `--metrics-addr` (default `:9312`) in Prometheus format, covering:

- **Agent connectivity**: agents currently connected, authentication failures, disconnections by agent ID
- **Pipeline throughput**: probing directives sent and FIEs received, queue size per agent, labeled by agent ID
- **PD scheduling**: total directives loaded, active set size, unused pool size labeled by IP version (`4` or `6`), cycle duration, cycles completed, directives replaced by responsible probing or consecutive misses, permanent evictions — labeled by agent ID where applicable
- **Streaming endpoint**: connected HTTP clients, total connections/disconnections by reason, FIEs streamed, stream lag distribution

See `internal/orchestrator/metrics.go` for the full list.

## License

MIT License - see [LICENSE](LICENSE) for details