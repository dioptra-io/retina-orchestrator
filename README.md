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

To build only the binary:
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
  --pd-path=pds.jsonl \
  --issuance-rate=1000 \
  --impact-threshold=2.0 \
  --log-level=info
```

## Flags

| Flag                        | Default          | Description                                           |
| --------------------------- | ---------------- | ----------------------------------------------------- |
| `--api-addr`                | `localhost:8080` | TCP address for the HTTP API server (host:port)       |
| `--agent-addr`              | `localhost:50050`| TCP address for agent connections (host:port)         |
| `--pd-path`                 | `""`             | Path to the JSONL file containing Probing Directives  |
| `--issuance-rate`           | `1.0`            | Target PD issuance rate in PDs per second             |
| `--impact-threshold`        | `1.0`            | Maximum directives allowed to impact a single address |
| `--seed`                    | `42`             | Seed for the random scheduler                         |
| `--api-read-header-timeout` | `5s`             | Timeout for reading HTTP request headers              |
| `--metrics-addr`            | `:9312`          | Address to expose Prometheus metrics on              |
| `--log-level`               | `info`           | Log level (`debug`, `info`, `warn`, `error`)          |

## Environment

| Variable        | Description                            |
| --------------- | -------------------------------------- |
| `RETINA_SECRET` | Shared secret for agent authentication |

## Behavior

- The orchestrator connects to agents over TCP using newline-delimited JSON.
- Agents authenticate using the `RETINA_SECRET` environment variable before receiving directives.
- PDs are scheduled using a responsible probing algorithm that limits the number of concurrent directives impacting any single address.
- FIEs received from agents are streamed to HTTP clients via the `/stream` endpoint as NDJSON.
- Swagger UI is available at `/swagger/index.html` when the server is running.
- Logs are written to stdout in JSON format, compatible with Loki/Grafana pipelines.
- The program handles `SIGINT` and `SIGTERM` for graceful shutdown.

## License

MIT License - see [LICENSE](LICENSE) for details