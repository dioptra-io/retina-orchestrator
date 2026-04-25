# Agent Project Documentation

**retina-orchestrator** is part of the Retina system, which includes:
- **Generator**: Creates probing directives
- **Orchestrator**: Distributes directives to agents, collects FIEs (this component)
- **Agent**: Executes network probes

## Overview

- **Language**: Go 1.25.7
- **Module**: `github.com/dioptra-io/retina-orchestrator`
- **Purpose**: Schedules Probing Directives (PDs) to connected Retina agents, collects Forwarding Info Elements (FIEs), and streams them to HTTP clients via SSE/NDJSON
- **License**: MIT

## Project Structure

```
.
├── main.go                    # Entry point, CLI flags, server initialization
├── internal/orchestrator/    # Core package (~4800 LOC)
│   ├── orchestrator.go        # Main orchestrator logic
│   ├── api_server.go         # HTTP API server with /stream endpoint
│   ├── agent_server.go       # TCP server for agent connections
│   ├── scheduler.go          # Responsible probing algorithm
│   ├── randomizer.go        # Random number generator (seeded)
│   ├── metrics.go           # Prometheus metrics definitions
│   ├── errors.go           # Custom error types
│   ├── structures/         # Data structures
│   │   ├── queue.go        # Per-agent PD queue
│   │   └── ringbuffer.go  # Ring buffer for streaming
│   └── *_test.go          # Test files
├── docs/                    # Swagger documentation (auto-generated)
├── Makefile               # Build, lint, test targets
└── go.mod / go.sum         # Dependencies
```

## Key Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `github.com/dioptra-io/retina-commons` | v0.3.0 | Shared types (PD, FIE) |
| `github.com/prometheus/client_golang` | v1.23.2 | Metrics exposition |
| `github.com/swaggo/swag` | v1.16.6 | Swagger/OpenAPI docs |
| `golang.org/x/sync` | v0.20.0 | Synchronization primitives |

## Development Commands

```bash
# Build the binary
make build

# Run linters (golangci-lint)
make lint

# Format code
make fmt

# Run tests with race detection
make test

# Generate Swagger docs
make docs

# Clean build artifacts
make clean
```

## Running the Application

```bash
# Required: Set shared secret for agent authentication
RETINA_SECRET=mysecret ./retina-orchestrator \
  --agent-addr=0.0.0.0:9100 \
  --api-addr=0.0.0.0:8080 \
  --pd-path=pds.jsonl \
  --issuance-rate=1000 \
  --impact-threshold=2.0 \
  --log-level=info
```

## Command-Line Flags

All flags can also be set via environment variables (e.g., `--api-addr` → `RETINA_API_ADDR`).

| Flag | Default | Description |
|------|---------|-------------|
| `--api-addr` | `localhost:8080` | HTTP API server address |
| `--agent-addr` | `localhost:50050` | TCP address for agents |
| `--pd-path` | `""` | Path to JSONL file with Probing Directives |
| `--issuance-rate` | `1.0` | Target PDs per second |
| `--impact-threshold` | `1.0` | Max directives per address (responsible probing) |
| `--max-cycles` | `0 (unlimited)` | Maximum cycles (0 = indefinite) |
| `--seed` | `42` | Randomizer seed |
| `--fie-filter-policy` | `any` | FIE filtering: `any`, `one`, or `both` |
| `--log-level` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `--metrics-addr` | `:9312` | Prometheus metrics endpoint |

## API Endpoints

- `GET /stream` - SSE endpoint streaming FIEs as NDJSON
- `GET /swagger/index.html` - Swagger UI (when running)
- `GET /metrics` - Prometheus metrics

## Architecture Notes

- **Agent Communication**: TCP with newline-delimited JSON (NDJSON)
- **Agent Authentication**: Uses `RETINA_SECRET` environment variable
- **PD Scheduling**: Responsible probing algorithm limits concurrent directives per address
- **Streaming**: Ring buffer used for FIFO event streaming
- **Logging**: JSON format to stdout (Loki/Grafana compatible)
- **Shutdown**: Handles SIGINT/SIGTERM gracefully

## Important Files for Modifications

- `main.go`: CLI parsing, server startup, logging setup
- `internal/orchestrator/orchestrator.go`: Core orchestration loop
- `internal/orchestrator/api_server.go`: HTTP server, streaming endpoint
- `internal/orchestrator/agent_server.go`: TCP agent connections, authentication
- `internal/orchestrator/scheduler.go`: Responsible probing algorithm
- `internal/orchestrator/metrics.go`: All Prometheus metric definitions

## Testing

```bash
# Run all tests
go test -race -cover ./...

# Run specific package
go test ./internal/orchestrator -v
```

## Additional Notes

- This is currently a **research branch** - features may change
- Code has linting (golangci-lint) and formatting checks in CI
- Uses `slog` for structured logging

## Commit Message Conventions

Use the following format for commit messages:

```
<word><optional context in pharanthesis>: <short description of what was changed>
```

Examples:
- `change: remove the line in my file`
- `change: fix bug in scheduler logic`
- `add: new flag for determining pdQueue size`
- `feat(metrics): add agent queue size metric`
- `scheduler: avoid sleep granularity issues with busy-wait for sub-10ms issuance periods`
- `feat(orchestrator): add Prometheus metrics instrumentation`
- `ci: add test and docker build workflow`
- `add Dockerfile for containerized deployment`
- `refactor: clean up orchestrator tooling, docs, and test data`
- `test: improve orchestrator coverage`
- `refactor: unexport apiServer types and methods`
- `fix: update swag command and dependencies; suppress G118 false positive`

Keep descriptions concise and in lowercase.
