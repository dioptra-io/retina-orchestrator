# retina-orchestrator

The probing orchestrator component of Retina written in Go.

## Overview

retina-orchestrator is responsible for coordinating and scheduling network probing operations across multiple agents. It provides a central control plane for managing probing directives, collecting results, and streaming data to consumers.

## Requirements

- Go 1.24.4
- golangci-lint
- swag (for API documentation)

## Installation

```bash
git clone https://github.com/dioptra-io/retina-orchestrator.git
cd retina-orchestrator
go mod download
```

## Development

### Available Make Targets

| Target        | Description                        |
| ------------- | ---------------------------------- |
| `make help`   | Show available targets             |
| `make build`  | Format, lint, and build the binary |
| `make proper` | Run formatters and linters         |
| `make test`   | Run all tests                      |
| `make docs`   | Generate Swagger documentation     |
| `make clean`  | Remove build artifacts             |

## API Documentation

After running `make docs`, Swagger documentation is available at `/swagger/index.html` when the server is running.

## License

See [LICENSE](LICENSE) for details.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Ensure tests pass (`make test`)
4. Ensure code is properly formatted (`make proper`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request
