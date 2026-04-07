.PHONY: build lint fmt tidy test docs clean help

help:
	@echo "Valid targets:"
	@echo "  build  - Format, lint, generate docs, and build retina-orchestrator binary"
	@echo "  lint   - Format code and run linters"
	@echo "  fmt    - Format code"
	@echo "  tidy   - Tidy go modules"
	@echo "  test   - Run tests with race detection"
	@echo "  docs   - Generate Swagger documentation"
	@echo "  clean  - Remove built binaries"

build: docs lint build_orch

lint: fmt
	golangci-lint run

fmt:
	go fmt ./...

tidy:
	go mod tidy

test:
	go test -v -race -cover ./...

docs:
	swag init --parseDependency --parseInternal -g ./internal/orchestrator/api_server.go --output docs
	swag fmt

build_orch:
	go build -o retina-orchestrator .

clean:
	rm -f retina-orchestrator