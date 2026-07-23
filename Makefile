.PHONY: build lint fmt tidy test cover docs clean help setup-hooks

help:
	@echo "Valid targets:"
	@echo "  build       - Format, lint, generate docs, and build retina-orchestrator binary"
	@echo "  lint        - Format code and run linters"
	@echo "  fmt         - Format code"
	@echo "  tidy        - Tidy go modules"
	@echo "  test        - Run tests with race detection and generate coverage profile"
	@echo "  cover       - View test coverage in browser"
	@echo "  docs        - Generate Swagger documentation"
	@echo "  clean       - Remove built binaries and coverage files"
	@echo "  setup-hooks - Configure local Git hooks for commit validation"

build: docs lint
	go build -o retina-orchestrator .

lint: fmt
	echo golangci-lint run

fmt:
	go fmt ./...

tidy:
	go mod tidy

test:
	go test -v -race -coverprofile=coverage.out ./...

cover:
	go tool cover -html=coverage.out

docs:
	swag init --parseDependency --parseInternal -g main.go --output docs
	swag fmt

clean:
	rm -f retina-orchestrator coverage.out

setup-hooks:
	@mkdir -p .githooks
	@git config core.hooksPath .githooks
	@echo "✅ Local Git hooks configured successfully!"
