.PHONY: build proper help docs test clean

help:
	@echo valid targets: build proper clean docs test
build: docs proper orch
proper:
	find . -name '*.go' ! -name '*_test.go' | sort | xargs wc -l
	gofmt -s -w $(shell go list -f '{{.Dir}}' ./...)
	@if command -v goimports >/dev/null 2>&1; then \
		echo goimports -w $(shell go list -f '{{.Dir}}' ./...); \
		goimports -w $(shell go list -f '{{.Dir}}' ./...); \
	fi
	golangci-lint run --tests=false
test:
	go test ./...
orch: 
	go build -o retina-orchestrator .
clean:
	rm -f retina-orchestrator
docs:
	swag init --parseDependency --parseInternal -g ./internal/retina/servers/http_server.go --output docs
	swag fmt
