.PHONY: build proper help docs test
help:
	@echo valid targets: build proper clean docs test
build: proper orch 
proper:
	find . -name '*.go' ! -name '*_test.go' | sort | xargs wc -l
	gofmt -s -w $(shell go list -f '{{.Dir}}' ./...)
	@if command -v goimports >/dev/null 2>&1; then \
		echo goimports -w $(shell go list -f '{{.Dir}}' ./...); \
		goimports -w $(shell go list -f '{{.Dir}}' ./...); \
	fi
	golangci-lint run --tests=false
docs:
	swag init --parseDependency
test:
	go test ./...
orch: 
	go build -o retina-orchestrator .
clean:
	rm -f retina-orchestrator
