.PHONY: build proper help
help:
	@echo valid targets: build proper clean
build: proper orch prober
proper:
	find . -name '*.go' ! -name '*_test.go' | sort | xargs wc -l
	gofmt -s -w $(shell go list -f '{{.Dir}}' ./...)
	@if command -v goimports >/dev/null 2>&1; then \
		echo goimports -w $(shell go list -f '{{.Dir}}' ./...); \
		goimports -w $(shell go list -f '{{.Dir}}' ./...); \
	fi
	golangci-lint run --tests=false
orch: cmd/orch/orch.go common/common.go
	go build -o orch cmd/orch/orch.go
prober: cmd/prober/prober.go common/common.go
	go build -o prober cmd/prober/prober.go
clean:
	rm -f orch prober
