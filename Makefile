GO ?= go
GOLANGCI_LINT ?= golangci-lint
REDIS_ADDR ?= localhost:6379

.PHONY: help install test test-integration test-all benchmark lint lint-fix fmt tidy audit coverage clean check

## help: Show this help message
help:
	@echo "Usage: make [target]"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'

## install: Download and tidy dependencies
install:
	$(GO) mod download
	$(GO) mod tidy

## test: Run unit tests with race detection
test:
	$(GO) test -race -shuffle=on -coverprofile=coverage.out -coverpkg=. .

## test-integration: Run integration tests (requires Redis at REDIS_ADDR)
test-integration:
	QUOTAPOOL_REDIS_ADDR=$(REDIS_ADDR) $(GO) test -tags=integration -race -shuffle=on -count=1 ./...

## test-all: Run unit and integration tests together
test-all:
	QUOTAPOOL_REDIS_ADDR=$(REDIS_ADDR) $(GO) test -tags=integration -race -shuffle=on -count=1 -coverprofile=coverage.out -coverpkg=. .

## coverage: Generate an HTML coverage report
coverage: test
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "report written to coverage.html"

## benchmark: Run benchmarks
benchmark:
	$(GO) test -run=NONE -bench=. -benchmem ./...

## lint: Run golangci-lint
lint:
	$(GOLANGCI_LINT) run ./...
	$(GOLANGCI_LINT) run --build-tags=integration ./...

## lint-fix: Run golangci-lint with auto-fix
lint-fix:
	$(GOLANGCI_LINT) run --fix ./...

## fmt: Format code and run go vet
fmt:
	$(GO) fmt ./...
	$(GO) vet ./...
	$(GO) vet -tags=integration ./...

## tidy: Tidy go.mod and verify it is unchanged
tidy:
	$(GO) mod tidy
	git diff --exit-code go.mod go.sum

## audit: Scan dependencies for known vulnerabilities
audit:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...

## check: Run everything CI runs
check: fmt lint test

## clean: Remove build and coverage artifacts
clean:
	rm -f coverage.out coverage.html
	$(GO) clean -testcache
