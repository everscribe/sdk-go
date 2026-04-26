.PHONY: help test fmt

help:
	@echo "Targets:"
	@echo "  test  - run tests with the race detector"
	@echo "  fmt   - format code with goimports"

test:
	go test -v -race ./...

fmt:
	@command -v goimports >/dev/null 2>&1 || { \
		echo "goimports not found; install with: go install golang.org/x/tools/cmd/goimports@latest"; \
		exit 1; \
	}
	goimports -w .
