GOLANGCI ?= $(shell command -v golangci-lint 2>/dev/null || echo $(HOME)/go/bin/golangci-lint)

GO_MODULES := engine
# backend joins GO_MODULES in Phase 3; frontend/website targets join when scaffolded.

.PHONY: check lint vet test build tidy

check: lint vet test ## lint + vet + tests for everything — must pass before any phase completes

lint:
	@for m in $(GO_MODULES); do \
		echo "== lint $$m"; \
		(cd $$m && $(GOLANGCI) run ./...) || exit 1; \
	done

vet:
	@for m in $(GO_MODULES); do \
		echo "== vet $$m"; \
		(cd $$m && go vet ./...) || exit 1; \
	done

test:
	@for m in $(GO_MODULES); do \
		echo "== test $$m"; \
		(cd $$m && go test -race ./...) || exit 1; \
	done

build:
	cd engine && go build -o ../bin/lsengine ./cmd/lsengine

tidy:
	@for m in $(GO_MODULES); do (cd $$m && go mod tidy) || exit 1; done
