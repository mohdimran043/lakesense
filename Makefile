GOLANGCI ?= $(shell command -v golangci-lint 2>/dev/null || echo $(HOME)/go/bin/golangci-lint)

GO_MODULES := engine backend
# website target joins when scaffolded.

.PHONY: check lint vet test build tidy frontend

check: lint vet test frontend ## lint + vet + tests + frontend build — must pass before any phase completes

# Frontend: lint + strict typecheck + production build. Skipped gracefully if
# deps aren't installed (run `cd frontend && npm install` first).
frontend:
	@if [ -d frontend/node_modules ]; then \
		echo "== frontend build"; \
		(cd frontend && npm run lint && npm run build) || exit 1; \
	else \
		echo "== frontend skipped (run: cd frontend && npm install)"; \
	fi

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
