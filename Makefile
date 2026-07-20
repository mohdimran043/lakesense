GOLANGCI ?= $(shell command -v golangci-lint 2>/dev/null || echo $(HOME)/go/bin/golangci-lint)

GO_MODULES := engine backend
# website target joins when scaffolded.

.PHONY: check lint vet test build tidy frontend website verify verify-all bench release-check
SRC ?= all

LAKESENSE_URL ?= http://localhost:8080

# verify: self-contained migration-correctness proof, plus the whole-product
# feature proof when a stack is reachable.
verify:
	bash scripts/verify-migration.sh sqlite
	@if curl -fsS "$(LAKESENSE_URL)/healthz" >/dev/null 2>&1; then \
		LAKESENSE_URL=$(LAKESENSE_URL) bash scripts/verify-features.sh; \
	else \
		echo "== verify-features skipped (no stack at $(LAKESENSE_URL); run: docker compose -f deploy/docker-compose.yml up -d)"; \
	fi

verify-all: check verify
	bash scripts/verify-migration.sh all

# bench: measure real full-load throughput (postgres + sqlite) and regenerate
# docs/BENCHMARKS.md. Numbers are yours, measured on your hardware.
bench:
	bash scripts/benchmark.sh $(SRC)

# release-check: clean-machine simulation — builds the whole product from the
# committed tree only and proves the quickstart. Run before every tag (slow;
# builds Docker images from scratch).
release-check:
	bash scripts/verify-release.sh

check: lint vet test frontend website ## lint + vet + tests + frontend & website builds — must pass before any phase completes

# Frontend: lint + strict typecheck + production build. Skipped gracefully if
# deps aren't installed (run `cd frontend && npm install` first).
frontend:
	@if [ -d frontend/node_modules ]; then \
		echo "== frontend build"; \
		(cd frontend && npm run lint && npm run build) || exit 1; \
	else \
		echo "== frontend skipped (run: cd frontend && npm install)"; \
	fi

# Website: marketing site build (strict typecheck + vite build). Skipped when
# deps aren't installed.
website:
	@if [ -d website/node_modules ]; then \
		echo "== website build"; \
		(cd website && npm run build) || exit 1; \
	else \
		echo "== website skipped (run: cd website && npm install)"; \
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
