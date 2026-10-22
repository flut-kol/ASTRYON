SHELL := /bin/bash
GO_MODS := $(shell find . -name go.mod -not -path './vendor/*' -not -path './node_modules/*' -exec dirname {} \; 2>/dev/null | sort)

.PHONY: dev-up dev-down topics test lint migrate apps-up apps-down apps-logs extras-up extras-down aggregator-run

dev-up:
	./scripts/dev/up.sh

dev-down:
	./scripts/dev/down.sh

apps-up:
	./scripts/dev/apps-up.sh

apps-down:
	./scripts/dev/apps-down.sh

apps-logs:
	docker logs -f --tail 100 astryon_apps

# Redis, Kafka Connect + Debezium (own CDC PostgreSQL), see infra/docker/EXTRAS.md
extras-up:
	./scripts/dev/extras-up.sh

extras-down:
	./scripts/dev/extras-down.sh

# one-shot ch-aggregator run; `make aggregator-run ARGS=reconcile`
aggregator-run:
	./scripts/dev/aggregator-run.sh $(ARGS)

topics:
	./scripts/dev/create-topics.sh

test:
	@set -e; for d in $(GO_MODS); do \
		echo "==> $$d: go vet + go test"; \
		(cd $$d && go vet ./... && go test ./...); \
	done; \
	[ -n "$(GO_MODS)" ] || echo "no Go modules found"

lint:
	@set -e; test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*') 2>/dev/null)" || { echo "gofmt issues:"; gofmt -l $$(find . -name '*.go' -not -path './vendor/*'); exit 1; }; \
	for d in $(GO_MODS); do (cd $$d && go vet ./...); done; \
	bash -n scripts/dev/*.sh; \
	if command -v golangci-lint >/dev/null; then for d in $(GO_MODS); do (cd $$d && golangci-lint run ./...); done; else echo "golangci-lint not installed, skipped"; fi

migrate:
	@if [ -x scripts/dev/migrate.sh ]; then ./scripts/dev/migrate.sh; else echo "scripts/dev/migrate.sh not found, nothing to do"; fi
