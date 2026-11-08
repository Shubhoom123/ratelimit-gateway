.PHONY: build test vet fmt up down loadtest-local

COMPOSE := docker compose -f deploy/compose/docker-compose.yml

build:
	mkdir -p bin
	go build -o bin/ ./cmd/gateway ./cmd/backend ./cmd/loadgen ./cmd/algobench

vet:
	go vet ./...

# Fails if any file needs gofmt.
fmt:
	@test -z "$$(gofmt -l cmd internal)" || (gofmt -l cmd internal; echo "gofmt needed"; exit 1)

# Needs Redis on 127.0.0.1:6379 (try: make up). Fails if any test is skipped,
# because Redis-backed tests skip silently when Redis is unreachable.
test:
	go test -race -count=1 -v ./... 2>&1 | tee /tmp/ratelimit-test.out | grep -E '^(--- FAIL|FAIL|ok|panic)' || true
	@! grep -q -- '--- SKIP' /tmp/ratelimit-test.out || (echo "ERROR: tests were skipped (is Redis running on :6379?)"; exit 1)
	@! grep -qE '^(--- FAIL|FAIL|panic)' /tmp/ratelimit-test.out || (echo "ERROR: test failures"; exit 1)

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

loadtest-local: build
	python3 scripts/loadtest_local.py
