.PHONY: build vet test test-race test-integration test-compose up down migrate

TEST_COMPOSE = docker compose --env-file /dev/null -f tests/integration/docker-compose.yml

build:
	go build -o bin/server ./cmd/server
	go build -o bin/worker ./cmd/worker

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./...

test-integration:
	@set -eu; trap '$(TEST_COMPOSE) down --timeout 30' EXIT; \
	$(TEST_COMPOSE) up -d --build --wait; \
	go test -race -tags=integration ./tests/integration -count=1

test-compose:
	docker compose build
	TEST_COMPOSE_E2E=1 go test -race -tags=integration ./tests/integration -run '^TestComposeStack$$' -count=1

up:
	docker compose up -d --build --wait

down:
	docker compose down --timeout 60

migrate:
	docker compose run --rm migrate
