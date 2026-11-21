.PHONY: build run fmt tidy coverage db-up migrate up down logs k8s-apply

build:
	go build ./...

run:
	go run ./cmd/api

fmt:
	gofmt -w cmd/api/main.go internal

tidy:
	go mod tidy

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -n 1
	go tool cover -func=coverage.out | awk '/^total:/ { gsub(/%/, "", $$3); if ($$3 < 95) exit 1 }'

db-up:
	docker compose up -d postgres redis kafka

migrate:
	for migration in migrations/*.sql; do docker compose exec -T postgres psql -U orderflow -d orderflow < "$$migration"; done

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f api

k8s-apply:
	kubectl apply -k k8s
