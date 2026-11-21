# OrderFlow

OrderFlow is a Go REST API for creating and tracking orders. PostgreSQL is the source of truth, Redis speeds up order reads, and Kafka receives order events through a transactional outbox. Prometheus collects API metrics and Grafana provides a preconfigured dashboard.

## Follow an order

```mermaid
flowchart LR
    Client --> API
    API -->|read / cache| Redis
    API -->|order and event in one transaction| PostgreSQL
    Worker[Outbox worker] -->|poll pending events| PostgreSQL
    Worker -->|publish| Kafka
    Prometheus -->|scrape metrics| API
    Grafana -->|query| Prometheus
```

Start the Compose stack below and open [Swagger UI](http://localhost:8080/docs).
The UI loads its assets from jsDelivr. The API also serves the [OpenAPI contract](internal/app/openapi.json) directly at `/openapi.json`.

```bash
# Use a fresh key for a new order. Repeat the same request to replay the response.
curl -i localhost:8080/api/v1/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-001' -d '{"amount":"250.00"}'
curl -i 'localhost:8080/api/v1/orders?status=pending&limit=2'
```

A page defaults to 20 orders and allows up to 100. Pass the response's
`X-Next-Cursor` header as `cursor` with the same filter to fetch the next page.
Reusing a creation key with a different amount or currency returns HTTP 409.

The API returns an `X-Request-ID` that also appears in its JSON request log.
Prometheus exposes cache hits and database fallbacks in `orderflow_cache_reads_total`
and Kafka publish attempts by outcome in `orderflow_kafka_writes_total`.

![HTTP throughput, latency and status codes in Grafana](docs/images/grafana.png)

The Headlamp screenshot below records startup events during the first kind rollout.
The Kafka probe originally timed out after one second, and the image registry lacked
the requested latest tag. The manifests now allow ten seconds and select the main tag.
Use Workloads → Pods to inspect current readiness.

![Historical Kubernetes startup warnings in Headlamp](docs/images/headlamp-events.png)

## Stack

- Go HTTP server with handler, service, and repository layers
- PostgreSQL for orders and the event outbox
- Redis cache with a five-minute TTL and cache-aside reads
- Apache Kafka for `order.created` and `order.status_changed` events
- Prometheus and Grafana for HTTP request rate, latency, and status metrics
- Docker Compose for the local stack

## Checks

Run the test suite with the same 95% total-coverage gate used in CI:

```bash
make coverage
go vet ./...
go build ./...
```

With the Compose API running, execute `bash tests/integration/orders.sh` from
the repository root (requires Bash, curl, jq and Docker Compose). It creates
test orders and checks PostgreSQL, Redis, Kafka delivery and idempotent replay.
GitHub Actions runs this suite on a fresh stack before publishing the image.
For an existing Compose database run `make migrate` after pulling new migrations.
For an existing Kubernetes PostgreSQL volume, apply new SQL migrations before
rolling out the new API, since init scripts run only on first initialization.

## Start the full stack

Requirements: Docker Compose and `curl`.

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
```

On a new database volume, PostgreSQL applies the SQL files in `migrations/` during initialization. The Compose PostgreSQL port is `5433` to avoid colliding with an existing local PostgreSQL on `5432`. Set `POSTGRES_HOST_PORT` in `.env` to change it.

Check readiness and metrics:

```bash
curl -i http://localhost:8080/ready
curl http://localhost:8080/metrics
```

Open the monitoring UI:

- Grafana: <http://localhost:3000> (default login: `admin` / `admin`; set `GRAFANA_ADMIN_PASSWORD` in `.env` to change it)
- Prometheus: <http://localhost:9090>

To stop the stack while preserving database and monitoring data:

```bash
docker compose down
```

To run the Go process directly, start its dependencies first:

```bash
make db-up
make migrate
make run
```

Its default database URL points to the Compose PostgreSQL port `5433`; `DATABASE_URL`, `REDIS_URL`, `KAFKA_BROKERS`, `KAFKA_TOPIC`, and `PORT` can be overridden through environment variables.

## API

All successful endpoints return JSON. Errors use `{"error":"..."}`.

### Create an order

`POST /api/v1/orders`

```bash
curl -i -X POST http://localhost:8080/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"amount":"250.00","currency":"RUB"}'
```

`amount` is a positive decimal with up to two fractional digits. `currency` is an optional three-letter uppercase code and defaults to `RUB`. A successful request returns `201 Created`.

### Get an order

`GET /api/v1/orders/{id}`

```bash
curl -i http://localhost:8080/api/v1/orders/ORDER_UUID
```

Returns `400` for a malformed UUID and `404` if the order does not exist. Successful reads use Redis when available; a Redis failure falls back to PostgreSQL.

### List orders

`GET /api/v1/orders`

```bash
curl -i http://localhost:8080/api/v1/orders
```

Returns a JSON array ordered newest first. An empty database returns `[]`.

### Change order status

`PATCH /api/v1/orders/{id}/status`

```bash
curl -i -X PATCH http://localhost:8080/api/v1/orders/ORDER_UUID/status \
  -H 'Content-Type: application/json' \
  -d '{"status":"processing"}'
```

Allowed transitions are `pending` → `processing` or `failed`, and `processing` → `completed` or `failed`. Completed and failed orders are terminal. Invalid transitions return `409 Conflict`; an unknown status returns `400 Bad Request`.

### Health and monitoring endpoints

- `GET /health` is a process liveness check.
- `GET /ready` checks PostgreSQL and Redis; Kafka delivery is buffered in the outbox, so a temporary broker outage does not stop order creation.
- `GET /metrics` exposes Prometheus metrics.

## Kafka event delivery

Order changes and their outbox events are committed in the same PostgreSQL transaction. A background publisher sends pending events to the `order-events` topic and marks them published. Delivery is at least once: consumers should use the `event-id` Kafka header to deduplicate messages if the publisher restarts after sending but before marking the row published.

## Kubernetes

The `k8s/` Kustomize bundle deploys the API, PostgreSQL, Redis, single-node Kafka, Prometheus, and Grafana. It is intended for a local or small demo cluster with a default StorageClass; the stateful services use persistent volume claims.

Create the namespace and secrets before applying the resources. Use the same PostgreSQL username and password in the URL and database fields; URL-encode special characters in the password inside `DATABASE_URL`.

```bash
kubectl create namespace orderflow
kubectl -n orderflow create secret generic orderflow-secrets \
  --from-literal=DATABASE_URL='postgres://orderflow:CHANGE_ME@postgres:5432/orderflow?sslmode=disable' \
  --from-literal=POSTGRES_USER=orderflow \
  --from-literal=POSTGRES_PASSWORD=CHANGE_ME \
  --from-literal=GRAFANA_ADMIN_PASSWORD=CHANGE_ME
kubectl -n orderflow create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io \
  --docker-username=YOUR_GITHUB_USERNAME \
  --docker-password=YOUR_GHCR_READ_TOKEN
kubectl apply -k k8s
```

The GHCR package must contain an image for this repository. To use a particular build, set the image tag after applying:

```bash
kubectl -n orderflow set image deployment/orderflow-api \
  api=ghcr.io/sobig-f/orderflow:sha-COMMIT_SHA
kubectl -n orderflow rollout status deployment/orderflow-api
```

Forward the API and monitoring services from separate terminals:

```bash
kubectl -n orderflow port-forward service/orderflow-api 8080:8080
kubectl -n orderflow port-forward service/prometheus 9090:9090
kubectl -n orderflow port-forward service/grafana 3000:3000
```

GitHub Actions runs formatting, vet, tests, and build on pull requests and pushes. It publishes a GHCR image on pushes to `main` and version tags. To enable automatic deployment after pushes to `main`, set the repository variable `KUBE_DEPLOY_ENABLED=true` and configure these repository secrets: `KUBE_CONFIG` (raw kubeconfig YAML), `ORDERFLOW_DATABASE_URL`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `GRAFANA_ADMIN_PASSWORD`, `GHCR_USERNAME`, and `GHCR_READ_TOKEN` (with `read:packages`). The workflow creates the namespace secrets and rolls out the commit-tagged image.

For a GitHub repository, make the GHCR package available to the cluster with a read token or set its visibility to public. The bundled Kubernetes databases and broker use one replica each; this setup is for development and portfolio demos.

## Project structure

```text
cmd/api/                          application startup and HTTP routes
internal/cache/                   Redis cache adapter
internal/config/                  environment configuration
internal/handler/                 HTTP request and response handling
internal/messaging/               Kafka publisher and outbox worker
internal/metrics/                 Prometheus HTTP instrumentation
internal/order/                   order models and domain errors
internal/repository/              PostgreSQL queries and transactions
internal/service/                 validation and order workflow rules
migrations/                       PostgreSQL schema migrations
monitoring/                        Prometheus and Grafana configuration
.github/workflows/                 GitHub Actions CI/CD pipeline
k8s/                               Kubernetes manifests and Kustomize entry point
```
