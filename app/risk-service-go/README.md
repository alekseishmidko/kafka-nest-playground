# risk-service-go

Go service for CPU-heavy risk scoring, antifraud rules and suspicious order checks.

Publishes:

- `risk.risk-events`

Consumes:

- `order.order-events`

## Runtime

The service consumes `OrderCreated` from `order.order-events`, performs deterministic CPU-heavy scoring, and publishes one of:

- `OrderRiskApproved` to `risk.risk-events`
- `OrderRiskRejected` to `risk.risk-events`

Events use the same Confluent Schema Registry Avro wire format as the NestJS services.

## Configuration

Environment files follow the same pattern as `order-service`:

- `.env.local` for local development
- `.env.prod` for container/runtime defaults
- `.env.example` for required keys

Important settings:

- `KAFKA_BROKERS`
- `KAFKA_CONSUMER_GROUP_ID`
- `SCHEMA_REGISTRY_URL`
- `RISK_SCORE_THRESHOLD`
- `RISK_SCORE_ITERATIONS`

## Commands

```bash
go mod tidy
go test ./...
go run ./cmd/risk-service
```

Health endpoints:

- `GET /healthz`
- `GET /readyz`
