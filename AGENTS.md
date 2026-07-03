# Agent Instructions

## Project Snapshot

This is a Kafka learning playground for a marketplace order pipeline.

- Monorepo layout: `app/*` applications and `packages/*` shared libraries.
- Runtime stack: NestJS services, one Go risk service, Kafka, Schema Registry, PostgreSQL, Prometheus, Grafana, Tempo.
- Package manager: `pnpm@9.15.4`; Node.js target is 22+.
- Main flow: `gateway-service` -> `order-service` -> Kafka -> `risk-service`/`risk-service-go` -> `payment-service` -> `order-service` -> `notification-service`.
- Primary learning topics: Avro contracts, ordering by Kafka key, transactional outbox, durable inbox/idempotency, retry topics, DLQ management, metrics, and distributed tracing.

## Important Paths

- Root docs: `README.md`, `MONOREPO.md`, `kafka-readme.md`.
- Infrastructure: `infrastructure/docker-compose.yml`, `infrastructure/.env`.
- Event contracts and topics: `packages/contracts/src/events.ts`, `packages/contracts/src/topics.ts`, `packages/contracts/schemas`.
- Kafka toolkit: `packages/kafka/src`.
- Outbox toolkit: `packages/outbox/src`.
- Observability helpers: `packages/observability/src`.
- E2E scripts: `packages/testing/scripts`.
- Order domain and orchestration: `app/order-service/src/modules/orders`.
- DLQ admin flow: `app/order-service/src/modules/dlq`.
- Order DB migrations: `app/order-service/src/database/migrations`.
- Gateway API: `app/gateway-service/src/modules/orders`.
- Worker services: `app/risk-service/src/modules/risk`, `app/payment-service/src/modules/payment`, `app/notification-service/src/modules/notification`.
- Go risk service: `app/risk-service-go`.

## Local Commands

Install dependencies:

```bash
pnpm install
```

Start local infrastructure:

```bash
pnpm infra:up
```

Register Avro schemas and run order-service migrations:

```bash
pnpm contracts:schemas:register
pnpm --filter order-service migration:run
```

Run the main services in separate terminals:

```bash
pnpm dev:gateway
pnpm dev:order
pnpm dev:risk
pnpm dev:payment
pnpm dev:notification
```

Use the Go risk implementation instead of the NestJS one when needed:

```bash
pnpm dev:risk-service-go
```

Project checks:

```bash
pnpm build
pnpm lint
pnpm test
```

Useful focused checks:

```bash
pnpm --filter order-service test
pnpm --filter order-service lint
pnpm --filter @kafka-playground/kafka lint
pnpm --filter @kafka-playground/contracts build
pnpm test:e2e:order-pipeline
pnpm test:e2e:order-lifecycle
pnpm test:e2e:dlq-management
pnpm test:e2e:consumer-inbox
```

Go service checks:

```bash
cd app/risk-service-go
go test ./...
```

## Local Addresses

- Gateway API: `http://localhost:3000`
- Swagger UI: `http://localhost:3000/docs`
- Order service gRPC: `localhost:50052`
- Order DLQ Admin API: `http://localhost:3003/admin/dlq`
- Order metrics: `http://localhost:3003/metrics`
- Risk metrics: `http://localhost:3004/metrics`
- Payment metrics: `http://localhost:3005/metrics`
- Notification metrics: `http://localhost:3006/metrics`
- Kafka: `localhost:9092`
- Kafka UI: `http://localhost:8080`
- Schema Registry: `http://localhost:8081`
- PostgreSQL: `localhost:55432`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3001`
- Tempo: `http://localhost:3200`

## Architectural Rules

- Keep message contracts schema-first. Update Avro schemas, generated/TypeScript contract types, and registration behavior together.
- Kafka messages use an envelope with `eventId`, `eventType`, `eventVersion`, `occurredAt`, `correlationId`, `causationId`, `producer`, and `payload`.
- Topic names live in `packages/contracts/src/topics.ts`; do not duplicate topic string literals in services.
- Preserve ordering keys: order flow uses `orderId`; inventory uses `productId`; user events use `userId`.
- Do not publish business events directly after a database write when transactional safety matters. Use transactional outbox.
- Consumer side effects must remain idempotent. Use the durable inbox pattern for retryable Kafka workers.
- Retry and DLQ behavior belongs in `packages/kafka`; service handlers should focus on domain behavior.
- DLQ reprocess must keep auditability: validate payload changes, keep original causation, and persist decisions transactionally.
- Tracing context is technical metadata. Keep it in Kafka headers and outbox `trace_context`, not in domain payloads or Avro contracts.
- Metrics should avoid high-cardinality labels. Prefer service, topic, event type, result, and retry stage over IDs.
- Shared behavior belongs in `packages/*` only when at least two services need it or it is part of the Kafka/outbox/observability platform.

## Coding Conventions

- Follow the existing CommonJS TypeScript setup and NestJS module style.
- Prefer existing helpers from `@kafka-playground/config`, `@kafka-playground/contracts`, `@kafka-playground/kafka`, `@kafka-playground/outbox`, and `@kafka-playground/observability`.
- Keep imports through public package entrypoints unless editing internals of that package.
- Add TypeORM migrations for database schema changes; do not rely on synchronize-style behavior.
- Keep comments sparse and only for non-obvious transactional, retry, or tracing behavior.
- Do not commit secrets. Existing local `.env.local` and `infrastructure/.env` are development defaults only.
- Avoid unrelated refactors while changing pipeline behavior; these services are tightly coupled through contracts and E2E scripts.

## Before Finishing a Task

- Run the narrowest relevant check first, then broader checks when shared packages or contracts changed.
- For contract changes, run `pnpm --filter @kafka-playground/contracts build` and consider `pnpm contracts:schemas:register` against running infrastructure.
- For order-service domain, DLQ, outbox, or inbox changes, run `pnpm --filter order-service test` and the matching E2E script when infrastructure is available.
- For shared Kafka changes, run `pnpm --filter @kafka-playground/kafka lint` plus at least one order pipeline or consumer inbox E2E check when possible.
- Report clearly when an infra-dependent check was not run because Docker/Kafka/PostgreSQL was unavailable.
