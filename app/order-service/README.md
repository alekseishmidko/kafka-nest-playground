# order-service

NestJS service for order creation and order state transitions.

## Publishes

- `order.order-events`

## Consumes

- `risk.risk-events`
- `payment.payment-events`

## Event Flow

1. HTTP/gRPC request creates a `PENDING` order.
2. Order and `OrderCreated` outbox row are committed in one PostgreSQL transaction.
3. `OutboxPublisherService` publishes the persisted event to `order.order-events`.
4. The outbox row becomes `PUBLISHED`; failed attempts are retried with backoff.
5. `risk-service` publishes `OrderRiskApproved` or `OrderRiskRejected`.
6. Risk approval moves the order to `RISK_APPROVED`.
7. Risk rejection atomically moves the order to `CANCELLED` and queues `OrderCancelled`.
8. `payment-service` publishes `PaymentAuthorized` or `PaymentFailed` after risk approval.
9. Payment authorization atomically moves the order to `CONFIRMED` and queues `OrderConfirmed`.
10. Payment failure atomically moves the order to `CANCELLED` and queues `OrderCancelled`.

Incoming Kafka events are deduplicated by `eventId` through
`processed_kafka_events`.

Kafka key is `orderId` for order/risk/payment topics.

## Statuses

- `PENDING`
- `RISK_APPROVED`
- `CONFIRMED`
- `CANCELLED`

`RISK_REJECTED`, `PAYMENT_AUTHORIZED` and `PAYMENT_FAILED` remain in the
PostgreSQL enum only for compatibility with data created before the final state
machine was introduced.

## State Machine

```text
PENDING + OrderRiskApproved       -> RISK_APPROVED
PENDING + OrderRiskRejected       -> CANCELLED + OrderCancelled
RISK_APPROVED + PaymentAuthorized -> CONFIRMED + OrderConfirmed
RISK_APPROVED + PaymentFailed     -> CANCELLED + OrderCancelled
```

All other transitions are rejected. Incoming `eventId` is still persisted as
processed, so invalid or late events do not create an endless Kafka retry loop.

## Runtime

`order-service` does not expose HTTP. It runs as a gRPC server and consumes Kafka events.

Default gRPC bind address:

```text
0.0.0.0:50052
```

The address is written to the startup log as `grpcUrl`.

## Configuration

`order-service` needs a gRPC bind URL plus producer and consumer Kafka clients:

```env
ORDER_GRPC_URL=0.0.0.0:50052
KAFKA_CLIENT_ID=order-service
KAFKA_BROKERS=localhost:9092
KAFKA_CONSUMER_GROUP_ID=order-service
SCHEMA_REGISTRY_URL=http://localhost:8081
TYPEORM_MIGRATIONS_RUN=true
```

## Database Migrations

`TYPEORM_SYNCHRONIZE` is not used. The service runs registered migrations on
startup when `TYPEORM_MIGRATIONS_RUN=true`.

Manual commands:

```bash
pnpm --filter order-service migration:run
pnpm --filter order-service migration:revert
```

The initial migration is safe to baseline a local database previously created
with TypeORM `synchronize`, because tables, enum types and indexes are created
only when absent.

## Tests

```bash
pnpm --filter order-service test
pnpm test:e2e:order-lifecycle
```
