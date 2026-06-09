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
6. `order-service` updates status to `RISK_APPROVED` or `RISK_REJECTED`.
7. `payment-service` publishes `PaymentAuthorized` or `PaymentFailed` after risk approval.
8. `order-service` updates status to `PAYMENT_AUTHORIZED` or `PAYMENT_FAILED`.

Incoming Kafka events are deduplicated by `eventId` through
`processed_kafka_events`.

Kafka key is `orderId` for order/risk/payment topics.

## Statuses

- `PENDING`
- `RISK_APPROVED`
- `RISK_REJECTED`
- `PAYMENT_AUTHORIZED`
- `PAYMENT_FAILED`

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
