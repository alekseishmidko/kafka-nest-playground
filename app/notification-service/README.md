# notification-service

NestJS service for email, push and webhook notifications.

Publishes:

- notification delivery result events can be added later.

Consumes:

- `notification.notification-commands`
- `order.order-events`

## Responsibility

- Consume explicit `NotificationCommand` messages.
- React to final `OrderConfirmed` and `OrderCancelled` domain events.
- Preserve `correlationId` and use the consumed event id as delivery causation id.
- Perform deterministic mock delivery by logging the notification payload.
- Store processed event ids in durable `kafka_consumer_inbox`.
- Pass the source `eventId` to a delivery provider as `idempotencyKey`.

## Idempotency

Completed Kafka events are skipped after restart. A real email/push/webhook
adapter must forward `idempotencyKey` to the provider or enforce uniqueness in
its own delivery table. This closes the crash window between external delivery
and marking the inbox record as `COMPLETED`.

## Event Handling

| Source topic | Event | Template |
| --- | --- | --- |
| `notification.notification-commands` | `NotificationCommand` | command payload `template` |
| `order.order-events` | `OrderConfirmed` | `order.confirmed` |
| `order.order-events` | `OrderCancelled` | `order.cancelled` |

`notification.notification-commands` uses Avro subject:

```text
notification.notification-commands-NotificationCommand-value
```

## Configuration

```env
APP_ENV=local
LOG_LEVEL=debug

KAFKA_CLIENT_ID=notification-service
KAFKA_BROKERS=localhost:9092
KAFKA_CONSUMER_GROUP_ID=notification-service
SCHEMA_REGISTRY_URL=http://localhost:8081

POSTGRES_HOST=localhost
POSTGRES_PORT=55432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=kafka_playground
POSTGRES_SSL=false

NOTIFICATION_DEFAULT_RECIPIENT=ops@example.com
```

## Local Run

```bash
pnpm infra:up
pnpm contracts:schemas:register
pnpm dev:notification
```

## Checks

```bash
pnpm --filter notification-service lint
pnpm --filter notification-service build
pnpm --filter notification-service test
```

## Runtime

`notification-service` is a background Kafka worker. It does not expose HTTP endpoints.
The startup log identifies it with `transport=worker`; there is no application port.
