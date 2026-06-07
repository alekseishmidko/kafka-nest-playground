# notification-service

NestJS service for email, push and webhook notifications.

Publishes:

- notification delivery result events can be added later.

Consumes:

- `notification.notification-commands`
- `order.order-events`
- `payment.payment-events`

## Responsibility

- Consume explicit `NotificationCommand` messages.
- React to `OrderCreated`, `PaymentAuthorized` and `PaymentFailed` domain events.
- Preserve `correlationId` and use the consumed event id as delivery causation id.
- Perform deterministic mock delivery by logging the notification payload.

## Event Handling

| Source topic | Event | Template |
| --- | --- | --- |
| `notification.notification-commands` | `NotificationCommand` | command payload `template` |
| `order.order-events` | `OrderCreated` | `order.created` |
| `payment.payment-events` | `PaymentAuthorized` | `payment.authorized` |
| `payment.payment-events` | `PaymentFailed` | `payment.failed` |

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
```

## Runtime

`notification-service` is a background Kafka worker. It does not expose HTTP endpoints.
The startup log identifies it with `transport=worker`; there is no application port.
