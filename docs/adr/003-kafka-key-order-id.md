# ADR-003: Kafka Key Is Order Id

## Status

Accepted.

## Context

Kafka сохраняет порядок сообщений только внутри одной partition. Если события
одного заказа попадут в разные partitions, consumers могут увидеть
`PaymentAuthorized` раньше `OrderCreated` или финальное событие раньше
промежуточного.

Order lifecycle зависит от порядка: `OrderCreated`, risk result, payment result
и финальное состояние должны обрабатываться последовательно для одного заказа.

## Decision

Для order flow Kafka key всегда равен `orderId`.

Это правило применяется к:

- `order.order-events`;
- `risk.risk-events`;
- `payment.payment-events`;
- финальным order events, которые читает `notification-service`.

Другие домены используют свой стабильный aggregate key: inventory - `productId`,
user events - `userId`.

## Consequences

Плюсы:

- все события одного заказа попадают в одну Kafka partition;
- consumers получают упорядоченный поток для конкретного aggregate;
- проще отлаживать causation chain по `orderId`;
- retry и DLQ reprocess сохраняют порядок на уровне aggregate key.

Минусы:

- очень активный `orderId` может стать hot key;
- глобального порядка между разными заказами нет и быть не должно;
- изменение key policy требует миграции producer-ов и consumers.

## Operational Rules

- Не дублировать topic string literals в сервисах; использовать
  `packages/contracts/src/topics.ts`.
- При публикации order event key должен быть `payload.orderId`.
- E2E/chaos tests должны проверять, что reprocess не меняет Kafka key.
- Новые event types обязаны явно определить partition key в contracts.

