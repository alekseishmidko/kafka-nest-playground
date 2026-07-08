# ADR-002: Durable Consumer Inbox

## Status

Accepted.

## Context

Kafka гарантирует at-least-once delivery. Consumer может получить одно и то же
сообщение повторно после rebalance, retry, crash или offset commit failure.

Для side effects этого недостаточно. Например, `payment-service` не должен
создавать два разных платежных результата для одного `OrderRiskApproved`, а
`notification-service` не должен отправлять одно уведомление дважды из-за
повторной доставки Kafka message.

## Decision

Для retryable workers используется durable consumer inbox в таблице
`kafka_consumer_inbox`.

Inbox хранит запись по паре `consumer_name + event_id`, статус обработки,
lease-поля и подготовленный `result`. Если процесс падает после подготовки
результата, но до финального completion, повторная обработка берет сохраненный
result и выполняет downstream effect с тем же `eventId`.

Для простых order lifecycle events также используется таблица
`processed_kafka_events`, чтобы один и тот же `eventId` не менял заказ повторно.

## Consequences

Плюсы:

- duplicate messages не выполняют бизнес-изменение повторно;
- crash window между prepare и publish становится восстанавливаемым;
- rebalance во время обработки не должен создавать второй side effect;
- состояние consumer-а можно диагностировать через БД.

Минусы:

- появляется дополнительная таблица и retention policy;
- обработчик должен разделять prepare result и effect;
- старые незавершенные inbox rows требуют восстановления или ручной диагностики.

## Operational Rules

- Consumer side effects должны проходить через idempotency/inbox layer.
- Ключ идемпотентности - `eventId` входящего Kafka envelope.
- Для downstream publish результат должен быть детерминированным или сохраненным
  в inbox до publish.
- `COMPLETED` inbox rows можно удалять retention job-ом только после
  достаточного окна дедупликации.
- `PROCESSING`/`PREPARED` rows нельзя чистить как обычный мусор.

