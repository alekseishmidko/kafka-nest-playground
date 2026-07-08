# ADR-005: Retry And DLQ

## Status

Accepted.

## Context

Kafka consumers сталкиваются с разными ошибками:

- временная недоступность PostgreSQL, Kafka или Schema Registry;
- transient network failures;
- невалидный payload;
- несовместимый schema/event contract;
- бизнес-событие, которое нельзя применить к текущему состоянию.

Одинаково retry-ить все ошибки опасно. Poison message может бесконечно
блокировать partition, а transient failure не должен сразу уходить в DLQ.

## Decision

Retry и DLQ являются платформенным поведением shared Kafka layer
`packages/kafka`, а не ручной логикой каждого handler-а.

Retryable failures проходят через retry topics/backoff. Невосстановимые события
попадают в `dead-letter.events` с исходным payload, headers, причиной ошибки,
source topic/partition/offset и audit metadata.

DLQ admin flow в `order-service` позволяет посмотреть событие, исправить payload
и поставить исправленную копию обратно в transactional outbox.

## Consequences

Плюсы:

- service handlers остаются сфокусированы на доменной логике;
- retry policy едина для всех Kafka workers;
- DLQ сохраняет audit trail;
- reprocess не обходит outbox guarantees.

Минусы:

- retry topics и DLQ требуют операционного мониторинга;
- неверная классификация ошибки может либо задержать обработку, либо слишком
  рано отправить событие в DLQ;
- DLQ reprocess должен валидировать исправленный payload.

## Operational Rules

- Handler не должен вручную делать бесконечные retry loops.
- DLQ event должен сохранять original causation и source metadata.
- Reprocess обязан быть транзакционным: audit decision и новая outbox row
  фиксируются вместе.
- Payload fix должен валидироваться до постановки в outbox.
- Метрики должны показывать retry stage, DLQ backlog и результат reprocess.

