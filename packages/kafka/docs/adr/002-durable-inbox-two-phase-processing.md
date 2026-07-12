# ADR 002: Durable inbox uses prepare/effect phases

## Status

Accepted.

## Context

Kafka доставляет сообщения at-least-once. Consumer может упасть после внешнего
side effect, но до commit/mark completed. Нельзя сделать одну ACID transaction
между PostgreSQL, Kafka и внешним provider-ом.

## Decision

`KafkaIdempotentEventProcessor` разделяет handler на две фазы:

- `prepare`: вычисляет deterministic result и сохраняет его в inbox;
- `effect`: публикует downstream event или вызывает внешний provider;
- после `effect` запись помечается `COMPLETED`.

Если процесс падает между `effect` и `COMPLETED`, повторная попытка берёт
сохранённый result.

## Consequences

Плюсы:

- бизнес-решение не пересчитывается при дублях;
- downstream event может иметь стабильный `eventId`;
- внешние provider calls получают idempotency key;
- повторная доставка Kafka становится штатным сценарием.

Минусы:

- `effect` всё равно может выполниться повторно;
- downstream consumer/provider тоже должен быть идемпотентным;
- `prepare` нельзя делать недетерминированным.
