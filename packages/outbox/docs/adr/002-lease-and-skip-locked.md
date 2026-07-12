# ADR 002: Lease and SKIP LOCKED for multi-replica publishers

## Status

Accepted.

## Context

В production publisher может быть запущен в нескольких репликах. Если две
реплики прочитают одну `PENDING` строку и одновременно опубликуют event, будет
лишний дубль. Дубли допустимы после crash window, но не должны создаваться
обычной конкурентной работой.

## Decision

`PostgresOutboxStore.findPublishable` атомарно выбирает и помечает строки:

```sql
select id
from outbox_events
where status in (...)
  and (locked_until is null or locked_until <= now())
for update skip locked
```

Затем выставляет:

```text
locked_by
locked_until
```

`markPublished` и `markFailed` очищают lease.

## Consequences

Плюсы:

- две реплики не публикуют одну строку одновременно;
- падение реплики после claim не оставляет строку заблокированной навсегда;
- `locked_by` помогает диагностировать stuck rows.

Минусы:

- lease duration нужно выбирать больше обычного времени publish batch;
- при слишком коротком lease возможны лишние повторы во время долгой Kafka
  публикации;
- это не отменяет requirement downstream idempotency.
