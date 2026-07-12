# Outbox Source Guide

Код в этой папке реализует producer-side часть надёжной доставки:

```text
business transaction -> outbox_events -> publisher -> broker
```

## Файлы

| Файл | Ответственность |
| --- | --- |
| `outbox-event.entity.ts` | TypeORM entity таблицы `outbox_events` и enum статусов. |
| `create-outbox-event-entity.ts` | Маленькая фабрика entity из domain event envelope. |
| `outbox-message-store.ts` | Transport-agnostic контракты `TransactionalMessageStore` и `MessagePublisher`. |
| `postgres-outbox.store.ts` | PostgreSQL реализация store: claim через `FOR UPDATE SKIP LOCKED`, mark published/failed, counts. |
| `outbox-publisher.service.ts` | NestJS background publisher и тестируемая функция `publishOutboxBatch`. |
| `outbox-migration-helpers.ts` | SQL helper-ы для приложений, которые хотят собрать свои migrations. |
| `index.ts` | Единственный публичный barrel export пакета. |

## Основной инвариант

Outbox-запись создаётся только внутри той же DB transaction, что и бизнесовое
изменение:

```ts
await dataSource.transaction(async (manager) => {
  const order = await manager.save(OrderEntity, createOrder());

  await manager.save(
    createOutboxEventEntity({
      topic: EVENT_TOPIC_MAP.OrderCreated,
      messageKey: order.id,
      event: createOrderCreatedEvent(order),
      traceContext: captureActiveTraceContext()
    })
  );
});
```

Не вызывайте Kafka producer внутри этой transaction. PostgreSQL transaction
защищает только PostgreSQL; Kafka publish не откатывается вместе с ней.

## Publisher flow

`OutboxPublisherService` периодически вызывает:

```text
findPublishable(batchSize)
  -> publisher.publish(...)
  -> markPublished(id)
```

При ошибке:

```text
publisher.publish(...) throws
  -> markFailed(id, attempts + 1, error)
  -> nextAttemptAt = exponential backoff
```

## Multi-replica защита

`PostgresOutboxStore.findPublishable` использует:

```sql
for update skip locked
```

и lease поля:

```text
locked_by
locked_until
```

Это не даёт двум publisher-репликам одновременно публиковать одну строку.
Если реплика упала после claim, другая реплика сможет забрать строку после
истечения `locked_until`.

## Ожидаемые дубли

Если процесс упал после Kafka publish, но до `markPublished`, запись будет
опубликована ещё раз. Это нормальная цена transactional outbox. Downstream
consumers обязаны использовать durable inbox и дедуплицировать по `eventId`.

## Что менять осторожно

- `OutboxEventStatus`: новый статус влияет на metrics, admin API и publisher
  selection.
- `findPublishable`: ошибка здесь может сломать multi-replica safety.
- `markFailed`: backoff влияет на скорость восстановления после Kafka outage.
- `traceContext`: это техническая metadata, её нельзя переносить в Avro payload.
