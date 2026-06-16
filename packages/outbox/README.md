# @kafka-playground/outbox

Переиспользуемый transactional outbox для сервисов, которые сохраняют
бизнес-изменение в PostgreSQL и затем публикуют доменное событие в broker.

## Зачем нужен outbox

Нельзя надёжно выполнить две операции как одну транзакцию:

```text
1. сохранить бизнес-данные в PostgreSQL
2. отправить событие в Kafka
```

Если процесс упадёт между этими шагами, событие может потеряться. Outbox меняет
последовательность:

```text
DB transaction
  -> business rows
  -> outbox_events PENDING
commit
  -> background publisher
  -> Kafka
  -> outbox_events PUBLISHED
```

Если Kafka временно недоступна, строка остаётся в `FAILED` и повторяется после
backoff. Если процесс упал после Kafka publish, но до `PUBLISHED`, событие
может быть опубликовано повторно. Поэтому downstream consumer должен быть
идемпотентным.

## Связка producer и consumer

Полный надёжный pipeline выглядит так:

```text
DB transaction -> outbox -> Kafka -> inbox -> handler
```

- `TransactionalMessageStore` отвечает за producer-side durability.
- `MessagePublisher` доставляет сохранённое сообщение во внешний transport.
- `IdempotentConsumerStore` из `@kafka-playground/kafka` отвечает за
  consumer-side идемпотентность.

Outbox гарантирует "не потерять событие". Inbox гарантирует "не выполнить
бизнес-side effect дважды".

## Основные компоненты

### `OutboxEventEntity`

TypeORM entity таблицы `outbox_events`. Хранит topic, key, event envelope,
trace context, status, attempts, backoff и последнюю ошибку.

### `TransactionalMessageStore`

Контракт хранилища outbox-сообщений. Его можно реализовать поверх другой БД,
если проект не использует TypeORM/PostgreSQL.

### `PostgresOutboxStore`

TypeORM/PostgreSQL реализация `TransactionalMessageStore`.

### `MessagePublisher`

Transport-agnostic контракт публикации. В текущем проекте его выполняет
`KafkaProducerService`, но для другого проекта можно написать adapter под NATS,
RabbitMQ или HTTP webhook.

### `OutboxPublisherService`

NestJS background worker, который polling-ом читает publishable записи,
публикует их и обновляет статус.

### Migration helpers

`createOutboxSchemaQueries()` и `dropOutboxSchemaQueries()` возвращают SQL для
TypeORM migrations. Helper-ы не выполняют запросы сами: приложение остаётся
владельцем порядка DDL-операций.

## Использование в транзакции

```ts
await dataSource.transaction(async (manager) => {
  const order = await manager.save(OrderEntity, createOrder());
  const event = createOrderCreatedEvent(order);

  await manager.save(
    manager.create(OutboxEventEntity, {
      topic: "order.order-events",
      messageKey: order.id,
      eventType: event.eventType,
      eventId: event.eventId,
      event,
      traceContext: captureActiveTraceContext(),
      status: OutboxEventStatus.Pending
    })
  );
});
```

Важно: запись outbox создаётся внутри той же транзакции, что и бизнес-данные.
Не создавайте outbox-запись после commit отдельным запросом.

## Проверка

```bash
pnpm --filter @kafka-playground/outbox test
```
