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

## Быстрый старт в NestJS-сервисе

### 1. Подключить entity и providers

```ts
import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import {
  OutboxEventEntity,
  OutboxPublisherService,
  PostgresOutboxStore
} from "@kafka-playground/outbox";

@Module({
  imports: [
    TypeOrmModule.forFeature([
      OrderEntity,
      OutboxEventEntity
    ])
  ],
  providers: [
    OrdersRepository,
    OrdersService,
    OutboxPublisherService,
    PostgresOutboxStore
  ],
  exports: [OutboxPublisherService, PostgresOutboxStore]
})
export class OrdersModule {}
```

`OutboxPublisherService` сам запускает polling в `onModuleInit`. Если сервис
пишет outbox, но не должен публиковать события, не регистрируйте publisher в
этом процессе.

### 2. Добавить миграцию

Приложение должно создать таблицу `outbox_events`. В текущем проекте это делает
initial migration order-service. Для нового сервиса можно использовать helper:

```ts
import {
  createOutboxSchemaQueries,
  dropOutboxSchemaQueries
} from "@kafka-playground/outbox";

export class CreateOutbox1717977600000 {
  async up(queryRunner: QueryRunner): Promise<void> {
    for (const query of createOutboxSchemaQueries()) {
      await queryRunner.query(query);
    }
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    for (const query of dropOutboxSchemaQueries()) {
      await queryRunner.query(query);
    }
  }
}
```

Если сервис использует кастомную initial migration, убедитесь, что в схеме есть
поля:

```text
status
attempts
next_attempt_at
locked_by
locked_until
trace_context
last_error
```

`locked_by` и `locked_until` обязательны для безопасной работы нескольких
publisher replicas.

## Использование в транзакции

```ts
await dataSource.transaction(async (manager) => {
  const order = await manager.save(OrderEntity, createOrder());
  const event = createOrderCreatedEvent(order);

  await manager.save(
    createOutboxEventEntity({
      topic: "order.order-events",
      messageKey: order.id,
      event,
      traceContext: captureActiveTraceContext()
    })
  );
});
```

Важно: запись outbox создаётся внутри той же транзакции, что и бизнес-данные.
Не создавайте outbox-запись после commit отдельным запросом.

## Пример repository method

```ts
async createPendingOrderWithOutbox(
  params: CreatePendingOrderParams
): Promise<OrderEntity> {
  return this.dataSource.transaction(async (manager) => {
    const order = await manager.save(
      manager.create(OrderEntity, {
        userId: params.userId,
        status: OrderStatus.Pending,
        totalAmount: params.totalAmount.toFixed(2)
      })
    );

    const event: OrderCreatedEvent = {
      eventId: randomUUID(),
      eventType: "OrderCreated",
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      correlationId: randomUUID(),
      causationId: null,
      producer: "order-service",
      payload: {
        orderId: order.id,
        userId: order.userId,
        currency: order.currency,
        totalAmount: Number(order.totalAmount),
        itemCount: order.itemCount
      }
    };

    await manager.save(
      createOutboxEventEntity({
        topic: EVENT_TOPIC_MAP.OrderCreated,
        messageKey: order.id,
        event,
        traceContext: captureActiveTraceContext()
      })
    );

    return order;
  });
}
```

## Ускорение публикации после commit

После успешной бизнес-команды можно вызвать fast path:

```ts
void this.outboxPublisher.publishPending();
```

Это не гарантия доставки. Гарантия уже находится в persisted outbox row.
Fast path только сокращает latency до публикации; если вызов не выполнится,
interval publisher заберёт запись позже.

## Статусы

| Статус | Значение |
| --- | --- |
| `PENDING` | Запись создана и ожидает публикации. |
| `FAILED` | Последняя попытка упала; будет retry после `next_attempt_at`. |
| `PUBLISHED` | Publish завершился успешно. |
| `IGNORED` | Оператор явно исключил запись из публикации через admin API. |

Publisher выбирает только:

```text
PENDING
FAILED with next_attempt_at <= now()
```

`PUBLISHED` и `IGNORED` не публикуются.

## Admin и диагностика

Order-service предоставляет admin API поверх outbox:

```http
GET  /admin/outbox?status=FAILED
GET  /admin/outbox/{id}
POST /admin/outbox/{id}/retry
POST /admin/outbox/retry-failed?limit=100
POST /admin/outbox/{id}/ignore
```

Эти endpoints не являются частью пакета `@kafka-playground/outbox`; они живут в
`app/order-service`, потому что auth/RBAC/audit являются приложенческой
политикой. Пакет предоставляет только storage и publisher primitives.

## Метрики

`OutboxPublisherService` через `ApplicationMetrics` пишет:

```text
outbox_publish_attempts_total{topic,result}
```

Order-service дополнительно снимает DB snapshot:

```text
outbox_events{status}
outbox_pending_events
```

Для production alert-ов полезны:

- `PENDING` растёт дольше N минут;
- `FAILED` не возвращается к нулю;
- publish failure rate выше baseline;
- `locked_until` в прошлом, но строка долго не публикуется.

## Типовые ошибки

### Публикация Kafka внутри business transaction

Плохо:

```ts
await dataSource.transaction(async (manager) => {
  await manager.save(order);
  await kafkaProducer.publish(event);
});
```

Если transaction откатится после publish, Kafka уже получила событие о факте,
которого нет в базе.

### Создание outbox после commit

Плохо:

```ts
const order = await repository.save(order);
await outboxRepository.save(event);
```

Если процесс упадёт между двумя строками, событие потеряется.

### Нестабильный message key

Для order flow key должен быть `orderId`. Случайный key ломает ordering одного
агрегата в Kafka partitions.

### Отсутствие consumer inbox

Outbox не даёт exactly-once. Если downstream consumer не идемпотентен, crash
window после publish может вызвать повторный side effect.

## ADR

Package-local decisions:

- [`docs/adr/001-package-api.md`](./docs/adr/001-package-api.md)
- [`docs/adr/002-lease-and-skip-locked.md`](./docs/adr/002-lease-and-skip-locked.md)

Глобальное решение проекта:

- [`../../docs/adr/001-transactional-outbox.md`](../../docs/adr/001-transactional-outbox.md)

## Проверка

```bash
pnpm --filter @kafka-playground/outbox test
```
