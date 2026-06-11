# Order Service

`order-service` управляет заказами, transactional outbox, обработкой событий
жизненного цикла заказа и административным хранилищем Dead Letter Queue.

Сервис является hybrid NestJS application:

- gRPC `0.0.0.0:50052` обслуживает команды заказов;
- HTTP `0.0.0.0:3003` обслуживает внутренний DLQ Admin API;
- Kafka consumers обрабатывают risk, payment и DLQ events;
- PostgreSQL хранит заказы, outbox, consumer idempotency и DLQ.

## DLQ: принцип работы

```text
consumer handler error
  -> retry topics
  -> DeadLetterEvent
  -> dead-letter.events
  -> DlqConsumer
  -> dead_letter_events
  -> решение оператора
       |-> IGNORED
       `-> corrected event -> outbox_events -> original topic
```

`DlqConsumer` не выполняет бизнес-исправления. Он сохраняет входной
`DeadLetterEvent` и технические headers. Уникальный индекс по
`dead_letter_event_id` делает сохранение идемпотентным при повторной Kafka
доставке.

## Таблица `dead_letter_events`

Основные поля:

| Поле | Назначение |
| --- | --- |
| `dead_letter_event_id` | Уникальный `eventId` события из DLQ topic |
| `original_event_id` | `eventId` исходного domain event |
| `original_topic` | Topic, куда разрешено выполнить reprocess |
| `original_partition`, `original_offset` | Исходная Kafka-позиция |
| `message_key` | Key для сохранения partition ordering |
| `error_code`, `error_message`, `error_stack` | Причина последней ошибки |
| `retry_count` | Количество выполненных retry-переходов |
| `first_failed_at` | Время первой ошибки |
| `original_event` | Полный исходный event envelope в JSONB |
| `status` | `NEW`, `REPROCESSED` или `IGNORED` |
| `reprocessed_event_id` | `eventId` новой исправленной копии |

Индекс `(status, created_at desc)` ускоряет основной экран оператора, а индекс
`original_event_id` позволяет найти историю конкретного события.

## Статусы

```text
NEW -> REPROCESSED
NEW -> IGNORED
```

Обратные переходы запрещены. Обе команды используют
`SELECT ... FOR UPDATE` через TypeORM `pessimistic_write`, поэтому два
одновременных HTTP-запроса не смогут принять разные решения по одной записи.

`REPROCESSED` означает, что исправленное событие атомарно сохранено в
`outbox_events`. Фактическая Kafka-публикация может произойти немного позже.
Если Kafka недоступна, outbox останется `FAILED` и повторит отправку.

## Admin API

Базовый адрес локально:

```text
http://localhost:3003/admin/dlq
```

### Получить список

```http
GET /admin/dlq?status=NEW&limit=50&offset=0
```

`status` необязателен. `limit` должен находиться в диапазоне `1..200`.

### Получить одну запись

```http
GET /admin/dlq/{id}
```

### Повторно обработать

```http
POST /admin/dlq/{id}/reprocess
Content-Type: application/json

{
  "payload": {
    "orderId": "a2a25e8e-3bd8-42ed-aafe-0da8889d1a75"
  }
}
```

Перед созданием outbox-записи сервис:

1. Загружает DLQ-запись под row lock.
2. Проверяет статус `NEW`.
3. Объединяет исходный payload и исправленные поля.
4. Валидирует обязательные поля конкретного event type.
5. Проверяет соответствие event type исходному topic.
6. Создаёт новый `eventId`.
7. Сохраняет исходный `eventId` в `causationId`.
8. Очищает retry metadata: новый producer создаст только стандартные headers.
9. В одной транзакции создаёт `outbox_events` и ставит `REPROCESSED`.

### Игнорировать

```http
POST /admin/dlq/{id}/ignore
Content-Type: application/json

{
  "reason": "Событие относится к удалённым тестовым данным"
}
```

Причина обязательна и сохраняется для аудита.

## Запуск миграций

При старте миграции выполняются автоматически, если:

```env
TYPEORM_MIGRATIONS_RUN=true
```

Ручной запуск:

```bash
pnpm --filter order-service migration:run
```

## Проверка

Модульные тесты:

```bash
pnpm --filter order-service test
pnpm --filter @kafka-playground/kafka test
```

Полный E2E:

```bash
pnpm test:e2e:dlq-management
```

Для E2E должны быть запущены инфраструктура, зарегистрированы Avro-схемы и
запущен `order-service`. Тест создаёт изолированный заказ напрямую в PostgreSQL,
публикует неисправимое risk-событие, исправляет его через Admin API и проверяет
успешный переход заказа в `RISK_APPROVED`.

## Безопасность

Admin API является внутренним. Перед production-развёртыванием необходимо:

- закрыть порт сетевой политикой;
- добавить authentication и RBAC;
- логировать identity оператора;
- ограничить размер исправленного payload;
- добавить rate limit;
- не отдавать `errorStack` пользователям без административных прав.
