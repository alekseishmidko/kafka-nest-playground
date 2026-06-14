# Order Service

`order-service` управляет заказами, transactional outbox, обработкой событий
жизненного цикла заказа и административным хранилищем Dead Letter Queue.

Сервис является hybrid NestJS application:

- gRPC `0.0.0.0:50052` обслуживает команды заказов;
- HTTP `0.0.0.0:3003` обслуживает внутренний DLQ Admin API;
- Kafka consumers обрабатывают risk, payment и DLQ events;
- PostgreSQL хранит заказы, outbox, consumer idempotency и DLQ.

## Метрики

Prometheus endpoint:

```text
GET http://localhost:3003/metrics
```

`order-service` экспортирует:

- counters обработки, ошибок, retry и DLQ;
- histogram времени Kafka handler-а;
- consumer lag по каждой partition;
- фактический outbox backlog из PostgreSQL;
- число DLQ-записей `NEW`;
- результаты попыток outbox publish;
- стандартные Node.js runtime metrics.

`OperationalMetricsCollector` каждые 15 секунд перечитывает PostgreSQL.
Значения gauges не зависят от памяти процесса и корректно восстанавливаются
после рестарта.

`KafkaLagMonitor` через Kafka Admin API сравнивает latest и committed offsets.
Для новой consumer group без committed offset lag считается от low offset.

Проверка:

```bash
curl http://localhost:3003/metrics
```

Основные выражения и правила cardinality описаны в
[`packages/observability/README.md`](../../packages/observability/README.md).

## Distributed tracing

```text
HTTP POST /orders
 -> gRPC OrdersService/CreateOrder
 -> postgres transaction create order
 -> outbox_events.trace_context
 -> outbox publish
 -> Kafka producer
 -> Kafka consumer
 -> postgres transaction process order event
```

`trace_context` хранится в outbox как JSONB. Фоновый publisher выполняется
после завершения gRPC-запроса и может стартовать после рестарта, поэтому без
persisted context Kafka-публикация стала бы корнем нового trace.

Tracing metadata не добавляется в domain event и Avro contract. Она хранится
только в Kafka headers и техническом поле outbox.

Явные spans:

- `postgres transaction create order`;
- `postgres transaction process order event`;
- `outbox publish`;
- `dlq reprocess`;
- `dlq ignore`.

Внутри них `pg` instrumentation создаёт spans конкретных SQL-запросов.

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
| `resolved_by` | Идентификатор оператора, принявшего решение |
| `resolution_comment` | Обязательное обоснование действия |
| `version` | Версия записи для optimistic locking |

Индекс `(status, created_at desc)` ускоряет основной экран оператора, а индекс
`original_event_id` позволяет найти историю конкретного события. Частичный
индекс `(status, updated_at)` обслуживает retention только для завершённых
записей и не раздувает индекс строками `NEW`.

## Статусы

```text
NEW -> REPROCESSED
NEW -> IGNORED
```

Обратные переходы запрещены. Обе команды одновременно используют:

- `version` для обнаружения устаревшего экрана оператора;
- `SELECT ... FOR UPDATE` для сериализации параллельных транзакций;
- единую транзакцию для изменения DLQ, audit log и outbox.

`REPROCESSED` означает, что исправленное событие атомарно сохранено в
`outbox_events`. Фактическая Kafka-публикация может произойти немного позже.
Если Kafka недоступна, outbox останется `FAILED` и повторит отправку.

## Admin API

Базовый адрес локально:

```text
http://localhost:3003/admin/dlq
```

Каждый запрос должен передавать:

```http
X-Admin-Api-Key: <secret>
```

Роли задаются разными переменными окружения:

| Переменная | Роль | Разрешения |
| --- | --- | --- |
| `DLQ_ADMIN_VIEWER_API_KEY` | `DLQ_VIEWER` | `GET` списка и записи |
| `DLQ_ADMIN_OPERATOR_API_KEY` | `DLQ_OPERATOR` | чтение, reprocess, ignore |

Ключи нельзя хранить в Git. В production их следует выдавать через secret
manager и регулярно ротировать. Текущий rate limit равен 60 запросам в минуту
на ключ в рамках одного процесса. Для нескольких replicas состояние limiter-а
следует перенести в Redis или API gateway.

### Получить список

```http
GET /admin/dlq?status=NEW&limit=50&offset=0
X-Admin-Api-Key: <viewer-or-operator-key>
```

`status` необязателен. `limit` должен находиться в диапазоне `1..200`.

### Получить одну запись

```http
GET /admin/dlq/{id}
X-Admin-Api-Key: <viewer-or-operator-key>
```

### Повторно обработать

```http
POST /admin/dlq/{id}/reprocess
Content-Type: application/json
X-Admin-Api-Key: <operator-key>

{
  "version": 1,
  "comment": "Исправлен orderId после восстановления заказа",
  "payload": {
    "orderId": "a2a25e8e-3bd8-42ed-aafe-0da8889d1a75"
  }
}
```

Перед созданием outbox-записи сервис:

1. Загружает DLQ-запись под row lock.
2. Проверяет статус `NEW` и совпадение `version`.
3. Разрешает изменять только whitelist полей конкретного event type.
4. Объединяет исходный payload и исправленные поля.
5. Валидирует обязательные поля и соответствие исходному topic.
6. Создаёт новый `eventId`.
7. Сохраняет исходный `eventId` в `causationId`.
8. Очищает retry metadata: новый producer создаст только стандартные headers.
9. В одной транзакции создаёт `outbox_events`, audit log и ставит
   `REPROCESSED`.

### Игнорировать

```http
POST /admin/dlq/{id}/ignore
Content-Type: application/json
X-Admin-Api-Key: <operator-key>

{
  "version": 1,
  "reason": "Событие относится к удалённым тестовым данным"
}
```

Причина обязательна, должна содержать от 5 до 1000 символов и сохраняется в
DLQ-записи и неизменяемой таблице `dlq_audit_log`.

## Retention

`DlqRetentionService` раз в сутки удаляет только завершённые
`REPROCESSED`/`IGNORED` записи старше заданного срока:

```env
DLQ_RETENTION_DAYS=90
```

Записи `NEW` не удаляются независимо от возраста. Связанный audit log удаляется
по `ON DELETE CASCADE` только вместе с завершённой записью.

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

Тест также проводит известный `traceId` по цепочке
`retry-5s -> DLQ -> reprocess -> outbox -> Kafka`.

## Безопасность

Admin API уже использует API key, роли, operator identity, whitelist
исправляемых полей, optimistic locking, audit log и rate limit. В production
дополнительно обязательны TLS, сетевые политики, secret manager и фильтрация
`errorStack` для роли viewer.
