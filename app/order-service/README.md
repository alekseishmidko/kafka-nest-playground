# Order Service

`order-service` управляет заказами, transactional outbox, обработкой событий
жизненного цикла заказа и административным хранилищем Dead Letter Queue.

Сервис является hybrid NestJS application:

- gRPC `0.0.0.0:50052` обслуживает команды заказов;
- HTTP `0.0.0.0:3003` обслуживает внутренний DLQ Admin API;
- Kafka consumers обрабатывают risk, payment и DLQ events;
- PostgreSQL хранит заказы, outbox, consumer idempotency и DLQ.

Outbox-инфраструктура вынесена в общий пакет `@kafka-playground/outbox`.
`order-service` остаётся владельцем доменных транзакций и схемы таблицы, но
publisher, TypeORM entity и store-контракты теперь переиспользуемы другими
сервисами.

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
http://localhost:3003/admin
```

Каждый запрос должен передавать:

```http
X-Admin-Api-Key: <secret>
```

Роли задаются разными переменными окружения:

| Переменная | Роль | Permissions |
| --- | --- | --- |
| `DLQ_ADMIN_VIEWER_API_KEY` | `ADMIN_VIEWER` | `admin:read` |
| `DLQ_ADMIN_OPERATOR_API_KEY` | `ADMIN_OPERATOR` | `admin:read`, `admin:write`, `admin:dangerous` |

Ключи нельзя хранить в Git. В production их следует выдавать через secret
manager и регулярно ротировать. Текущий rate limit равен 60 запросам в минуту
на ключ.

Rate limit поддерживает два backend-а:

| Переменная | Значение | Для чего |
| --- | --- | --- |
| `ADMIN_RATE_LIMIT_BACKEND` | `memory` | Local/dev режим без внешней зависимости. Каждая replica считает лимит отдельно. |
| `ADMIN_RATE_LIMIT_BACKEND` | `redis` | Production режим для нескольких replicas. Все процессы используют общий Redis-счётчик. |
| `ADMIN_RATE_LIMIT_REDIS_URL` | `redis://redis:6379` | Redis endpoint для общего limiter-а. |
| `ADMIN_RATE_LIMIT_MAX_REQUESTS` | `60` | Сколько запросов разрешено в окне. |
| `ADMIN_RATE_LIMIT_WINDOW_MS` | `60000` | Размер fixed-window окна. |

Для нескольких replicas используйте `ADMIN_RATE_LIMIT_BACKEND=redis`. Если
Redis недоступен, Admin API возвращает `429`, потому что безопаснее временно
закрыть admin endpoints, чем обходить общий limiter и разрешить каждой replica
считать лимит независимо.

Auth, RBAC и rate limit реализованы общим `AdminSecurityModule`. Названия
переменных окружения пока сохраняют `DLQ_` prefix для обратной совместимости с
локальными `.env` и e2e-сценариями, но guards/decorators больше не зависят от
DLQ-модуля.

Текущая классификация permissions:

| Permission | Для чего |
| --- | --- |
| `admin:read` | Только чтение admin state: DLQ/outbox list и details. |
| `admin:write` | Ограниченные изменения без повторной публикации событий, например `ignore`. |
| `admin:dangerous` | Действия, которые могут повторно опубликовать события или запустить reprocess/retry. |

Текущие endpoints:

| Endpoint | Permission |
| --- | --- |
| `GET /admin/dlq` | `admin:read` |
| `GET /admin/dlq/:id` | `admin:read` |
| `POST /admin/dlq/:id/reprocess` | `admin:dangerous` |
| `POST /admin/dlq/:id/ignore` | `admin:write` |
| `GET /admin/outbox` | `admin:read` |
| `GET /admin/outbox/:id` | `admin:read` |
| `POST /admin/outbox/:id/retry` | `admin:dangerous` |
| `POST /admin/outbox/retry-failed` | `admin:dangerous` |
| `POST /admin/outbox/:id/ignore` | `admin:write` |
| `GET /admin/audit-events` | `admin:read` |
| `GET /admin/audit-events/:id` | `admin:read` |

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

### Outbox Admin API

Outbox endpoints используют те же `X-Admin-Api-Key`, роли, rate limit и общий
`admin_audit_events`, что и DLQ endpoints.

Посмотреть stuck/failed события:

```http
GET /admin/outbox?status=FAILED&limit=50&offset=0
X-Admin-Api-Key: <viewer-or-operator-key>
```

Посмотреть одну запись:

```http
GET /admin/outbox/{id}
X-Admin-Api-Key: <viewer-or-operator-key>
```

Повторить одну `FAILED` запись прямо сейчас:

```http
POST /admin/outbox/{id}/retry
X-Admin-Api-Key: <operator-key>
```

Команда не публикует событие напрямую. Она снимает `next_attempt_at` и lease,
после чего обычный `OutboxPublisherService` забирает запись тем же путём, что и
автоматический retry. Это сохраняет единую delivery logic.

Повторить пачку `FAILED` записей:

```http
POST /admin/outbox/retry-failed?limit=100
X-Admin-Api-Key: <operator-key>
```

Исключить запись из публикации после ручного расследования:

```http
POST /admin/outbox/{id}/ignore
Content-Type: application/json
X-Admin-Api-Key: <operator-key>

{
  "reason": "Событие относится к тестовым данным и не должно публиковаться"
}
```

`ignore` переводит только `PENDING` или `FAILED` записи в `IGNORED`.
`PUBLISHED` записи менять нельзя.

### Общий audit trail

Каждый HTTP-запрос к `/admin/*` дополнительно сохраняется в
`admin_audit_events`. Это общий журнал для всех admin endpoints, не только DLQ:
reprocess, ignore, будущий outbox replay, ручной retention run или изменение
состояния заказа должны оставлять одинаковый след.

Запись содержит:

- `actor` и `role` из проверенного admin principal-а;
- `method`, `path`, `action`;
- `entity_type`, `entity_id`;
- `decision`: `ALLOWED`, `DENIED` или `FAILED`;
- `request_id`, `correlation_id`;
- `ip`, `user_agent`, `duration_ms`;
- `created_at`.

`dlq_audit_log` остаётся доменным журналом решения по конкретной DLQ-записи, а
`admin_audit_events` отвечает на общий эксплуатационный вопрос: кто и когда
обращался к admin API и чем закончился запрос.

Посмотреть audit trail:

```http
GET /admin/audit-events?decision=ALLOWED&entityType=outbox_event&limit=50&offset=0
X-Admin-Api-Key: <viewer-or-operator-key>
```

Поддерживаемые фильтры работают как exact match: `actor`, `role`, `method`,
`path`, `action`, `entityType`, `entityId`, `decision`. `limit` должен быть в
диапазоне `1..200`.

Посмотреть одну audit-запись:

```http
GET /admin/audit-events/{id}
X-Admin-Api-Key: <viewer-or-operator-key>
```

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
