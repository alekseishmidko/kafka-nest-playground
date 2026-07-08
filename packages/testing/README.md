# @kafka-playground/testing

Shared package for integration tests, fixtures and contract tests.

Planned responsibilities:

- Testcontainers setup.
- Kafka topic fixtures.
- event fixture factories.
- schema compatibility tests.
- end-to-end flow helpers.

## Order Pipeline E2E

Smoke-test for the full `order -> risk -> payment -> order` flow:

```bash
pnpm test:e2e:order-pipeline
```

## Production E2E Gate

Обязательный production-like gate для полного order flow:

```bash
pnpm test:e2e:gate
```

Gate проверяет:

- создание заказа через Gateway API;
- публикацию `OrderCreated` в Kafka;
- обработку risk-service;
- обработку payment-service;
- переход заказа в терминальный статус;
- consumption финального order event в `notification-service`;
- идемпотентность duplicate message;
- защиту outbox от одновременной публикации одной записи двумя publisher-репликами.

Перед запуском gate нужен чистый pending outbox. Если в `outbox_events`
остался старый backlog, gate падает до создания заказа: outbox publisher
обрабатывает записи oldest-first, поэтому старые `PENDING` события могут скрыть
реальное состояние нового flow.

Prerequisites:

- infrastructure is running: `pnpm infra:up`
- Avro schemas are registered: `pnpm contracts:schemas:register`
- `gateway-service`, `order-service`, `risk-service` or `risk-service-go`, and `payment-service` are running
- Postgres connection matches `E2E_POSTGRES_*` env vars or the local defaults from `infrastructure/.env`

Useful overrides:

```bash
E2E_GATEWAY_URL=http://localhost:3000 \
E2E_POSTGRES_HOST=localhost \
E2E_POSTGRES_PORT=5432 \
E2E_POSTGRES_USER=postgres \
E2E_POSTGRES_PASSWORD=postgres \
E2E_POSTGRES_DB=kafka_playground \
pnpm test:e2e:order-pipeline
```

## Order Pipeline Load Test

Нагрузочный тест отвечает на вопрос: сколько заказов в секунду выдерживает
синхронная часть публичного API до роста latency и ошибок.

Сценарий находится в:

```text
packages/testing/load/order-pipeline.k6.js
```

Он выполняет `POST /orders` через gateway и измеряет:

- `http_req_duration` для `POST /orders`;
- `http_req_failed`;
- custom metric `order_create_errors`;
- custom metric `order_create_latency`;
- custom counter `orders_created`.

Важно: k6 измеряет только синхронный путь HTTP-запроса:

```text
client -> gateway-service -> gRPC order-service
       -> PostgreSQL transaction -> outbox insert -> HTTP response
```

Асинхронный хвост pipeline нужно смотреть в Grafana/Prometheus:

- `outbox_events{status="PENDING"|"FAILED"}`;
- Kafka consumer lag;
- retry rate;
- DLQ backlog;
- p95 Kafka handler duration;
- PostgreSQL CPU/connections/locks;
- Node.js event loop lag и memory.

### Установка k6

k6 используется как внешний CLI-инструмент, а не npm-зависимость.

```bash
brew install k6
```

Для Linux используйте инструкцию из документации Grafana k6:

```text
https://grafana.com/docs/k6/latest/set-up/install-k6/
```

### Запуск

Smoke-проверка на малой нагрузке:

```bash
pnpm test:load:smoke
```

Обычный ступенчатый load test:

```bash
pnpm test:load:order-pipeline
```

Более агрессивный stress profile:

```bash
pnpm test:load:stress
```

### Сохранённый baseline

Baseline фиксирует не только k6-результат, а полный operational snapshot:

- RPS;
- p95/p99 latency;
- error rate;
- Kafka consumer lag;
- outbox/DLQ backlog;
- Node.js CPU/RAM по Prometheus;
- PostgreSQL connections по `pg_stat_activity`.

Создать или обновить локальный эталон:

```bash
pnpm test:load:baseline
```

Эталон сохраняется в:

```text
packages/testing/baselines/order-pipeline.local.json
```

Полный timestamp-отчёт каждого прогона сохраняется в:

```text
packages/testing/reports/load-baseline/
```

Сравнить текущий прогон с сохранённым эталоном:

```bash
pnpm test:load:compare
```

После оптимизации смотрите не только “RPS вырос”, а весь набор метрик. Плохой
результат: RPS вырос, но p99, Kafka lag, outbox backlog или Postgres connections
растут быстрее, чем ожидалось.

Перед запуском нужны:

- `pnpm infra:up`;
- `pnpm contracts:schemas:register`;
- `pnpm --filter order-service migration:run`;
- запущенные `gateway-service`, `order-service`, `risk-service` или
  `risk-service-go`, `payment-service`, `notification-service`.

### Настройки

```env
LOAD_GATEWAY_URL=http://localhost:3000
LOAD_PROFILE=smoke|load|stress
LOAD_HTTP_P95_THRESHOLD_MS=500
LOAD_HTTP_P99_THRESHOLD_MS=1000
LOAD_HTTP_ERROR_RATE_THRESHOLD=0.01
LOAD_ORDER_ERROR_RATE_THRESHOLD=0.01
LOAD_PRE_ALLOCATED_VUS=50
LOAD_MAX_VUS=200
LOAD_STAGE_1_RPS=10
LOAD_STAGE_2_RPS=25
LOAD_STAGE_3_RPS=50
LOAD_STAGE_4_RPS=100
LOAD_BASELINE_RPS=25
LOAD_BASELINE_DURATION=5m
LOAD_PROMETHEUS_URL=http://localhost:9090
LOAD_POSTGRES_SAMPLE_INTERVAL_MS=5000
```

Пример:

```bash
LOAD_GATEWAY_URL=http://localhost:3000 \
LOAD_STAGE_4_RPS=150 \
LOAD_HTTP_P95_THRESHOLD_MS=700 \
pnpm test:load:order-pipeline
```

### Как читать результат

Если k6 thresholds прошли, но в Grafana растёт outbox backlog или Kafka lag,
значит HTTP API принимает заказы быстрее, чем асинхронные workers их
обрабатывают. Это не видно только по HTTP latency, поэтому load test всегда
нужно смотреть вместе с operational metrics.

## Local Developer Experience

Новый разработчик может поднять инфраструктуру и подготовить локальное окружение
одной командой:

```bash
pnpm setup:local
```

Команда выполняет:

- `pnpm install`;
- `pnpm infra:up`;
- `pnpm contracts:schemas:register`;
- `pnpm --filter order-service migration:run`.

Проверить локальный стенд production-like flow:

```bash
pnpm verify:local
```

Команда сама стартует `gateway`, `order`, `risk`, `payment` и `notification`
dev-сервисы, ждёт их readiness endpoints, запускает production e2e gate и затем
останавливает дочерние процессы. Логи сервисов остаются в stdout команды, чтобы
ошибку можно было видеть без ручного поиска по нескольким терминалам.

## Transactional Outbox E2E

Happy-path проверка создаёт заказ и подтверждает три независимых факта:

- запись `OrderCreated` существует в `outbox_events`;
- publisher перевёл её в `PUBLISHED`;
- событие с тем же `eventId` реально прочитано из Kafka.

```bash
pnpm test:e2e:outbox
```

Failure-проверка останавливает Kafka через Docker Compose, создаёт заказ,
дожидается `FAILED`, восстанавливает Kafka и проверяет переход в `PUBLISHED`:

```bash
pnpm test:e2e:outbox-failure
```

Тест всегда пытается поднять Kafka обратно в `finally`, но требует доступ к
локальному Docker daemon. Во время выполнения не запускайте параллельно другие
сценарии, которым нужна Kafka.

Проверка идемпотентности дважды публикует одно событие `PaymentAuthorized` с
одинаковым `eventId` и подтверждает, что:

- в `processed_kafka_events` появилась ровно одна строка;
- статус заказа обновился;
- повторная доставка не изменила `orders.updatedAt`.

```bash
pnpm test:e2e:idempotency
```

## Order Lifecycle E2E

Один сценарий проверяет пять правил финальной state machine:

- `PENDING -> RISK_APPROVED -> CONFIRMED`;
- `PENDING -> CANCELLED` после `OrderRiskRejected`;
- `RISK_APPROVED -> CANCELLED` после `PaymentFailed`;
- повтор одного `eventId` не создаёт вторую outbox-запись;
- событие в неправильном порядке не меняет терминальный статус.

```bash
pnpm test:e2e:order-lifecycle
```

Тест напрямую публикует Avro risk/payment events, поэтому не зависит от
настроек scoring и вероятности отказа mock payment provider.

Общие настройки:

```env
E2E_GATEWAY_URL=http://localhost:3000
E2E_KAFKA_BROKERS=localhost:9092
E2E_SCHEMA_REGISTRY_URL=http://localhost:8081
E2E_POSTGRES_HOST=localhost
E2E_POSTGRES_PORT=55432
E2E_POSTGRES_USER=postgres
E2E_POSTGRES_PASSWORD=postgres
E2E_POSTGRES_DB=kafka_playground
E2E_TIMEOUT_MS=60000
E2E_POLL_INTERVAL_MS=500
```

## DLQ Management E2E

Сценарий проверяет полный административный цикл:

```text
невалидное risk-событие
 -> DeadLetterEvent
 -> dead_letter_events
 -> POST /admin/dlq/:id/reprocess
 -> outbox_events
 -> risk.risk-events
 -> заказ RISK_APPROVED
```

Одновременно проверяется сохранение distributed trace:

```text
known traceparent
 -> retry-5s headers
 -> dead-letter.events headers
 -> HTTP reprocess traceparent
 -> outbox_events.trace_context
 -> reprocessed Kafka headers
```

Запуск:

```bash
pnpm test:e2e:dlq-management
```

Дополнительная настройка:

```env
E2E_ORDER_ADMIN_URL=http://localhost:3003
E2E_DLQ_OPERATOR_API_KEY=local-dlq-operator-key
```

Требуются запущенные Kafka, Schema Registry, PostgreSQL и `order-service`.
Миграции `dead_letter_events` и `dlq_audit_log` должны быть применены. Тест
передаёт текущую optimistic version и обязательный комментарий оператора.
