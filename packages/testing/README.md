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
