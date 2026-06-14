# @kafka-playground/observability

Общий пакет структурированного логирования и Prometheus-метрик NestJS-сервисов.

## Логирование

Регистрация Pino:

```ts
createServiceLoggerModule({
  serviceName: "order-service",
  environment: process.env.APP_ENV ?? "local"
})
```

- `APP_ENV=local`: форматирование через `pino-pretty`;
- `APP_ENV=prod`: newline-delimited JSON;
- `LOG_LEVEL`: переопределяет уровень;
- `x-correlation-id` используется как request ID;
- при отсутствии correlation ID применяется `x-request-id` или UUID.

## Prometheus Metrics

Регистрация:

```ts
MetricsModule.register({
  serviceName: "order-service"
})
```

Модуль создаёт отдельный `prom-client Registry`, добавляет service label,
регистрирует стандартные Node.js метрики и публикует:

```text
GET /metrics
```

### Прикладные метрики

| Метрика | Тип | Значение |
| --- | --- | --- |
| `kafka_events_consumed_total` | Counter | Успешная обработка Kafka events |
| `kafka_events_failed_total` | Counter | Ошибки handler-ов по error code |
| `kafka_retry_events_total` | Counter | Публикации на retry-этап |
| `kafka_dlq_events_total` | Counter | Успешные публикации в DLQ |
| `kafka_event_processing_duration_seconds` | Histogram | Время deserialize + handler, без retry delay |
| `kafka_consumer_lag` | Gauge | Lag по group/topic/partition |
| `outbox_events` | Gauge | DB snapshot по статусам outbox |
| `outbox_pending_events` | Gauge | Текущий PENDING backlog |
| `outbox_publish_attempts_total` | Counter | Успешные и неуспешные publish attempts |
| `dlq_new_events` | Gauge | DLQ-записи, ожидающие решения |

### Правила cardinality

Labels содержат только значения с ограниченным множеством вариантов:

- service;
- topic;
- event type;
- error code;
- retry stage;
- partition;
- status/result.

Нельзя добавлять в labels `eventId`, `orderId`, correlation ID, error message
или stack trace. Для них используются структурированные логи. Каждый уникальный
набор labels создаёт отдельный time series, поэтому идентификаторы приводят к
неконтролируемому потреблению памяти Prometheus.

### Примеры PromQL

Доля ошибок:

```promql
sum(rate(kafka_events_failed_total[5m]))
/
clamp_min(
  sum(rate(kafka_events_consumed_total[5m]))
  + sum(rate(kafka_events_failed_total[5m])),
  0.001
)
```

p95 обработки:

```promql
histogram_quantile(
  0.95,
  sum by (le, topic) (
    rate(kafka_event_processing_duration_seconds_bucket[5m])
  )
)
```

Максимальный lag:

```promql
max by (group, topic) (kafka_consumer_lag)
```

## Тесты

```bash
pnpm --filter @kafka-playground/observability test
```

## OpenTelemetry tracing

Пакет централизует инициализацию OpenTelemetry для всех Node.js сервисов.
Каждый `main.ts` первым импортирует `tracing-bootstrap.ts`, поэтому
instrumentation подключается до загрузки HTTP, gRPC, NestJS, TypeORM и `pg`.

Автоматические spans создаются для HTTP, gRPC, NestJS и PostgreSQL. Kafka
инструментируется вручную в `packages/kafka`, где доступны `eventId`, тип
события, retry stage и DLQ semantics.

| Функция | Назначение |
| --- | --- |
| `initializeTracing()` | Запускает NodeSDK и OTLP exporter |
| `runInTraceSpan()` | Выполняет операцию внутри span и фиксирует ошибки |
| `captureActiveTraceContext()` | Сериализует W3C context для outbox |
| `extractTraceContext()` | Восстанавливает parent из headers или JSONB |
| `injectTraceContext()` | Добавляет W3C и диагностические headers |
| `getActiveTraceLogFields()` | Возвращает `traceId`/`spanId` для логов |

Настройки:

```env
OTEL_TRACING_ENABLED=true
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
```

Pino `mixin` автоматически добавляет активные `traceId` и `spanId` в
структурированные логи. По `traceId` можно перейти от ошибки к trace в Grafana.
