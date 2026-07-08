# Kafka Nest Playground

Учебный монорепозиторий с NestJS/Go сервисами, Kafka, Schema Registry и PostgreSQL.

## Быстрый старт

Требования:

- Node.js 22+
- pnpm 9
- Docker Compose
- Go 1.25.12+ для `risk-service-go`

Grafana и `gateway-service` по умолчанию используют порт `3000`. Для одновременного
запуска задайте в `infrastructure/.env`:

```env
GRAFANA_PORT=3001
```

Запуск инфраструктуры и регистрация Avro-схем:

```bash
pnpm install
pnpm infra:up
pnpm contracts:schemas:register
pnpm --filter order-service migration:run
```

Основной order pipeline запускается в отдельных терминалах:

```bash
pnpm dev:gateway
pnpm dev:order
pnpm dev:risk
pnpm dev:payment
pnpm dev:notification
```

Вместо NestJS `risk-service` можно использовать Go-реализацию:

```bash
pnpm dev:risk-service-go
```

## Адреса и порты

| Компонент | Адрес по умолчанию | Назначение |
| --- | --- | --- |
| Gateway API | `http://localhost:3000` | Публичный HTTP API |
| Swagger UI | `http://localhost:3000/docs` | Документация Gateway API |
| Order service | `localhost:50052` | gRPC |
| Order DLQ Admin API | `http://localhost:3003/admin/dlq` | Внутреннее управление DLQ |
| Risk metrics | `http://localhost:3004/metrics` | Kafka и runtime метрики |
| Payment metrics | `http://localhost:3005/metrics` | Kafka и runtime метрики |
| Notification metrics | `http://localhost:3006/metrics` | Kafka и runtime метрики |
| Risk service Go | `http://localhost:3002` | Health/ready endpoints |
| Kafka | `localhost:9092` | Broker |
| Kafka UI | `http://localhost:8080` | Просмотр topics и consumer groups |
| Schema Registry | `http://localhost:8081` | Avro schemas |
| PostgreSQL | `localhost:55432` | Заказы, outbox, DLQ и consumer inbox |
| Grafana | `http://localhost:3001` | При `GRAFANA_PORT=3001` |
| Prometheus | `http://localhost:9090` | Метрики |
| Tempo | `http://localhost:3200` | Хранилище distributed traces |
| Order metrics | `http://localhost:3003/metrics` | Kafka, outbox, DLQ и Node.js metrics |

`payment-service`, `risk-service` и `notification-service` являются Kafka
workers и не открывают сетевые порты.

## Логирование

NestJS-сервисы используют общий пакет `@kafka-playground/observability` и Pino.
В local-режиме логи форматируются через `pino-pretty`, в prod выводятся как JSON.

Уровень задается через:

```env
LOG_LEVEL=debug
```

После успешного старта сервис пишет событие `Service started`. Для HTTP-сервиса
в нем есть `host`, `port`, `url` и `docsUrl`; для gRPC - `grpcUrl`; для фоновых
consumer-сервисов - `transport=worker`.

HTTP-запросы логируются автоматически с method, URL, status code, duration и
request/correlation ID. Kafka-логи содержат topic, partition, offset, event type
и event ID.

## Retry и Dead Letter Queue

Ошибки обработки событий жизненного цикла заказа проходят через общую
retry-цепочку:

```text
order.order-events | risk.risk-events | payment.payment-events
  -> order.order-events.retry-5s
  -> order.order-events.retry-30s
  -> order.order-events.retry-5m
  -> dead-letter.events
```

Consumer автоматически подписывается на retry topics. Текущее число попыток,
исходный topic, время первой ошибки и код последней ошибки передаются в Kafka
headers. Подробности реализации и ручной проверки описаны в
[packages/kafka/README.md](./packages/kafka/README.md).

События из `dead-letter.events` сохраняются в PostgreSQL. Внутренний Admin API
позволяет просмотреть запись, исправить payload и поставить новую копию события
в transactional outbox либо пометить событие как проигнорированное. Подробное
описание находится в [app/order-service/README.md](./app/order-service/README.md).

## Kafka Observability

Prometheus собирает метрики `order-service` с `/metrics`. Grafana автоматически
загружает dashboard **Kafka Order Flow Observability**, содержащий:

- Kafka success/error rate;
- retry rate по этапам;
- p95 времени обработки;
- consumer lag по partition;
- outbox backlog;
- число необработанных DLQ events.

Alert rules находятся в
[`infrastructure/prometheus-rules.yml`](./infrastructure/prometheus-rules.yml):

- появление нового DLQ event;
- DLQ backlog дольше пяти минут;
- consumer lag выше 100;
- outbox backlog выше 25;
- доля ошибок handler-ов выше 10%.

После изменения provisioning перезапустите Prometheus и Grafana:

```bash
pnpm infra:up
```

## Distributed tracing

```text
Node.js services -> OTLP HTTP :4318 -> OpenTelemetry Collector
                 -> OTLP gRPC -> Tempo -> Grafana
```

В Grafana откройте `Explore`, выберите datasource `Tempo` и выполните поиск по
`traceId` из структурированного лога или Kafka header `x-trace-id`.

Трассируется путь HTTP/gRPC, outbox, Kafka producer/consumer, retry, DLQ,
reprocess и PostgreSQL. Контекст outbox хранится в БД, поэтому trace не
обрывается при асинхронной публикации или рестарте order-service.

## Проверки

```bash
pnpm lint
pnpm build
pnpm test
```

Подробная структура монорепозитория описана в [MONOREPO.md](./MONOREPO.md).
