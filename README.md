# Kafka Nest Playground

Учебный монорепозиторий с NestJS/Go сервисами, Kafka, Schema Registry и PostgreSQL.

## Быстрый старт

Требования:

- Node.js 22+
- pnpm 9
- Docker Compose
- Go 1.24+ для `risk-service-go`

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
| Risk service Go | `http://localhost:3002` | Health/ready endpoints |
| Kafka | `localhost:9092` | Broker |
| Kafka UI | `http://localhost:8080` | Просмотр topics и consumer groups |
| Schema Registry | `http://localhost:8081` | Avro schemas |
| PostgreSQL | `localhost:5432` | Хранилище заказов |
| Grafana | `http://localhost:3001` | При `GRAFANA_PORT=3001` |
| Prometheus | `http://localhost:9090` | Метрики |

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

## Проверки

```bash
pnpm lint
pnpm build
pnpm test
```

Подробная структура монорепозитория описана в [MONOREPO.md](./MONOREPO.md).
