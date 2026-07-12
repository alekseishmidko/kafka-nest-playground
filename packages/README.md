# packages

Shared packages для Kafka marketplace playground.

Эта папка содержит код, который должен переиспользоваться несколькими
приложениями из `app/*` или задаёт платформенный контракт проекта.

## Пакеты

| Пакет | Назначение |
| --- | --- |
| `@kafka-playground/config` | Загрузка `.env` файлов сервисов единым способом. |
| `@kafka-playground/contracts` | Topic names, TypeScript event contracts, Avro schemas и gRPC proto. |
| `@kafka-playground/kafka` | Kafka producer/consumer toolkit, retry/DLQ, durable inbox, lag monitor. |
| `@kafka-playground/observability` | Logger, Prometheus metrics, OpenTelemetry tracing helpers. |
| `@kafka-playground/outbox` | Transactional outbox для producer-side durability. |
| `@kafka-playground/testing` | E2E, chaos и load testing scripts. |

## Когда добавлять код в `packages/*`

Добавляйте shared package code только если выполняется хотя бы одно правило:

- поведение нужно двум или более приложениям;
- это часть платформенной инфраструктуры Kafka/outbox/observability;
- это контракт между сервисами;
- это тестовый harness, который должен переиспользоваться сценариями.

Не переносите в `packages/*` доменную логику одного сервиса. Например, state
machine заказа живёт в `app/order-service`, а не в shared пакете.

## Как читать пакеты

Начинайте с README конкретного пакета:

- [`kafka/README.md`](./kafka/README.md)
- [`outbox/README.md`](./outbox/README.md)
- [`contracts/README.md`](./contracts/README.md)
- [`observability/README.md`](./observability/README.md)
- [`testing/README.md`](./testing/README.md)

Для Kafka и outbox также есть package-local ADR:

- [`kafka/docs/adr`](./kafka/docs/adr)
- [`outbox/docs/adr`](./outbox/docs/adr)

## Проверки

```bash
pnpm --filter @kafka-playground/contracts build
pnpm --filter @kafka-playground/kafka test
pnpm --filter @kafka-playground/outbox test
pnpm --filter @kafka-playground/observability test
```

Если меняете contracts, дополнительно регистрируйте Avro schemas на поднятой
инфраструктуре:

```bash
pnpm contracts:schemas:register
```
