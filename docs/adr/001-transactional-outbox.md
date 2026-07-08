# ADR-001: Transactional Outbox

## Status

Accepted.

## Context

`order-service` должен одновременно сохранить бизнес-состояние заказа в
PostgreSQL и опубликовать Kafka event. Если сначала записать заказ, а потом
публиковать напрямую в Kafka, процесс может упасть между этими действиями. Тогда
заказ останется в БД, но downstream-сервисы никогда не узнают о событии.

Обратный порядок тоже небезопасен: если сначала опубликовать Kafka event, а
потом не закоммитить транзакцию БД, consumers увидят событие о факте, которого
нет в базе.

## Decision

Для бизнес-событий, связанных с изменением PostgreSQL state, используется
transactional outbox.

`order-service` сохраняет заказ и строку `outbox_events` в одной DB-транзакции.
Отдельный outbox publisher читает `PENDING`/retryable `FAILED` строки и
публикует их в Kafka. После успешной публикации строка переводится в
`PUBLISHED`.

## Consequences

Плюсы:

- бизнес-факт и намерение опубликовать event фиксируются атомарно;
- Kafka outage не приводит к потере событий;
- outbox backlog можно наблюдать через метрики и БД;
- reprocess DLQ может безопасно поставить исправленное событие обратно в outbox.

Минусы:

- появляется eventual consistency между БД и Kafka;
- возможен duplicate publish, если процесс упал после Kafka publish, но до
  `markPublished`;
- consumers обязаны быть идемпотентными;
- нужна retention policy для опубликованных технических записей.

## Operational Rules

- Не публиковать доменные Kafka events напрямую после DB write, если нужна
  transactional safety.
- Новые события заказа должны создаваться через outbox.
- `PENDING` и `FAILED` outbox rows нельзя удалять retention job-ом.
- Outbox publisher должен использовать lease, чтобы несколько реплик не
  публиковали одну строку одновременно.
- E2E gate должен запускаться на чистом pending outbox или явно подготовленной
  тестовой БД.

