# ADR 001: Outbox package API

## Status

Accepted.

## Context

Сервисам нужно атомарно сохранять бизнес-изменение в PostgreSQL и намерение
опубликовать Kafka event. При этом shared пакет не должен знать доменную модель
заказа, конкретные topic constants или детали KafkaJS.

## Decision

Пакет разделён на три слоя:

- `OutboxEventEntity` и `PostgresOutboxStore` отвечают за PostgreSQL storage;
- `TransactionalMessageStore` и `MessagePublisher` задают transport-agnostic
  контракты;
- `OutboxPublisherService` связывает store и publisher через NestJS DI.

Приложение создаёт outbox-запись через `createOutboxEventEntity` внутри своей
business transaction. Publisher публикует уже сохранённый event envelope без
пересборки доменной логики.

## Consequences

Плюсы:

- outbox можно использовать в любом сервисе с TypeORM/PostgreSQL;
- Kafka-specific код не попадает в store;
- batch publish тестируется без NestJS через `publishOutboxBatch`;
- дубли после crash window остаются ожидаемой частью at-least-once модели.

Минусы:

- downstream consumer обязан быть идемпотентным;
- package не решает exactly-once между PostgreSQL и Kafka;
- каждое приложение должно явно добавить миграции и entity.
