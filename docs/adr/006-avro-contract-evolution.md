# ADR-006: Avro Contract Evolution

## Status

Accepted.

## Context

Сервисы в проекте обмениваются Kafka events. Если producer и consumer по-разному
понимают payload, ошибка проявится асинхронно: consumer упадет позже, событие
уйдет в retry/DLQ, а источник проблемы будет неочевиден.

Проект использует Schema Registry и Avro, поэтому contract changes должны быть
явными и совместимыми.

## Decision

Message contracts являются schema-first.

При изменении event contract нужно обновлять вместе:

- Avro schema в `packages/contracts/schemas`;
- TypeScript types в `packages/contracts/src/events.ts`;
- topic/subject mapping в `packages/contracts/src/topics.ts` и event helpers;
- schema registration script;
- producers, consumers и e2e tests, которые зависят от payload.

Kafka envelope остается стабильным: `eventId`, `eventType`, `eventVersion`,
`occurredAt`, `correlationId`, `causationId`, `producer`, `payload`.

Tracing context не добавляется в Avro payload. Он остается в Kafka headers или
`outbox_events.trace_context`.

## Consequences

Плюсы:

- producer/consumer compatibility проверяется до runtime;
- Schema Registry становится источником истины для wire format;
- проще поддерживать Go и TypeScript реализации одновременно;
- DLQ и reprocess могут опираться на стабильный envelope.

Минусы:

- любое изменение contract требует синхронного обновления нескольких файлов;
- breaking changes требуют новой версии события или миграционного периода;
- локальный dev flow должен регистрировать схемы перед e2e.

## Operational Rules

- Не добавлять поля в payload только в TypeScript без Avro schema.
- Новые optional поля должны иметь Avro default.
- Breaking change требует нового event version или нового event type.
- После contract change запускать:

```bash
pnpm --filter @kafka-playground/contracts build
pnpm contracts:schemas:register
```

- Для Go consumers/producers проверять соответствие структур Avro payload.

