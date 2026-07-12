# ADR 003: Configurable retry policy outside NestJS

## Status

Accepted.

## Context

Retry/DLQ routing нужен разным bounded contexts. Если policy будет зашита в
NestJS provider и topic constants order-flow, пакет нельзя будет нормально
переиспользовать.

## Decision

Маршрутизация вынесена в `ConfigurableKafkaRetryPolicy`. Класс принимает:

- source topics;
- ordered retry stages;
- dead letter topic.

NestJS provider `KafkaRetryPolicy` является только preset-ом для текущего
playground order-flow.

## Consequences

Плюсы:

- retry policy можно тестировать как чистую функцию;
- новый bounded context задаёт свои topics без fork-а пакета;
- dispatcher и runner используют один контракт.

Минусы:

- приложение должно явно описать retry route;
- общие retry topics требуют корректного `x-original-topic`;
- raw Avro deserialize failures пока не попадают в этот retry flow.
