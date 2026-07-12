# ADR 001: Single consumer loop per process

## Status

Accepted.

## Context

NestJS feature modules хотят независимо регистрировать Kafka handlers. KafkaJS
consumer при этом должен запускать один `consumer.run()`. Если каждый module
самостоятельно запускает loop, приложение получает гонки подписок и сложный
shutdown/rebalance behavior.

## Decision

Все feature modules регистрируют handlers в `KafkaConsumerRunner` до bootstrap.
После bootstrap runner:

1. вычисляет main + retry topics;
2. вызывает `consumer.subscribe`;
3. запускает единственный `consumer.run()`;
4. маршрутизирует сообщения к handler-ам по topic или `x-original-topic`.

## Consequences

Плюсы:

- один consumer lifecycle на процесс;
- retry topics подключаются централизованно;
- feature modules не знают про KafkaJS loop;
- проще собирать lag metrics по фактическим subscriptions.

Минусы:

- topic может иметь только один handler в процессе;
- подписки нельзя добавлять после bootstrap;
- dynamic subscriptions потребуют отдельного lifecycle API.
