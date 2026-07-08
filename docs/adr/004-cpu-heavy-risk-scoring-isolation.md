# ADR-004: CPU Heavy Risk Scoring Isolation

## Status

Accepted.

## Context

Risk scoring может быть CPU-heavy: hash loops, fraud rules, graph lookup или
ML inference. Если такую работу выполнять в основном NestJS event loop, один
тяжелый scoring batch может задержать Kafka heartbeats, HTTP metrics endpoint,
timers, retries и graceful shutdown.

Это особенно опасно для Kafka consumers: заблокированный event loop повышает
риск rebalance и повторной доставки сообщений.

## Decision

CPU-heavy risk scoring должен быть изолирован от основного NestJS event loop.

В проекте поддерживаются два допустимых варианта:

- отдельный Go worker `risk-service-go`;
- Node.js worker threads, если scoring остается внутри TypeScript runtime.

NestJS `risk-service` допустим для учебного сценария и легкой нагрузки, но не
должен становиться местом тяжелого production scoring без изоляции.

## Consequences

Плюсы:

- Kafka consumer loop остается отзывчивым;
- проще масштабировать risk independently от gateway/order/payment;
- Go worker дает предсказуемую CPU-bound производительность;
- worker threads позволяют сохранить TypeScript код, но не блокировать event
  loop.

Минусы:

- появляется дополнительная operational единица;
- нужно синхронизировать Avro contracts между реализациями;
- трассировка и метрики должны быть одинаковыми для NestJS и Go варианта;
- локальный dev setup становится сложнее.

## Operational Rules

- При росте `RISK_SCORE_ITERATIONS` сначала проверять event loop lag и consumer
  lag.
- Для production предпочитать `risk-service-go` или worker-thread pool.
- Любая новая risk implementation обязана читать/писать те же Avro contracts.
- Нельзя менять semantics risk events ради одной реализации.
- Load tests должны отдельно отслеживать latency HTTP create order и Kafka lag
  downstream risk processing.

