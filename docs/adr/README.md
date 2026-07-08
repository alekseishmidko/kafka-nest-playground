# Architecture Decision Records

ADR фиксируют архитектурные решения, которые должны оставаться понятными после
добавления новых сервисов, topics и бизнес-фич.

Формат каждого документа:

- `Status`: текущее состояние решения.
- `Context`: почему решение понадобилось.
- `Decision`: что именно выбрано.
- `Consequences`: какие trade-off приняты.
- `Operational Rules`: практические правила для разработки.

## Список ADR

- [ADR-001 Transactional Outbox](./001-transactional-outbox.md)
- [ADR-002 Durable Consumer Inbox](./002-durable-consumer-inbox.md)
- [ADR-003 Kafka Key Is Order Id](./003-kafka-key-order-id.md)
- [ADR-004 CPU Heavy Risk Scoring Isolation](./004-cpu-heavy-risk-scoring-isolation.md)
- [ADR-005 Retry And DLQ](./005-retry-and-dlq.md)
- [ADR-006 Avro Contract Evolution](./006-avro-contract-evolution.md)

