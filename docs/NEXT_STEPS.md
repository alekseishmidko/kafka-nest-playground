# Next Steps Checklist

Рабочий список дальнейшего развития проекта. Отмечайте выполненное через
`[x]`, переносите пункты между приоритетами и добавляйте ссылки на PR/commit,
если задача закрыта.

## Priority 1: Production Safety

- [x] Вынести DLQ-specific auth/RBAC/rate limit в общий `AdminSecurityModule`.
- [x] Применить общий `AdminSecurityModule` ко всем `/admin/*` endpoint-ам.
- [x] Разделить admin permissions на `read`, `write`, `dangerous`.
- [x] Добавить Redis-backed rate limit для нескольких replicas.
- [x] Добавить e2e-проверку, что `401/403/429` пишутся в `admin_audit_events`.
- [x] Добавить admin endpoint для просмотра `admin_audit_events`.
- [ ] Добавить retention policy для `admin_audit_events`.
- [ ] Добавить alert rules для outbox backlog, DLQ backlog и consumer lag.

## Priority 2: Business Flow

- [ ] Добавить inventory-service.
- [ ] Добавить события `InventoryReservationRequested`, `InventoryReserved`, `InventoryRejected`.
- [ ] Добавить компенсацию `InventoryReleased` после отмены или failed payment.
- [ ] Добавить статус заказа `INVENTORY_RESERVED`.
- [ ] Добавить idempotent consumer inbox в inventory-service.
- [ ] Добавить e2e: нет товара -> заказ отменяется.
- [ ] Добавить e2e: inventory reserved -> payment failed -> inventory released.
- [ ] Добавить refund/void flow для отмены `CONFIRMED` заказа.
- [ ] Добавить статус `MANUAL_REVIEW` для risk-service.
- [ ] Добавить admin review queue: approve/reject manual review.

## Priority 3: API Reliability

- [ ] Добавить `Idempotency-Key` для `POST /orders`.
- [ ] Сохранять hash request body для idempotency key.
- [ ] Возвращать тот же response при повторе того же key/body.
- [ ] Возвращать `409 Conflict` при том же key, но другом body.
- [ ] Покрыть concurrent `POST /orders` с одинаковым idempotency key.
- [ ] Добавить `GET /orders/:id`.
- [ ] Добавить `GET /orders/:id/timeline`.
- [ ] Добавить read model/projection для order timeline.

## Priority 4: Contracts And Compatibility

- [ ] Добавить contract compatibility tests для Avro schemas.
- [ ] Проверять backward compatibility в CI.
- [ ] Документировать правила изменения event contracts.
- [ ] Добавить пример безопасного добавления optional поля в Avro schema.
- [ ] Добавить пример несовместимого изменения и ожидаемого CI failure.

## Priority 5: Observability

- [ ] Добавить Grafana dashboards as code.
- [ ] Dashboard: order pipeline overview.
- [ ] Dashboard: Kafka retry/DLQ.
- [ ] Dashboard: outbox/inbox health.
- [ ] Dashboard: Node.js runtime and event loop.
- [ ] Dashboard: Postgres connections/locks/query latency.
- [ ] Добавить trace examples в README: create order -> risk -> payment -> notification.
- [ ] Добавить correlation-id поиск по логам.

## Priority 6: Load And Chaos

- [ ] Зафиксировать первый реальный load baseline через `pnpm test:load:baseline`.
- [ ] Добавить performance regression gate: compare vs saved baseline.
- [ ] Fail gate при росте p95/p99 выше допустимого процента.
- [ ] Fail gate при error rate выше threshold.
- [ ] Fail gate если Kafka lag/outbox backlog не возвращается к baseline.
- [ ] Добавить nightly workflow для chaos/e2e.
- [ ] Добавить HA local profile: две реплики order-service/outbox publisher.
- [ ] Добавить chaos: broker restart во время publish.
- [ ] Добавить chaos: Schema Registry latency/timeouts.

## Priority 7: Admin Tooling

- [ ] Добавить `GET /admin/inbox`.
- [ ] Добавить `GET /admin/inbox/:id`.
- [ ] Добавить safe retry для зависших inbox records.
- [ ] Добавить outbox dry-run replay endpoint.
- [ ] Добавить batch limits и `dangerous` permission для replay.
- [ ] Добавить audit reason/comment для всех write admin actions.
- [ ] Добавить export audit events в JSON/CSV для расследований.

## Priority 8: Developer Experience

- [ ] Проверить `pnpm setup:local` на чистой машине.
- [ ] Проверить `pnpm verify:local` на чистой БД.
- [ ] Добавить `pnpm reset:local` с явным подтверждением.
- [ ] Добавить troubleshooting guide для Kafka/Postgres/Schema Registry.
- [ ] Добавить архитектурную диаграмму основного flow.
- [ ] Добавить diagram: outbox/inbox/retry/DLQ.
- [ ] Добавить examples folder с минимальным consumer и producer.

## Done Recently

- [x] Добавлен transactional outbox.
- [x] Добавлен durable consumer inbox.
- [x] Добавлен retry/DLQ flow.
- [x] Добавлен DLQ admin API.
- [x] Добавлен общий `/admin/*` audit trail.
- [x] Добавлен Outbox Admin API.
- [x] DLQ-specific admin auth/RBAC/rate limit вынесены в общий `AdminSecurityModule`.
- [x] Общий `AdminSecurityModule` применён ко всем текущим `/admin/*` endpoint-ам.
- [x] Admin permissions разделены на `admin:read`, `admin:write`, `admin:dangerous`.
- [x] Добавлен read-only Admin API для просмотра `admin_audit_events`.
- [x] Добавлен e2e-сценарий для audit trail по `401/403/429`.
- [x] Добавлен Redis-backed rate limit backend для нескольких replicas.
- [x] Добавлена пользовательская отмена заказа.
- [x] Добавлена retention policy для технических таблиц.
- [x] Добавлены Dockerfiles, CI, Trivy, dependency audit.
- [x] Добавлен load baseline runner.
- [x] Добавлена документация по `packages/kafka` и `packages/outbox`.

## Notes

- Не удаляйте пункты сразу после выполнения: переносите их в `Done Recently`,
  чтобы было видно историю развития проекта.
- Для задач, зависящих от инфраструктуры, фиксируйте, какой сценарий запускался:
  local, CI, nightly или ручной Docker Compose.
- Для бизнес-логики сначала обновляйте contracts/schema, затем сервисы, затем
  e2e/chaos сценарии.
