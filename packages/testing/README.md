# @kafka-playground/testing

Shared package for integration tests, fixtures and contract tests.

Planned responsibilities:

- Testcontainers setup.
- Kafka topic fixtures.
- event fixture factories.
- schema compatibility tests.
- end-to-end flow helpers.

## Order Pipeline E2E

Smoke-test for the full `order -> risk -> payment -> order` flow:

```bash
pnpm test:e2e:order-pipeline
```

Prerequisites:

- infrastructure is running: `pnpm infra:up`
- Avro schemas are registered: `pnpm contracts:schemas:register`
- `gateway-service`, `order-service`, `risk-service` or `risk-service-go`, and `payment-service` are running
- Postgres connection matches `E2E_POSTGRES_*` env vars or the local defaults from `infrastructure/.env`

Useful overrides:

```bash
E2E_GATEWAY_URL=http://localhost:3000 \
E2E_POSTGRES_HOST=localhost \
E2E_POSTGRES_PORT=5432 \
E2E_POSTGRES_USER=postgres \
E2E_POSTGRES_PASSWORD=postgres \
E2E_POSTGRES_DB=kafka_playground \
pnpm test:e2e:order-pipeline
```
