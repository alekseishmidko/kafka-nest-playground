# payment-service

NestJS service for deterministic mock payment authorization in the order pipeline.

## Responsibility

- Consume `OrderRiskApproved` from `risk.risk-events`.
- Authorize payment with deterministic mock provider logic.
- Publish exactly one payment event to `payment.payment-events`:
  - `PaymentAuthorized`
  - `PaymentFailed`
- Preserve `correlationId` and set `causationId` to the consumed `OrderRiskApproved.eventId`.

Kafka key is `orderId`, so events for one order stay ordered inside a partition.

## Event Flow

1. `risk-service` publishes `OrderRiskApproved`.
2. `payment-service` consumes the approval.
3. `PaymentAuthorizer` calculates a deterministic provider decision from `orderId`, `amount`, `currency` and `riskScore`.
4. `payment-service` publishes `PaymentAuthorized` or `PaymentFailed`.
5. `order-service` consumes payment events and updates order status.

## Consumes

Topic:

- `risk.risk-events`

Supported event:

- `OrderRiskApproved`

Required payload fields:

- `orderId`
- `amount`
- `currency`
- `riskScore`

## Publishes

Topic:

- `payment.payment-events`

`PaymentAuthorized` payload:

```ts
{
  paymentId: string,
  orderId: string,
  amount: number,
  currency: string,
  provider: string
}
```

`PaymentFailed` payload:

```ts
{
  paymentId: string | null,
  orderId: string,
  reason: "payment_provider_declined",
  provider: string
}
```

## Configuration

```env
APP_ENV=local
LOG_LEVEL=debug
PORT=3003
HOST=0.0.0.0

KAFKA_CLIENT_ID=payment-service
KAFKA_BROKERS=localhost:9092
KAFKA_CONSUMER_GROUP_ID=payment-service
SCHEMA_REGISTRY_URL=http://localhost:8081

PAYMENT_PROVIDER=mock-payment-provider
PAYMENT_FAILURE_THRESHOLD=0.18
```

`PAYMENT_FAILURE_THRESHOLD` must be between `0` and `1`; invalid values fall back to `0.18`.

## Local Run

```bash
pnpm infra:up
pnpm contracts:schemas:register
pnpm dev:payment
```

## Checks

```bash
pnpm --filter payment-service lint
pnpm --filter payment-service build
```

## Health Endpoints

```http
GET /healthz
GET /readyz
```
