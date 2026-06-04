# order-service

NestJS service for order creation and order state transitions.

## Publishes

- `order.order-events`

## Consumes

- `risk.risk-events`
- `payment.payment-events`

## Event Flow

1. HTTP/gRPC request creates a `PENDING` order.
2. `order-service` publishes `OrderCreated` to `order.order-events`.
3. `risk-service` publishes `OrderRiskApproved` or `OrderRiskRejected`.
4. `order-service` updates status to `RISK_APPROVED` or `RISK_REJECTED`.
5. `payment-service` publishes `PaymentAuthorized` or `PaymentFailed` after risk approval.
6. `order-service` updates status to `PAYMENT_AUTHORIZED` or `PAYMENT_FAILED`.

Kafka key is `orderId` for order/risk/payment topics.

## Statuses

- `PENDING`
- `RISK_APPROVED`
- `RISK_REJECTED`
- `PAYMENT_AUTHORIZED`
- `PAYMENT_FAILED`

## Configuration

`order-service` needs a producer and consumer Kafka client:

```env
KAFKA_CLIENT_ID=order-service
KAFKA_BROKERS=localhost:9092
KAFKA_CONSUMER_GROUP_ID=order-service
SCHEMA_REGISTRY_URL=http://localhost:8081
```
