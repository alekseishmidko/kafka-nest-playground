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

## Runtime

`order-service` does not expose HTTP. It runs as a gRPC server and consumes Kafka events.

## Configuration

`order-service` needs a gRPC bind URL plus producer and consumer Kafka clients:

```env
ORDER_GRPC_URL=0.0.0.0:50052
KAFKA_CLIENT_ID=order-service
KAFKA_BROKERS=localhost:9092
KAFKA_CONSUMER_GROUP_ID=order-service
SCHEMA_REGISTRY_URL=http://localhost:8081
```
