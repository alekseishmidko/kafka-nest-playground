# order-service

NestJS service for order creation, order state transitions and saga-like orchestration.

Publishes:

- `order.order-events`

Consumes:

- `risk.risk-events`
- `payment.payment-events`
- `catalog.inventory-events`
