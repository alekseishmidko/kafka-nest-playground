# analytics-service-go

Go service for event-driven analytics, aggregations and materialized read models.

Publishes:

- `analytics.domain-events`

Consumes:

- `identity.user-events`
- `catalog.product-events`
- `catalog.inventory-events`
- `order.order-events`
- `payment.payment-events`
- `risk.risk-events`
- `pricing.price-events`
