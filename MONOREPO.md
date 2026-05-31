# Monorepo Structure

Проект разделен на приложения в `app/*` и общие пакеты в `packages/*`.

## Applications

```text
app/
  gateway-service/
  identity-service/
  catalog-service/
  order-service/
  payment-service/
  notification-service/
  risk-service-go/
  pricing-service-go/
  analytics-service-go/
```

### NestJS services

- `gateway-service`: публичный HTTP/gRPC API, входная точка для клиентов.
- `identity-service`: пользователи, роли, профили, auth events.
- `catalog-service`: товары, цены, остатки, product/inventory events.
- `order-service`: создание заказа, order state machine, saga-like orchestration.
- `payment-service`: авторизация платежей, возвраты, payment events.
- `notification-service`: email/push/webhook уведомления и команды на отправку.

### Go services

- `risk-service-go`: тяжелый risk scoring, antifraud rules, async processing.
- `pricing-service-go`: динамические цены, batch recalculation, discount rules.
- `analytics-service-go`: агрегации, materialized views, consumer lag aware read models.

## Shared Packages

```text
packages/
  contracts/
  config/
  kafka/
  observability/
  testing/
```

- `contracts`: схемы событий, generated types, topic names.
- `config`: общая загрузка env и validation.
- `kafka`: NestJS Kafka client helpers, producer/consumer wrappers.
- `observability`: tracing/logging conventions.
- `testing`: testcontainers, fixtures, contract tests.

## Kafka Topics

Базовый список топиков хранится в `packages/contracts/src/topics.ts`.

```text
identity.user-events
catalog.product-events
catalog.inventory-events
order.order-events
payment.payment-events
risk.risk-events
pricing.price-events
notification.notification-commands
analytics.domain-events
dead-letter.events
```

Правила:

- Topic описывает поток событий или команд.
- `eventType` хранится внутри payload/envelope.
- Kafka message key выбирается по сущности, для которой нужен ordering.
- Для order pipeline использовать key = `orderId`.
- Для inventory использовать key = `productId`.
- Для user events использовать key = `userId`.
- Все failed poison messages отправлять в `dead-letter.events`.
