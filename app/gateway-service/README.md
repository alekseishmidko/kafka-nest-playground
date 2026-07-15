# gateway-service

Публичная HTTP-точка входа в order pipeline.

## Runtime

По умолчанию сервис слушает:

```text
http://localhost:3000
```

Swagger UI:

```text
http://localhost:3000/docs
```

Health check:

```text
GET http://localhost:3000/health
```

При старте лог содержит фактические `host`, `port`, `url` и `docsUrl`.

## Конфигурация

```env
APP_ENV=local
LOG_LEVEL=debug
HOST=0.0.0.0
PORT=3000
ORDER_SERVICE_GRPC_URL=127.0.0.1:50052
```

## Запуск

```bash
pnpm dev:gateway
```

## Idempotency-Key для POST /orders

`POST /orders` поддерживает опциональный HTTP header `Idempotency-Key`.
Gateway считает SHA-256 hash нормализованного JSON body и передает key/hash в
`order-service` через gRPC metadata. `order-service` сохраняет hash и готовый
response в PostgreSQL в той же транзакции, где создаются `orders` и
`outbox_events`.

Повтор с тем же ключом и тем же телом вернет тот же response без создания
второго заказа:

```http
POST /orders
Content-Type: application/json
Idempotency-Key: create-order-2026-07-15-001

{
  "userId": "user-123",
  "currency": "USD",
  "items": [
    {
      "productId": "product-001",
      "quantity": 2,
      "unitPrice": 19.99
    }
  ]
}
```

Повтор того же key с другим body отклоняется, потому что сохраненный request
hash больше не совпадает.

## Проверки

```bash
pnpm --filter gateway-service lint
pnpm --filter gateway-service build
```
