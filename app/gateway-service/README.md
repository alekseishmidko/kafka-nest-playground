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

## Проверки

```bash
pnpm --filter gateway-service lint
pnpm --filter gateway-service build
```
