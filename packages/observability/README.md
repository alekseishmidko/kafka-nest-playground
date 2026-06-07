# @kafka-playground/observability

Shared package for structured NestJS logging based on Pino.

## Usage

Register the logger module in a service:

```ts
createServiceLoggerModule({
  serviceName: "gateway-service",
  environment: process.env.APP_ENV ?? "local"
})
```

Use the NestJS adapter during bootstrap:

```ts
const logger = app.get(Logger);
app.useLogger(logger);
```

## Runtime format

- `APP_ENV=local`: human-readable output through `pino-pretty`.
- `APP_ENV=prod`: newline-delimited JSON.
- `LOG_LEVEL`: overrides the default log level.

HTTP logs include service, environment, request ID, correlation ID, method,
URL, remote address, status code and response time.

`x-correlation-id` is used as the request ID when present. Otherwise the logger
uses `x-request-id` or generates a UUID.

`logServiceStarted` writes a structured startup event. HTTP services should
provide `host`, `port`, `url` and `docsUrl`; gRPC services should provide
`grpcUrl`; background consumers use `transport: "worker"`.

Tracing and metrics remain planned responsibilities.
