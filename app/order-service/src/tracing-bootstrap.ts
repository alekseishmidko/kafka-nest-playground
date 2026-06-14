import { initializeTracing } from "@kafka-playground/observability";

/** Запускает tracing до загрузки NestJS, gRPC, TypeORM и PostgreSQL driver. */
initializeTracing({
  serviceName: "order-service",
  environment: process.env.APP_ENV,
  enabled: process.env.OTEL_TRACING_ENABLED !== "false"
});
