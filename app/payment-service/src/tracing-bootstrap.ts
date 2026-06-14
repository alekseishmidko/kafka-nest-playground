import { initializeTracing } from "@kafka-playground/observability";

/** Запускает tracing до загрузки Kafka worker dependencies. */
initializeTracing({
  serviceName: "payment-service",
  environment: process.env.APP_ENV,
  enabled: process.env.OTEL_TRACING_ENABLED !== "false"
});
