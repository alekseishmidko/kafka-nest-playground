import { initializeTracing } from "@kafka-playground/observability";

/**
 * Инициализация выполняется отдельным первым import-ом до загрузки NestJS,
 * HTTP и gRPC модулей, чтобы OpenTelemetry успел установить instrumentation.
 */
initializeTracing({
  serviceName: "gateway-service",
  environment: process.env.APP_ENV,
  enabled: process.env.OTEL_TRACING_ENABLED !== "false"
});
