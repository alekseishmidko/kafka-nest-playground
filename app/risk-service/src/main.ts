import "./tracing-bootstrap";
import { ConfigService } from "@nestjs/config";
import { NestFactory } from "@nestjs/core";
import { Logger } from "@kafka-playground/observability";
import { logServiceStarted } from "@kafka-playground/observability";
import { AppModule } from "./app.module";

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const config = app.get(ConfigService);
  const logger = app.get(Logger);

  app.useLogger(logger);
  app.enableShutdownHooks();
  const host = config.get<string>("METRICS_HOST", "0.0.0.0");
  const port = Number(config.get<string>("METRICS_PORT", "3004"));

  await app.listen(port, host);

  logServiceStarted(logger, {
    serviceName: "risk-service",
    transport: "worker",
    environment: config.getOrThrow<string>("APP_ENV"),
    url: `http://${host === "0.0.0.0" ? "localhost" : host}:${port}/metrics`
  });
}

void bootstrap();
