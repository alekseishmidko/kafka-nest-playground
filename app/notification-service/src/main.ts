import { ConfigService } from "@nestjs/config";
import { NestFactory } from "@nestjs/core";
import { Logger } from "@kafka-playground/observability";
import { logServiceStarted } from "@kafka-playground/observability";
import { AppModule } from "./app.module";

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);
  const config = app.get(ConfigService);
  const logger = app.get(Logger);

  app.useLogger(logger);
  app.enableShutdownHooks();

  logServiceStarted(logger, {
    serviceName: "notification-service",
    transport: "worker",
    environment: config.getOrThrow<string>("APP_ENV")
  });
}

void bootstrap();
