import { ValidationPipe } from "@nestjs/common";
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
  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true
    })
  );

  const port = Number(config.getOrThrow<string>("PORT"));
  const host = config.getOrThrow<string>("HOST");

  await app.listen(port, host);
  logServiceStarted(logger, {
    serviceName: "gateway-service",
    host,
    port,
    environment: config.getOrThrow<string>("APP_ENV")
  });
}

void bootstrap();
