import { ValidationPipe } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { NestFactory } from "@nestjs/core";
import { Logger } from "@kafka-playground/observability";
import { logServiceStarted } from "@kafka-playground/observability";
import type { AddressInfo } from "node:net";
import { AppModule } from "./app.module";
import { setupSwagger } from "./config/swagger.config";

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
  setupSwagger(app);

  const port = Number(config.getOrThrow<string>("PORT"));
  const host = config.getOrThrow<string>("HOST");

  await app.listen(port, host);
  const address = app.getHttpServer().address() as AddressInfo | null;
  const boundPort = address?.port ?? port;
  const publicHost = host === "0.0.0.0" ? "localhost" : host;
  const url = `http://${publicHost}:${boundPort}`;

  logServiceStarted(logger, {
    serviceName: "gateway-service",
    transport: "http",
    host,
    port: boundPort,
    environment: config.getOrThrow<string>("APP_ENV"),
    url,
    docsUrl: `${url}/docs`
  });
}

void bootstrap();
