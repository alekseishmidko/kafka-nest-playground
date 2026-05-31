import { ValidationPipe } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { NestFactory } from "@nestjs/core";
import { Transport } from "@nestjs/microservices";
import { Logger } from "nestjs-pino";
import { logServiceStarted } from "@kafka-playground/observability";
import { join } from "node:path";
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

  app.connectMicroservice({
    transport: Transport.GRPC,
    options: {
      package: "orders",
      protoPath: join(process.cwd(), "../../packages/contracts/proto/orders.proto"),
      url: config.getOrThrow<string>("ORDER_GRPC_URL")
    }
  });

  await app.startAllMicroservices();

  const port = Number(config.getOrThrow<string>("PORT"));
  const host = config.getOrThrow<string>("HOST");

  await app.listen(port, host);
  logServiceStarted(logger, {
    serviceName: "order-service",
    host,
    port,
    environment: config.getOrThrow<string>("APP_ENV"),
    grpcUrl: config.getOrThrow<string>("ORDER_GRPC_URL")
  });
}

void bootstrap();
