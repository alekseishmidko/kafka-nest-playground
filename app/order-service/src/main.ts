import { ValidationPipe } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { NestFactory } from "@nestjs/core";
import { type MicroserviceOptions, Transport } from "@nestjs/microservices";
import { Logger } from "@kafka-playground/observability";
import { logServiceStarted } from "@kafka-playground/observability";
import { join } from "node:path";
import { AppModule } from "./app.module";

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.GRPC,
    options: {
      package: "orders",
      protoPath: join(process.cwd(), "../../packages/contracts/proto/orders.proto"),
      url: process.env.ORDER_GRPC_URL ?? "0.0.0.0:50052"
    }
  });
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

  await app.listen();
  logServiceStarted(logger, {
    serviceName: "order-service",
    environment: config.getOrThrow<string>("APP_ENV"),
    grpcUrl: config.getOrThrow<string>("ORDER_GRPC_URL")
  });
}

void bootstrap();
