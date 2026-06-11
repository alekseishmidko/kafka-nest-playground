import { ValidationPipe } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { NestFactory } from "@nestjs/core";
import { type MicroserviceOptions, Transport } from "@nestjs/microservices";
import { Logger } from "@kafka-playground/observability";
import { logServiceStarted } from "@kafka-playground/observability";
import type { AddressInfo } from "node:net";
import { join } from "node:path";
import { AppModule } from "./app.module";

async function bootstrap() {
  const grpcUrl = process.env.ORDER_GRPC_URL ?? "0.0.0.0:50052";
  const app = await NestFactory.create(AppModule);
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.GRPC,
    options: {
      package: "orders",
      protoPath: join(process.cwd(), "../../packages/contracts/proto/orders.proto"),
      url: grpcUrl
    }
  }, {
    inheritAppConfig: true
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

  await app.startAllMicroservices();

  const adminHost = config.get<string>("ORDER_ADMIN_HOST", "0.0.0.0");
  const adminPort = Number(config.get<string>("ORDER_ADMIN_PORT", "3003"));

  await app.listen(adminPort, adminHost);
  const address = app.getHttpServer().address() as AddressInfo | null;
  const boundPort = address?.port ?? adminPort;
  const publicHost =
    adminHost === "0.0.0.0" ? "localhost" : adminHost;

  logServiceStarted(logger, {
    serviceName: "order-service",
    transport: "grpc",
    environment: config.getOrThrow<string>("APP_ENV"),
    grpcUrl
  });
  logServiceStarted(logger, {
    serviceName: "order-service",
    transport: "http",
    host: adminHost,
    port: boundPort,
    environment: config.getOrThrow<string>("APP_ENV"),
    url: `http://${publicHost}:${boundPort}/admin/dlq`
  });

}

void bootstrap();
