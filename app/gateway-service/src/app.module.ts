import { Module } from "@nestjs/common";
import { ConfigModule } from "@nestjs/config";
import { getServiceEnvFilePaths, loadServiceEnvFiles } from "@kafka-playground/config";
import { createServiceLoggerModule } from "@kafka-playground/observability";
import { join } from "node:path";
import { GrpcClientsModule } from "./grpc/grpc-clients.module";
import { HealthModule } from "./modules/health/health.module";
import { OrdersModule } from "./modules/orders/orders.module";

loadServiceEnvFiles(join(process.cwd()));

@Module({
  imports: [
    createServiceLoggerModule({
      serviceName: "gateway-service",
      environment: process.env.APP_ENV ?? "local"
    }),
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: getServiceEnvFilePaths(join(process.cwd()))
    }),
    GrpcClientsModule,
    HealthModule,
    OrdersModule
  ]
})
export class AppModule {}
