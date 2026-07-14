import { Module } from "@nestjs/common";
import { ConfigModule, ConfigService } from "@nestjs/config";
import { TypeOrmModule } from "@nestjs/typeorm";
import { getServiceEnvFilePaths, loadServiceEnvFiles } from "@kafka-playground/config";
import {
  KafkaJsConsumerClient,
  KafkaJsProducerClient,
  KafkaModule
} from "@kafka-playground/kafka";
import {
  createServiceLoggerModule,
  MetricsModule
} from "@kafka-playground/observability";
import { createOrderServiceDataSourceOptions } from "./database/order-service.data-source";
import { OrdersModule } from "./modules/orders/orders.module";
import { join } from "node:path";
import { DlqModule } from "./modules/dlq/dlq.module";
import { OperationalMetricsModule } from "./modules/operational-metrics/operational-metrics.module";
import { TechnicalRetentionModule } from "./modules/retention/technical-retention.module";
import { AdminAuditModule } from "./modules/admin-audit/admin-audit.module";
import { OutboxAdminModule } from "./modules/outbox-admin/outbox-admin.module";
import { AdminSecurityModule } from "./modules/admin-security/admin-security.module";

loadServiceEnvFiles(join(process.cwd()));

@Module({
  imports: [
    createServiceLoggerModule({
      serviceName: "order-service",
      environment: process.env.APP_ENV ?? "local"
    }),
    MetricsModule.register({
      serviceName: "order-service"
    }),
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: getServiceEnvFilePaths(join(process.cwd()))
    }),
    TypeOrmModule.forRootAsync({
      useFactory: () => createOrderServiceDataSourceOptions()
    }),
    KafkaModule.registerAsync({
      inject: [ConfigService],
      useFactory: (config: unknown) => {
        const configService = config as ConfigService;
        const clientId = configService.getOrThrow<string>("KAFKA_CLIENT_ID");
        const brokers = configService.getOrThrow<string>("KAFKA_BROKERS").split(",");
        const groupId = configService.getOrThrow<string>("KAFKA_CONSUMER_GROUP_ID");

        return {
          clientId,
          serviceName: "order-service",
          brokers,
          consumerGroupId: groupId,
          schemaRegistryUrl: configService.getOrThrow<string>("SCHEMA_REGISTRY_URL"),
          producerClient: new KafkaJsProducerClient({
            clientId,
            brokers
          }),
          consumerClient: new KafkaJsConsumerClient({
            clientId,
            brokers,
            groupId
          })
        };
      }
    }),
    AdminSecurityModule,
    AdminAuditModule,
    OrdersModule,
    DlqModule,
    OutboxAdminModule,
    TechnicalRetentionModule,
    OperationalMetricsModule
  ]
})
export class AppModule {}
