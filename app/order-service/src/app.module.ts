import { Module } from "@nestjs/common";
import { ConfigModule, ConfigService } from "@nestjs/config";
import { TypeOrmModule } from "@nestjs/typeorm";
import { getServiceEnvFilePaths, loadServiceEnvFiles } from "@kafka-playground/config";
import {
  KafkaJsConsumerClient,
  KafkaJsProducerClient,
  KafkaModule
} from "@kafka-playground/kafka";
import { createServiceLoggerModule } from "@kafka-playground/observability";
import { createOrderServiceDataSourceOptions } from "./database/order-service.data-source";
import { OrdersModule } from "./modules/orders/orders.module";
import { join } from "node:path";
import { DlqModule } from "./modules/dlq/dlq.module";

loadServiceEnvFiles(join(process.cwd()));

@Module({
  imports: [
    createServiceLoggerModule({
      serviceName: "order-service",
      environment: process.env.APP_ENV ?? "local"
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
    OrdersModule,
    DlqModule
  ]
})
export class AppModule {}
