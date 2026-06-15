import { Module } from "@nestjs/common";
import { ConfigModule, ConfigService } from "@nestjs/config";
import { getServiceEnvFilePaths, loadServiceEnvFiles } from "@kafka-playground/config";
import {
  KafkaJsConsumerClient,
  KafkaJsProducerClient,
  KafkaModule,
  PostgresKafkaInboxStore
} from "@kafka-playground/kafka";
import {
  createServiceLoggerModule,
  MetricsModule
} from "@kafka-playground/observability";
import { join } from "node:path";
import { PaymentModule } from "./modules/payment/payment.module";

loadServiceEnvFiles(join(process.cwd()));

@Module({
  imports: [
    createServiceLoggerModule({
      serviceName: "payment-service",
      environment: process.env.APP_ENV ?? "local"
    }),
    MetricsModule.register({
      serviceName: "payment-service"
    }),
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: getServiceEnvFilePaths(join(process.cwd()))
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
          serviceName: "payment-service",
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
          }),
          inboxStore: new PostgresKafkaInboxStore({
            host: configService.getOrThrow<string>("POSTGRES_HOST"),
            port: Number(configService.getOrThrow<string>("POSTGRES_PORT")),
            user: configService.getOrThrow<string>("POSTGRES_USER"),
            password: configService.getOrThrow<string>("POSTGRES_PASSWORD"),
            database: configService.getOrThrow<string>("POSTGRES_DB"),
            ssl:
              configService.get<string>("POSTGRES_SSL", "false") === "true"
                ? { rejectUnauthorized: false }
                : false
          })
        };
      }
    }),
    PaymentModule
  ]
})
export class AppModule {}
