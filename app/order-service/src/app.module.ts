import { Module } from "@nestjs/common";
import { ConfigModule, ConfigService } from "@nestjs/config";
import { TypeOrmModule } from "@nestjs/typeorm";
import { getServiceEnvFilePaths, loadServiceEnvFiles } from "@kafka-playground/config";
import { KafkaJsProducerClient, KafkaModule } from "@kafka-playground/kafka";
import { createServiceLoggerModule } from "@kafka-playground/observability";
import { OrdersModule } from "./modules/orders/orders.module";
import { join } from "node:path";

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
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        type: "postgres",
        host: config.getOrThrow<string>("POSTGRES_HOST"),
        port: Number(config.getOrThrow<string>("POSTGRES_PORT")),
        username: config.getOrThrow<string>("POSTGRES_USER"),
        password: config.getOrThrow<string>("POSTGRES_PASSWORD"),
        database: config.getOrThrow<string>("POSTGRES_DB"),
        autoLoadEntities: true,
        synchronize: config.getOrThrow<string>("TYPEORM_SYNCHRONIZE") === "true",
        ssl:
          config.getOrThrow<string>("POSTGRES_SSL") === "true"
            ? { rejectUnauthorized: false }
            : false
      })
    }),
    KafkaModule.registerAsync({
      inject: [ConfigService],
      useFactory: (config: unknown) => {
        const configService = config as ConfigService;
        const clientId = configService.getOrThrow<string>("KAFKA_CLIENT_ID");
        const brokers = configService.getOrThrow<string>("KAFKA_BROKERS").split(",");

        return {
          clientId,
          serviceName: "order-service",
          brokers,
          schemaRegistryUrl: configService.getOrThrow<string>("SCHEMA_REGISTRY_URL"),
          producerClient: new KafkaJsProducerClient({
            clientId,
            brokers
          })
        };
      }
    }),
    OrdersModule
  ]
})
export class AppModule {}
