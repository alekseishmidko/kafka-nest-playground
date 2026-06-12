import { DataSource, type DataSourceOptions } from "typeorm";
import { getServiceEnvFilePaths, loadServiceEnvFiles } from "@kafka-playground/config";
import { join } from "node:path";
import { OrderEntity } from "../modules/orders/entities/order.entity";
import { OutboxEventEntity } from "../modules/orders/entities/outbox-event.entity";
import { ProcessedKafkaEventEntity } from "../modules/orders/entities/processed-kafka-event.entity";
import { CreateOrderServiceSchema1717977600000 } from "./migrations/1717977600000-create-order-service-schema";
import { AddFinalOrderStatuses1718064000000 } from "./migrations/1718064000000-add-final-order-statuses";
import { CreateDeadLetterEvents1718150400000 } from "./migrations/1718150400000-create-dead-letter-events";
import { DeadLetterEventEntity } from "../modules/dlq/entities/dead-letter-event.entity";

loadServiceEnvFiles(join(process.cwd()));

/**
 * Собирает конфигурацию TypeORM для runtime и CLI-команд миграций.
 *
 * Единая фабрика исключает ситуацию, когда NestJS стартует с одним набором
 * entities/migrations, а `typeorm migration:run` использует другой.
 */
export function createOrderServiceDataSourceOptions(): DataSourceOptions {
  return {
    type: "postgres",
    host: readRequiredEnv("POSTGRES_HOST"),
    port: Number(readRequiredEnv("POSTGRES_PORT")),
    username: readRequiredEnv("POSTGRES_USER"),
    password: readRequiredEnv("POSTGRES_PASSWORD"),
    database: readRequiredEnv("POSTGRES_DB"),
    ssl:
      readRequiredEnv("POSTGRES_SSL") === "true"
        ? { rejectUnauthorized: false }
        : false,
    synchronize: false,
    migrationsRun: readBooleanEnv("TYPEORM_MIGRATIONS_RUN", true),
    entities: [
      OrderEntity,
      OutboxEventEntity,
      ProcessedKafkaEventEntity,
      DeadLetterEventEntity
    ],
    migrations: [
      CreateOrderServiceSchema1717977600000,
      AddFinalOrderStatuses1718064000000,
      CreateDeadLetterEvents1718150400000
    ]
  };
}

/**
 * DataSource используется TypeORM CLI для `migration:run` и `migration:revert`.
 */
export default new DataSource(createOrderServiceDataSourceOptions());

function readRequiredEnv(name: string): string {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Required environment variable ${name} is not configured`);
  }

  return value;
}

function readBooleanEnv(name: string, fallback: boolean): boolean {
  const value = process.env[name];

  if (value === undefined || value === "") {
    return fallback;
  }

  return value === "true";
}
