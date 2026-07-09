import { DataSource, type DataSourceOptions } from "typeorm";
import { getServiceEnvFilePaths, loadServiceEnvFiles } from "@kafka-playground/config";
import { OutboxEventEntity } from "@kafka-playground/outbox";
import { join } from "node:path";
import { OrderEntity } from "../modules/orders/entities/order.entity";
import { ProcessedKafkaEventEntity } from "../modules/orders/entities/processed-kafka-event.entity";
import { CreateOrderServiceSchema1717977600000 } from "./migrations/1717977600000-create-order-service-schema";
import { AddFinalOrderStatuses1718064000000 } from "./migrations/1718064000000-add-final-order-statuses";
import { CreateDeadLetterEvents1718150400000 } from "./migrations/1718150400000-create-dead-letter-events";
import { DeadLetterEventEntity } from "../modules/dlq/entities/dead-letter-event.entity";
import { DlqAuditLogEntity } from "../modules/dlq/entities/dlq-audit-log.entity";
import { AdminAuditEventEntity } from "../modules/admin-audit/entities/admin-audit-event.entity";
import { AddDlqSecurityAudit1718236800000 } from "./migrations/1718236800000-add-dlq-security-audit";
import { AddDlqRetentionIndex1718323200000 } from "./migrations/1718323200000-add-dlq-retention-index";
import { AddOutboxTraceContext1718409600000 } from "./migrations/1718409600000-add-outbox-trace-context";
import { CreateKafkaConsumerInbox1718496000000 } from "./migrations/1718496000000-create-kafka-consumer-inbox";
import { AddOutboxLeaseFields1718582400000 } from "./migrations/1718582400000-add-outbox-lease-fields";
import { AddTechnicalRetentionIndexes1718668800000 } from "./migrations/1718668800000-add-technical-retention-indexes";
import { CreateAdminAuditEvents1718755200000 } from "./migrations/1718755200000-create-admin-audit-events";
import { AddOutboxIgnoredStatus1718841600000 } from "./migrations/1718841600000-add-outbox-ignored-status";

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
      DeadLetterEventEntity,
      DlqAuditLogEntity,
      AdminAuditEventEntity
    ],
    migrations: [
      CreateOrderServiceSchema1717977600000,
      AddFinalOrderStatuses1718064000000,
      CreateDeadLetterEvents1718150400000,
      AddDlqSecurityAudit1718236800000,
      AddDlqRetentionIndex1718323200000,
      AddOutboxTraceContext1718409600000,
      CreateKafkaConsumerInbox1718496000000,
      AddOutboxLeaseFields1718582400000,
      AddTechnicalRetentionIndexes1718668800000,
      CreateAdminAuditEvents1718755200000,
      AddOutboxIgnoredStatus1718841600000
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
