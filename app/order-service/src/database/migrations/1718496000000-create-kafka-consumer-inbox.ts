import type { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Создаёт общий durable inbox для Kafka consumers.
 *
 * Таблица хранит не только факт обработки, но и подготовленный результат.
 * Если процесс завершится между публикацией результата и фиксацией
 * `COMPLETED`, повторная попытка возьмёт тот же JSON и отправит downstream
 * событие с прежним `eventId`.
 */
export class CreateKafkaConsumerInbox1718496000000
  implements MigrationInterface
{
  name = "CreateKafkaConsumerInbox1718496000000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      create table "kafka_consumer_inbox" (
        "consumer_name" varchar(120) not null,
        "event_id" varchar(128) not null,
        "event_type" varchar(120) not null,
        "source_topic" varchar(200) not null,
        "source_partition" integer not null,
        "source_offset" varchar(32) not null,
        "status" varchar(20) not null,
        "result" jsonb,
        "lock_token" uuid,
        "locked_until" timestamptz,
        "attempts" integer not null default 0,
        "last_error" text,
        "created_at" timestamptz not null default now(),
        "updated_at" timestamptz not null default now(),
        "completed_at" timestamptz,
        constraint "PK_kafka_consumer_inbox"
          primary key ("consumer_name", "event_id"),
        constraint "CK_kafka_consumer_inbox_status"
          check ("status" in ('PROCESSING', 'PREPARED', 'COMPLETED'))
      )
    `);

    await queryRunner.query(`
      create index "IDX_kafka_consumer_inbox_recovery"
        on "kafka_consumer_inbox" ("status", "locked_until")
        where "status" <> 'COMPLETED'
    `);
    await queryRunner.query(`
      create index "IDX_kafka_consumer_inbox_completed_at"
        on "kafka_consumer_inbox" ("completed_at")
        where "status" = 'COMPLETED'
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `drop index "IDX_kafka_consumer_inbox_completed_at"`
    );
    await queryRunner.query(
      `drop index "IDX_kafka_consumer_inbox_recovery"`
    );
    await queryRunner.query(`drop table "kafka_consumer_inbox"`);
  }
}
