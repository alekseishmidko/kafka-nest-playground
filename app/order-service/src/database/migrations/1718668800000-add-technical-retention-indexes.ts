import type { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Добавляет индексы для пакетной очистки старых технических записей.
 *
 * Retention job удаляет только завершённые записи. Частичные индексы держат
 * рабочие выборки маленькими и не раздувают индекс строками, которые нельзя
 * безопасно удалять: pending/failed outbox и незавершённый inbox.
 */
export class AddTechnicalRetentionIndexes1718668800000
  implements MigrationInterface
{
  name = "AddTechnicalRetentionIndexes1718668800000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      create index if not exists "IDX_outbox_events_published_retention"
      on "outbox_events" ("published_at", "id")
      where "status" = 'PUBLISHED'
    `);
    await queryRunner.query(`
      create index if not exists "IDX_processed_kafka_events_retention"
      on "processed_kafka_events" ("processed_at", "id")
    `);
    await queryRunner.query(`
      create index if not exists "IDX_kafka_consumer_inbox_retention"
      on "kafka_consumer_inbox" ("completed_at", "consumer_name", "event_id")
      where "status" = 'COMPLETED'
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `drop index if exists "IDX_kafka_consumer_inbox_retention"`
    );
    await queryRunner.query(
      `drop index if exists "IDX_processed_kafka_events_retention"`
    );
    await queryRunner.query(
      `drop index if exists "IDX_outbox_events_published_retention"`
    );
  }
}
