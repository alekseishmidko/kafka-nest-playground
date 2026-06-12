import type { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Добавляет частичный индекс для пакетной очистки завершённых DLQ-записей.
 *
 * Строки `NEW` намеренно исключены: они не участвуют в retention и не должны
 * увеличивать стоимость обновления индекса при поступлении новых ошибок.
 */
export class AddDlqRetentionIndex1718323200000
  implements MigrationInterface
{
  name = "AddDlqRetentionIndex1718323200000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      create index if not exists "IDX_dead_letter_events_retention"
      on "dead_letter_events" ("status", "updated_at")
      where "status" in ('REPROCESSED', 'IGNORED')
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `drop index if exists "IDX_dead_letter_events_retention"`
    );
  }
}
