import type { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Добавляет финальный outbox-статус для ручного административного ignore.
 */
export class AddOutboxIgnoredStatus1718841600000
  implements MigrationInterface
{
  name = "AddOutboxIgnoredStatus1718841600000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      alter type "outbox_events_status_enum"
      add value if not exists 'IGNORED'
    `);
  }

  async down(): Promise<void> {
    /**
     * PostgreSQL не умеет безопасно удалять значение enum без пересоздания типа.
     * Down migration намеренно no-op: откат кода должен сначала убедиться, что
     * в таблице нет `IGNORED`, и только потом выполнять ручную enum-миграцию.
     */
  }
}
