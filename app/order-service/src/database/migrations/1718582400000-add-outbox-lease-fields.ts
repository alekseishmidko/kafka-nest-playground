import type { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Добавляет lease-поля для будущего multi-instance outbox publisher-а.
 *
 * Сейчас publisher ещё читает publishable-записи старым способом, но схема уже
 * хранит владельца lease и момент истечения. Следующий шаг сможет атомарно
 * забирать batch через `FOR UPDATE SKIP LOCKED` без отдельной миграции.
 */
export class AddOutboxLeaseFields1718582400000
  implements MigrationInterface
{
  name = "AddOutboxLeaseFields1718582400000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      alter table "outbox_events"
      add column if not exists "locked_by" varchar(160)
    `);
    await queryRunner.query(`
      alter table "outbox_events"
      add column if not exists "locked_until" timestamptz
    `);
    await queryRunner.query(`
      create index if not exists "IDX_outbox_events_lease"
      on "outbox_events" ("locked_until", "status", "next_attempt_at")
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`drop index if exists "IDX_outbox_events_lease"`);
    await queryRunner.query(`
      alter table "outbox_events"
      drop column if exists "locked_until"
    `);
    await queryRunner.query(`
      alter table "outbox_events"
      drop column if exists "locked_by"
    `);
  }
}
