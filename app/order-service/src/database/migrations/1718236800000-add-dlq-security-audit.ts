import type { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Добавляет optimistic version, operator metadata и неизменяемый audit trail.
 */
export class AddDlqSecurityAudit1718236800000
  implements MigrationInterface
{
  name = "AddDlqSecurityAudit1718236800000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      alter table "dead_letter_events"
      add column if not exists "resolved_by" varchar(120)
    `);
    await queryRunner.query(`
      alter table "dead_letter_events"
      add column if not exists "resolution_comment" text
    `);
    await queryRunner.query(`
      alter table "dead_letter_events"
      add column if not exists "version" integer not null default 1
    `);
    await queryRunner.query(`
      create table if not exists "dlq_audit_log" (
        "id" uuid not null default uuid_generate_v4(),
        "dead_letter_event_id" uuid not null,
        "action" varchar(40) not null,
        "operator_id" varchar(120) not null,
        "previous_status" varchar(40) not null,
        "new_status" varchar(40) not null,
        "comment" text not null,
        "reprocessed_event_id" varchar(120),
        "created_at" timestamptz not null default now(),
        constraint "PK_dlq_audit_log_id" primary key ("id"),
        constraint "FK_dlq_audit_log_dead_letter_event"
          foreign key ("dead_letter_event_id")
          references "dead_letter_events" ("id")
          on delete cascade
      )
    `);
    await queryRunner.query(`
      create index if not exists "IDX_dlq_audit_log_event_created"
      on "dlq_audit_log" ("dead_letter_event_id", "created_at" desc)
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `drop index if exists "IDX_dlq_audit_log_event_created"`
    );
    await queryRunner.query(`drop table if exists "dlq_audit_log"`);
    await queryRunner.query(`
      alter table "dead_letter_events"
      drop column if exists "version"
    `);
    await queryRunner.query(`
      alter table "dead_letter_events"
      drop column if exists "resolution_comment"
    `);
    await queryRunner.query(`
      alter table "dead_letter_events"
      drop column if exists "resolved_by"
    `);
  }
}
