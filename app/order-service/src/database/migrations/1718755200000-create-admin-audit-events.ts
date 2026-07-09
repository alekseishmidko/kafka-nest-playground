import type { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Создаёт общий append-only audit trail для всех `/admin/*` endpoint-ов.
 */
export class CreateAdminAuditEvents1718755200000
  implements MigrationInterface
{
  name = "CreateAdminAuditEvents1718755200000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      create table if not exists "admin_audit_events" (
        "id" uuid not null default uuid_generate_v4(),
        "actor" varchar(160),
        "role" varchar(120),
        "method" varchar(16) not null,
        "path" text not null,
        "action" varchar(120) not null,
        "entity_type" varchar(120),
        "entity_id" varchar(160),
        "decision" varchar(24) not null,
        "status_code" integer not null,
        "request_id" varchar(160) not null,
        "correlation_id" varchar(160),
        "ip" inet,
        "user_agent" text,
        "duration_ms" integer not null,
        "metadata" jsonb,
        "created_at" timestamptz not null default now(),
        constraint "PK_admin_audit_events_id" primary key ("id")
      )
    `);
    await queryRunner.query(`
      create index if not exists "IDX_admin_audit_events_created"
      on "admin_audit_events" ("created_at" desc)
    `);
    await queryRunner.query(`
      create index if not exists "IDX_admin_audit_events_entity_created"
      on "admin_audit_events" ("entity_type", "entity_id", "created_at" desc)
    `);
    await queryRunner.query(`
      create index if not exists "IDX_admin_audit_events_actor_created"
      on "admin_audit_events" ("actor", "created_at" desc)
    `);
    await queryRunner.query(`
      create index if not exists "IDX_admin_audit_events_decision_created"
      on "admin_audit_events" ("decision", "created_at" desc)
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `drop index if exists "IDX_admin_audit_events_decision_created"`
    );
    await queryRunner.query(
      `drop index if exists "IDX_admin_audit_events_actor_created"`
    );
    await queryRunner.query(
      `drop index if exists "IDX_admin_audit_events_entity_created"`
    );
    await queryRunner.query(
      `drop index if exists "IDX_admin_audit_events_created"`
    );
    await queryRunner.query(`drop table if exists "admin_audit_events"`);
  }
}
