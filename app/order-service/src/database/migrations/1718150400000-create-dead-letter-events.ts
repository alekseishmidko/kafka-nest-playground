import type { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Создаёт административное хранилище Dead Letter Queue.
 */
export class CreateDeadLetterEvents1718150400000
  implements MigrationInterface
{
  name = "CreateDeadLetterEvents1718150400000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      do $$
      begin
        create type "dead_letter_events_status_enum" as enum (
          'NEW',
          'REPROCESSED',
          'IGNORED'
        );
      exception
        when duplicate_object then null;
      end
      $$
    `);
    await queryRunner.query(`
      create table if not exists "dead_letter_events" (
        "id" uuid not null default uuid_generate_v4(),
        "dead_letter_event_id" varchar(120) not null,
        "original_event_id" varchar(120),
        "original_topic" varchar(160) not null,
        "original_partition" integer not null,
        "original_offset" varchar(64) not null,
        "message_key" varchar(160),
        "error_code" varchar(120) not null,
        "error_message" text not null,
        "error_stack" text,
        "retry_count" integer not null default 0,
        "first_failed_at" timestamptz,
        "original_event" jsonb,
        "status" "dead_letter_events_status_enum" not null default 'NEW',
        "reprocessed_event_id" varchar(120),
        "reprocessed_at" timestamptz,
        "ignored_at" timestamptz,
        "ignore_reason" text,
        "created_at" timestamptz not null default now(),
        "updated_at" timestamptz not null default now(),
        constraint "PK_dead_letter_events_id" primary key ("id"),
        constraint "UQ_dead_letter_events_event_id"
          unique ("dead_letter_event_id")
      )
    `);
    await queryRunner.query(`
      create index if not exists "IDX_dead_letter_events_status_created"
      on "dead_letter_events" ("status", "created_at" desc)
    `);
    await queryRunner.query(`
      create index if not exists "IDX_dead_letter_events_original_event_id"
      on "dead_letter_events" ("original_event_id")
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `drop index if exists "IDX_dead_letter_events_original_event_id"`
    );
    await queryRunner.query(
      `drop index if exists "IDX_dead_letter_events_status_created"`
    );
    await queryRunner.query(`drop table if exists "dead_letter_events"`);
    await queryRunner.query(
      `drop type if exists "dead_letter_events_status_enum"`
    );
  }
}
