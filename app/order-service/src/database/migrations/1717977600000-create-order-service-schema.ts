import type { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Создаёт полную схему хранения `order-service`.
 *
 * Миграция является initial migration: она должна позволять запустить сервис
 * на пустой базе без `TYPEORM_SYNCHRONIZE=true`. Имена enum types заданы явно,
 * чтобы последующие миграции могли безопасно ссылаться на них.
 */
export class CreateOrderServiceSchema1717977600000 implements MigrationInterface {
  name = "CreateOrderServiceSchema1717977600000";

  /**
   * Создаёт orders, transactional outbox и таблицу идемпотентности consumers.
   */
  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`create extension if not exists "uuid-ossp"`);
    await queryRunner.query(`
      do $$
      begin
        create type "orders_status_enum" as enum (
          'PENDING',
          'RISK_APPROVED',
          'RISK_REJECTED',
          'PAYMENT_AUTHORIZED',
          'PAYMENT_FAILED'
        );
      exception
        when duplicate_object then null;
      end
      $$
    `);
    await queryRunner.query(`
      create table if not exists "orders" (
        "id" uuid not null default uuid_generate_v4(),
        "userId" varchar(120) not null,
        "currency" varchar(3) not null,
        "totalAmount" numeric(12, 2) not null,
        "itemCount" integer not null,
        "status" "orders_status_enum" not null default 'PENDING',
        "items" jsonb not null,
        "createdAt" timestamptz not null default now(),
        "updatedAt" timestamptz not null default now(),
        constraint "PK_orders_id" primary key ("id")
      )
    `);

    await queryRunner.query(`
      do $$
      begin
        create type "outbox_events_status_enum" as enum (
          'PENDING',
          'PUBLISHED',
          'FAILED'
        );
      exception
        when duplicate_object then null;
      end
      $$
    `);
    await queryRunner.query(`
      create table if not exists "outbox_events" (
        "id" uuid not null default uuid_generate_v4(),
        "topic" varchar(160) not null,
        "message_key" varchar(160) not null,
        "event_type" varchar(80) not null,
        "event_id" uuid not null,
        "event" jsonb not null,
        "status" "outbox_events_status_enum" not null default 'PENDING',
        "attempts" integer not null default 0,
        "next_attempt_at" timestamptz,
        "published_at" timestamptz,
        "last_error" text,
        "created_at" timestamptz not null default now(),
        "updated_at" timestamptz not null default now(),
        constraint "UQ_outbox_events_event_id" unique ("event_id"),
        constraint "PK_outbox_events_id" primary key ("id")
      )
    `);
    await queryRunner.query(`
      create index if not exists "IDX_outbox_events_publishable"
      on "outbox_events" ("status", "next_attempt_at", "created_at")
    `);

    await queryRunner.query(`
      create table if not exists "processed_kafka_events" (
        "id" uuid not null default uuid_generate_v4(),
        "event_id" uuid not null,
        "event_type" varchar(80) not null,
        "source_topic" varchar(160) not null,
        "source_offset" varchar(64) not null,
        "processed_at" timestamptz not null default now(),
        constraint "UQ_processed_kafka_events_event_id" unique ("event_id"),
        constraint "PK_processed_kafka_events_id" primary key ("id")
      )
    `);
  }

  /**
   * Удаляет схему в обратном порядке зависимостей.
   *
   * Revert предназначен для локальной разработки. На production откат
   * миграции с данными должен выполняться только после отдельного backup-плана.
   */
  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`drop table "processed_kafka_events"`);
    await queryRunner.query(`drop index "IDX_outbox_events_publishable"`);
    await queryRunner.query(`drop table "outbox_events"`);
    await queryRunner.query(`drop type "outbox_events_status_enum"`);
    await queryRunner.query(`drop table "orders"`);
    await queryRunner.query(`drop type "orders_status_enum"`);
  }
}
