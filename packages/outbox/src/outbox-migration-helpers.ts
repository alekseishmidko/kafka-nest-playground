/**
 * Параметры генерации SQL для таблицы transactional outbox.
 */
export interface OutboxMigrationOptions {
  /** Имя таблицы outbox. */
  tableName?: string;
  /** Имя PostgreSQL enum-типа для статуса. */
  statusEnumName?: string;
  /** Имя unique constraint по `event_id`. */
  eventIdUniqueConstraintName?: string;
  /** Имя primary key constraint. */
  primaryKeyConstraintName?: string;
  /** Имя индекса для выборки publishable-записей. */
  publishableIndexName?: string;
}

/**
 * Возвращает SQL-запросы для создания таблицы transactional outbox.
 *
 * Helper не выполняет запросы сам: миграция приложения остаётся владельцем
 * порядка DDL-операций и может добавить свои таблицы в той же migration file.
 */
export function createOutboxSchemaQueries(
  options: OutboxMigrationOptions = {}
): string[] {
  const names = resolveNames(options);

  return [
    `
      do $$
      begin
        create type "${names.statusEnumName}" as enum (
          'PENDING',
          'PUBLISHED',
          'FAILED'
        );
      exception
        when duplicate_object then null;
      end
      $$
    `,
    `
      create table if not exists "${names.tableName}" (
        "id" uuid not null default uuid_generate_v4(),
        "topic" varchar(160) not null,
        "message_key" varchar(160) not null,
        "event_type" varchar(80) not null,
        "event_id" uuid not null,
        "event" jsonb not null,
        "trace_context" jsonb,
        "status" "${names.statusEnumName}" not null default 'PENDING',
        "attempts" integer not null default 0,
        "next_attempt_at" timestamptz,
        "published_at" timestamptz,
        "last_error" text,
        "created_at" timestamptz not null default now(),
        "updated_at" timestamptz not null default now(),
        constraint "${names.eventIdUniqueConstraintName}" unique ("event_id"),
        constraint "${names.primaryKeyConstraintName}" primary key ("id")
      )
    `,
    `
      create index if not exists "${names.publishableIndexName}"
      on "${names.tableName}" ("status", "next_attempt_at", "created_at")
    `
  ];
}

/**
 * Возвращает SQL-запросы для удаления outbox-схемы.
 *
 * Используйте только в migration `down` и только если приложение действительно
 * владеет этой таблицей. На production откат с данными требует отдельного
 * backup-плана.
 */
export function dropOutboxSchemaQueries(
  options: OutboxMigrationOptions = {}
): string[] {
  const names = resolveNames(options);

  return [
    `drop index if exists "${names.publishableIndexName}"`,
    `drop table if exists "${names.tableName}"`,
    `drop type if exists "${names.statusEnumName}"`
  ];
}

function resolveNames(options: OutboxMigrationOptions): Required<OutboxMigrationOptions> {
  return {
    tableName: options.tableName ?? "outbox_events",
    statusEnumName: options.statusEnumName ?? "outbox_events_status_enum",
    eventIdUniqueConstraintName:
      options.eventIdUniqueConstraintName ?? "UQ_outbox_events_event_id",
    primaryKeyConstraintName:
      options.primaryKeyConstraintName ?? "PK_outbox_events_id",
    publishableIndexName:
      options.publishableIndexName ?? "IDX_outbox_events_publishable"
  };
}
