import type { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Сохраняет W3C trace context рядом с outbox-событием.
 *
 * JSONB выбран намеренно: кроме обязательного `traceparent` propagator может
 * добавить `tracestate`, а в будущем baggage без изменения схемы таблицы.
 */
export class AddOutboxTraceContext1718409600000
  implements MigrationInterface
{
  name = "AddOutboxTraceContext1718409600000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      alter table "outbox_events"
      add column if not exists "trace_context" jsonb
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      alter table "outbox_events"
      drop column if exists "trace_context"
    `);
  }
}
