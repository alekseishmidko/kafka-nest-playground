import type { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Добавляет durable хранилище для `Idempotency-Key` на `POST /orders`.
 *
 * Таблица хранит hash нормализованного тела запроса и готовый response. Это
 * позволяет вернуть клиенту тот же ответ при повторе того же ключа и тела, не
 * создавая второй заказ и второе `OrderCreated` событие.
 */
export class CreateOrderCreateIdempotencyKeys1718928000000 implements MigrationInterface {
  name = "CreateOrderCreateIdempotencyKeys1718928000000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      create table if not exists "order_create_idempotency_keys" (
        "idempotency_key" varchar(160) not null,
        "request_hash" varchar(64) not null,
        "response" jsonb,
        "order_id" uuid,
        "created_at" timestamptz not null default now(),
        "updated_at" timestamptz not null default now(),
        constraint "PK_order_create_idempotency_keys" primary key ("idempotency_key"),
        constraint "FK_order_create_idempotency_keys_order_id"
          foreign key ("order_id") references "orders" ("id") on delete restrict
      )
    `);
    await queryRunner.query(`
      create index if not exists "IDX_order_create_idempotency_keys_created_at"
      on "order_create_idempotency_keys" ("created_at")
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `drop index if exists "IDX_order_create_idempotency_keys_created_at"`
    );
    await queryRunner.query(
      `drop table if exists "order_create_idempotency_keys"`
    );
  }
}
