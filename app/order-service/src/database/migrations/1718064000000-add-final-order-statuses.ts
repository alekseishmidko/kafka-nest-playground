import type { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Расширяет PostgreSQL enum финальными статусами заказа.
 *
 * Старые `RISK_REJECTED`, `PAYMENT_AUTHORIZED` и `PAYMENT_FAILED` не удаляются:
 * они могли сохраниться в локальных или production-базах до введения финальной
 * state machine. Новая бизнес-логика больше не создаёт эти состояния.
 */
export class AddFinalOrderStatuses1718064000000
  implements MigrationInterface
{
  name = "AddFinalOrderStatuses1718064000000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      alter type "orders_status_enum"
      add value if not exists 'CONFIRMED'
    `);
    await queryRunner.query(`
      alter type "orders_status_enum"
      add value if not exists 'CANCELLED'
    `);
  }

  /**
   * PostgreSQL не поддерживает безопасное удаление отдельного enum value.
   *
   * Автоматический down намеренно отсутствует: откат требует проверки данных,
   * переноса строк из финальных статусов и пересоздания enum type.
   */
  async down(): Promise<void> {
    throw new Error(
      "Removing CONFIRMED/CANCELLED requires a manual data migration"
    );
  }
}
