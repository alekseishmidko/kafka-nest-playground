import {
  assertGatewayIsReachable,
  connectPostgres,
  createOrder,
  json,
  runCompose,
  waitFor,
  waitForPostgres
} from "./lib/e2e-toolkit.mjs";

const finalStatuses = new Set(["CONFIRMED", "CANCELLED"]);

/**
 * Проверяет, что сервисы восстанавливаются после restart PostgreSQL.
 *
 * Сценарий делает restart DB через Docker Compose, ждёт доступности Postgres,
 * затем создаёт новый заказ через gateway и проверяет, что order pipeline снова
 * может писать в БД, публиковать outbox и доводить заказ до финального статуса.
 */
async function main() {
  let postgres = null;

  try {
    await assertGatewayIsReachable();
    await runCompose("restart", "postgres");
    await waitForPostgres();

    postgres = await connectPostgres();
    const order = await createOrder({
      userId: `e2e-postgres-restart-${Date.now()}`
    });
    const finalOrder = await waitFor(
      () => readFinalOrder(postgres, order.id),
      `order ${order.id} to reach a final status after Postgres restart`,
      {
        timeoutMs: 90000
      }
    );

    console.log(
      json({
        ok: true,
        scenario: "Postgres restart and order pipeline recovery",
        orderId: order.id,
        initialStatus: order.status,
        finalStatus: finalOrder.status,
        updatedAt: finalOrder.updated_at
      })
    );
  } finally {
    await postgres?.end().catch(() => undefined);
  }
}

async function readFinalOrder(postgres, orderId) {
  const result = await postgres.query(
    `
      select id, status, "updatedAt" as updated_at
      from orders
      where id = $1
    `,
    [orderId]
  );
  const order = result.rows[0] ?? null;

  return order && finalStatuses.has(order.status) ? order : null;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
