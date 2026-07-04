import {
  assertGatewayIsReachable,
  connectPostgres,
  createOrder,
  json,
  readOrderCreatedOutbox,
  runCompose,
  waitFor,
  waitForKafka,
  waitForSchemaRegistry
} from "./lib/e2e-toolkit.mjs";

/**
 * Проверяет восстановление outbox после недоступности Schema Registry.
 *
 * Outbox publisher сериализует событие перед отправкой в Kafka. Если Schema
 * Registry недоступен, запись должна перейти в `FAILED`, сохранить ошибку и
 * затем опубликоваться после восстановления registry без потери события.
 */
async function main() {
  const postgres = await connectPostgres();
  let registryStopped = false;

  try {
    await assertGatewayIsReachable();
    await waitForKafka();
    await runCompose("stop", "schema-registry");
    registryStopped = true;

    const order = await createOrder({
      userId: `e2e-schema-registry-failure-${Date.now()}`
    });

    const failedOutbox = await waitFor(async () => {
      const row = await readOrderCreatedOutbox(postgres, order.id);

      return row?.status === "FAILED" ? row : null;
    }, `outbox event for order ${order.id} to become FAILED`, {
      timeoutMs: 90000
    });

    if (!failedOutbox.last_error || failedOutbox.attempts < 1) {
      throw new Error("FAILED outbox row must contain attempts and last_error");
    }

    await runCompose("up", "-d", "schema-registry");
    registryStopped = false;
    await waitForSchemaRegistry();

    const publishedOutbox = await waitFor(async () => {
      const row = await readOrderCreatedOutbox(postgres, order.id);

      return row?.status === "PUBLISHED" ? row : null;
    }, `failed outbox event ${failedOutbox.id} to recover`, {
      timeoutMs: 90000
    });

    console.log(
      json({
        ok: true,
        scenario: "Schema Registry outage and outbox recovery",
        orderId: order.id,
        outboxEventId: publishedOutbox.id,
        failedAttempts: publishedOutbox.attempts,
        finalStatus: publishedOutbox.status,
        publishedAt: publishedOutbox.published_at
      })
    );
  } finally {
    if (registryStopped) {
      await runCompose("up", "-d", "schema-registry").catch((restoreError) => {
        console.error(
          "Failed to restore Schema Registry after e2e failure",
          restoreError
        );
      });
    }

    await postgres.end().catch(() => undefined);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
