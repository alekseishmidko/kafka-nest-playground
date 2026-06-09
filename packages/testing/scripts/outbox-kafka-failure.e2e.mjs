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
 * Проверяет восстановление outbox после недоступности Kafka.
 *
 * Тест намеренно управляет Docker Compose и всегда пытается восстановить Kafka
 * в `finally`, чтобы не оставлять локальное окружение в сломанном состоянии.
 */
async function main() {
  const postgres = await connectPostgres();
  let kafkaStopped = false;

  try {
    await assertGatewayIsReachable();
    await runCompose("stop", "kafka");
    kafkaStopped = true;

    const order = await createOrder({
      userId: `e2e-kafka-failure-${Date.now()}`
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

    await runCompose("up", "-d", "kafka", "schema-registry");
    kafkaStopped = false;
    await waitForKafka();
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
        scenario: "Kafka outage and outbox recovery",
        orderId: order.id,
        outboxEventId: publishedOutbox.id,
        failedAttempts: publishedOutbox.attempts,
        finalStatus: publishedOutbox.status,
        publishedAt: publishedOutbox.published_at
      })
    );
  } finally {
    if (kafkaStopped) {
      await runCompose("up", "-d", "kafka", "schema-registry").catch(
        (restoreError) => {
          console.error("Failed to restore Kafka after e2e failure", restoreError);
        }
      );
    }

    await postgres.end().catch(() => undefined);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
