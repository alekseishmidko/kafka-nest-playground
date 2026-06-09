import {
  assertGatewayIsReachable,
  connectPostgres,
  createOrder,
  json,
  readOrderCreatedOutbox,
  waitFor,
  waitForKafkaEvent
} from "./lib/e2e-toolkit.mjs";

const ORDER_EVENTS_TOPIC = "order.order-events";

/**
 * Проверяет happy path transactional outbox:
 *
 * 1. Заказ и outbox-запись появляются в одной базе.
 * 2. Publisher переводит запись в `PUBLISHED`.
 * 3. Kafka действительно содержит `OrderCreated` с тем же `eventId`.
 */
async function main() {
  const postgres = await connectPostgres();
  let kafkaWaiter;

  try {
    await assertGatewayIsReachable();

    const order = await createOrder();
    kafkaWaiter = await waitForKafkaEvent({
      topic: ORDER_EVENTS_TOPIC,
      groupId: `e2e-outbox-${Date.now()}`,
      fromBeginning: true,
      predicate: async (event) =>
        event?.eventType === "OrderCreated" &&
        event?.payload?.orderId === order.id
    });
    const persistedOutbox = await waitFor(
      () => readOrderCreatedOutbox(postgres, order.id),
      `outbox row for order ${order.id}`
    );

    if (persistedOutbox.event.payload.orderId !== order.id) {
      throw new Error("Outbox payload orderId does not match created order");
    }

    const publishedOutbox = await waitFor(async () => {
      const row = await readOrderCreatedOutbox(postgres, order.id);

      return row?.status === "PUBLISHED" ? row : null;
    }, `outbox row ${persistedOutbox.id} to become PUBLISHED`);

    const kafkaEvent = await kafkaWaiter.eventPromise;

    if (kafkaEvent.eventId !== publishedOutbox.event_id) {
      throw new Error(
        `Kafka eventId ${kafkaEvent.eventId} does not match outbox eventId ${publishedOutbox.event_id}`
      );
    }

    console.log(
      json({
        ok: true,
        scenario: "transactional outbox happy path",
        orderId: order.id,
        outboxEventId: publishedOutbox.id,
        eventId: publishedOutbox.event_id,
        status: publishedOutbox.status,
        publishedAt: publishedOutbox.published_at
      })
    );
  } finally {
    await kafkaWaiter?.close().catch(() => undefined);
    await postgres.end().catch(() => undefined);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
