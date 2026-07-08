import {
  assertGatewayIsReachable,
  connectPostgres,
  createOrder,
  json,
  waitFor,
  waitForKafkaEvent
} from "./lib/e2e-toolkit.mjs";

const TOPICS = {
  orderEvents: "order.order-events",
  riskEvents: "risk.risk-events",
  paymentEvents: "payment.payment-events"
};

const FINAL_ORDER_EVENT_TYPES = new Set(["OrderConfirmed", "OrderCancelled"]);
const FINAL_ORDER_STATUSES = new Set(["CONFIRMED", "CANCELLED"]);

/**
 * Обязательный e2e gate для production-like order flow.
 *
 * Сценарий проверяет не только итоговую строку в `orders`, но и все важные
 * асинхронные границы: публикацию OrderCreated, обработку risk, обработку
 * payment, финальное order-событие и факт consumption в notification-service.
 */
async function main() {
  const postgres = await connectPostgres();
  const uniqueUserId = `e2e-gate-user-${Date.now()}`;
  let orderCreatedWatcher;

  try {
    await assertNoOutboxBacklog(postgres);
    await assertGatewayIsReachable();

    orderCreatedWatcher = await waitForKafkaEvent({
      topic: TOPICS.orderEvents,
      groupId: `e2e-gate-order-created-${Date.now()}`,
      predicate: (event) =>
        event.eventType === "OrderCreated" &&
        event.payload?.userId === uniqueUserId
    });

    const order = await createOrder({
      userId: uniqueUserId,
      currency: "USD",
      items: [
        {
          productId: "e2e-gate-product-1",
          quantity: 1,
          unitPrice: 1
        }
      ]
    });

    assertOrderCreatedResponse(order);

    const orderCreatedEvent = await orderCreatedWatcher.eventPromise;

    assertEqual(
      orderCreatedEvent.payload.orderId,
      order.id,
      "OrderCreated.payload.orderId"
    );

    const riskEvent = await waitForMatchingKafkaEvent({
      topic: TOPICS.riskEvents,
      groupIdPrefix: "e2e-gate-risk",
      orderId: order.id
    });

    if (riskEvent.eventType !== "OrderRiskApproved") {
      throw new Error(
        `Expected low-value e2e gate order to be risk approved, got ${json(riskEvent)}`
      );
    }

    const paymentEvent = await waitForMatchingKafkaEvent({
      topic: TOPICS.paymentEvents,
      groupIdPrefix: "e2e-gate-payment",
      orderId: order.id
    });

    const finalOrderEvent = await waitForMatchingKafkaEvent({
      topic: TOPICS.orderEvents,
      groupIdPrefix: "e2e-gate-final-order-event",
      orderId: order.id,
      predicate: (event) => FINAL_ORDER_EVENT_TYPES.has(event.eventType)
    });
    const finalOrder = await waitForFinalOrderStatus(postgres, order.id);
    const notificationInbox = await waitForNotificationConsumed(
      postgres,
      finalOrderEvent.eventId
    );

    console.log(
      json({
        ok: true,
        scenario: "production e2e gate",
        checks: {
          createOrder: {
            orderId: order.id,
            initialStatus: order.status
          },
          kafkaEventProduced: {
            eventType: orderCreatedEvent.eventType,
            eventId: orderCreatedEvent.eventId
          },
          riskProcessed: {
            eventType: riskEvent.eventType,
            eventId: riskEvent.eventId,
            riskScore: riskEvent.payload.riskScore
          },
          paymentProcessed: {
            eventType: paymentEvent.eventType,
            eventId: paymentEvent.eventId
          },
          orderStatusChanged: {
            status: finalOrder.status,
            updatedAt: finalOrder.updated_at
          },
          notificationConsumed: {
            consumerName: notificationInbox.consumer_name,
            eventId: notificationInbox.event_id,
            status: notificationInbox.status,
            completedAt: notificationInbox.completed_at
          }
        }
      })
    );
  } finally {
    await orderCreatedWatcher?.close().catch(() => undefined);
    await postgres.end().catch(() => undefined);
  }
}

async function assertNoOutboxBacklog(postgres) {
  const result = await postgres.query(
    `
      select count(*)::int as count
      from outbox_events
      where status = 'PENDING'
    `
  );
  const pendingCount = result.rows[0]?.count ?? 0;

  if (pendingCount > 0) {
    throw new Error(
      [
        `Production e2e gate requires an empty pending outbox, got ${pendingCount} PENDING rows.`,
        "The outbox publisher processes rows oldest-first, so existing backlog can hide whether the new order flow is healthy.",
        "Use a clean local database or explicitly drain/clear the local test backlog before running `pnpm test:e2e:gate`."
      ].join("\n")
    );
  }
}

async function waitForMatchingKafkaEvent({
  topic,
  groupIdPrefix,
  orderId,
  predicate = () => true
}) {
  const watcher = await waitForKafkaEvent({
    topic,
    groupId: `${groupIdPrefix}-${Date.now()}`,
    fromBeginning: true,
    predicate: (event) =>
      event.payload?.orderId === orderId && predicate(event)
  });

  try {
    return await watcher.eventPromise;
  } finally {
    await watcher.close().catch(() => undefined);
  }
}

function assertOrderCreatedResponse(order) {
  if (!order.id) {
    throw new Error(`Gateway response does not contain order id: ${json(order)}`);
  }

  if (order.status !== "PENDING") {
    throw new Error(
      `Expected newly created order to be PENDING, got ${order.status}: ${json(order)}`
    );
  }
}

async function waitForFinalOrderStatus(postgres, orderId) {
  return waitFor(async () => {
    const result = await postgres.query(
      `
        select id, status, "updatedAt" as updated_at
        from orders
        where id = $1
      `,
      [orderId]
    );
    const order = result.rows[0] ?? null;

    return FINAL_ORDER_STATUSES.has(order?.status) ? order : null;
  }, `order ${orderId} to reach final status`);
}

async function waitForNotificationConsumed(postgres, eventId) {
  return waitFor(async () => {
    const result = await postgres.query(
      `
        select consumer_name, event_id, event_type, status, completed_at
        from kafka_consumer_inbox
        where consumer_name = 'notification-service'
          and event_id = $1
          and status = 'COMPLETED'
      `,
      [eventId]
    );

    return result.rows[0] ?? null;
  }, `notification-service to consume event ${eventId}`);
}

function assertEqual(actual, expected, label) {
  if (actual !== expected) {
    throw new Error(`${label}: expected ${json(expected)}, got ${json(actual)}`);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
