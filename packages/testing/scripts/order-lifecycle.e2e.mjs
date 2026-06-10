import { randomUUID } from "node:crypto";
import {
  connectPostgres,
  createKafkaProducer,
  encodeEvent,
  json,
  readOrderOutboxEvent,
  sleep,
  waitFor
} from "./lib/e2e-toolkit.mjs";

const RISK_TOPIC = "risk.risk-events";
const PAYMENT_TOPIC = "payment.payment-events";
const SUBJECTS = {
  OrderRiskApproved: "risk.risk-events-OrderRiskApproved-value",
  OrderRiskRejected: "risk.risk-events-OrderRiskRejected-value",
  PaymentAuthorized: "payment.payment-events-PaymentAuthorized-value",
  PaymentFailed: "payment.payment-events-PaymentFailed-value"
};

/**
 * Проверяет полный lifecycle заказа на реальных Kafka и PostgreSQL:
 *
 * - успешное подтверждение;
 * - отмену по risk rejection;
 * - отмену по payment failure;
 * - повторную доставку одного eventId;
 * - событие, пришедшее в недопустимом порядке.
 */
async function main() {
  const postgres = await connectPostgres();
  const producer = await createKafkaProducer(`e2e-order-lifecycle-${Date.now()}`);

  try {
    const results = [];

    results.push(await verifySuccessfulOrder(postgres, producer));
    results.push(await verifyRiskRejection(postgres, producer));
    results.push(await verifyPaymentFailure(postgres, producer));
    results.push(await verifyDuplicateEvent(postgres, producer));
    results.push(await verifyInvalidOrder(postgres, producer));

    console.log(
      json({
        ok: true,
        scenario: "complete order lifecycle",
        results
      })
    );
  } finally {
    await producer.disconnect().catch(() => undefined);
    await postgres.end().catch(() => undefined);
  }
}

async function verifySuccessfulOrder(postgres, producer) {
  const orderId = await insertOrder(postgres, "PENDING");
  const riskEvent = createRiskApprovedEvent(orderId);

  await publishDomainEvent(producer, riskEvent);
  await waitForOrderStatus(postgres, orderId, "RISK_APPROVED");

  const paymentEvent = createPaymentAuthorizedEvent(orderId);

  await publishDomainEvent(producer, paymentEvent);
  await waitForOrderStatus(postgres, orderId, "CONFIRMED");
  const outbox = await waitForPublishedOutbox(
    postgres,
    orderId,
    "OrderConfirmed"
  );

  assertEqual(outbox.event.causationId, paymentEvent.eventId, "confirmation causationId");

  return {
    name: "successful order",
    orderId,
    status: "CONFIRMED",
    finalEvent: outbox.event_type
  };
}

async function verifyRiskRejection(postgres, producer) {
  const orderId = await insertOrder(postgres, "PENDING");
  const event = createRiskRejectedEvent(orderId);

  await publishDomainEvent(producer, event);
  await waitForOrderStatus(postgres, orderId, "CANCELLED");
  const outbox = await waitForPublishedOutbox(
    postgres,
    orderId,
    "OrderCancelled"
  );

  assertEqual(outbox.event.payload.cancelledBy, "risk", "risk cancellation source");
  assertEqual(outbox.event.payload.reason, event.payload.reason, "risk cancellation reason");

  return {
    name: "risk rejection",
    orderId,
    status: "CANCELLED",
    finalEvent: outbox.event_type
  };
}

async function verifyPaymentFailure(postgres, producer) {
  const orderId = await insertOrder(postgres, "RISK_APPROVED");
  const event = createPaymentFailedEvent(orderId);

  await publishDomainEvent(producer, event);
  await waitForOrderStatus(postgres, orderId, "CANCELLED");
  const outbox = await waitForPublishedOutbox(
    postgres,
    orderId,
    "OrderCancelled"
  );

  assertEqual(outbox.event.payload.cancelledBy, "payment", "payment cancellation source");

  return {
    name: "payment failure",
    orderId,
    status: "CANCELLED",
    finalEvent: outbox.event_type
  };
}

async function verifyDuplicateEvent(postgres, producer) {
  const orderId = await insertOrder(postgres, "RISK_APPROVED");
  const event = createPaymentAuthorizedEvent(orderId);

  await publishDomainEvent(producer, event);
  const firstOrder = await waitForOrderStatus(postgres, orderId, "CONFIRMED");
  await waitForProcessedCount(postgres, event.eventId, 1);

  await publishDomainEvent(producer, event);
  await sleep(2000);

  const order = await readOrder(postgres, orderId);
  const processedCount = await readProcessedCount(postgres, event.eventId);
  const outboxCount = await readOutboxCount(
    postgres,
    orderId,
    "OrderConfirmed"
  );

  assertEqual(processedCount, 1, "processed rows for duplicate event");
  assertEqual(outboxCount, 1, "outbox rows for duplicate event");
  assertEqual(
    new Date(order.updated_at).getTime(),
    new Date(firstOrder.updated_at).getTime(),
    "updatedAt after duplicate event"
  );

  return {
    name: "duplicate event",
    orderId,
    status: order.status,
    processedRows: processedCount,
    outboxRows: outboxCount
  };
}

async function verifyInvalidOrder(postgres, producer) {
  const orderId = await insertOrder(postgres, "CONFIRMED");
  const initialOrder = await readOrder(postgres, orderId);
  const event = createRiskRejectedEvent(orderId);

  await publishDomainEvent(producer, event);
  await waitForProcessedCount(postgres, event.eventId, 1);

  const order = await readOrder(postgres, orderId);
  const outboxCount = await readOutboxCount(
    postgres,
    orderId,
    "OrderCancelled"
  );

  assertEqual(order.status, "CONFIRMED", "terminal status after invalid event");
  assertEqual(outboxCount, 0, "outbox rows after invalid event");
  assertEqual(
    new Date(order.updated_at).getTime(),
    new Date(initialOrder.updated_at).getTime(),
    "updatedAt after invalid event"
  );

  return {
    name: "invalid event order",
    orderId,
    status: order.status,
    outboxRows: outboxCount
  };
}

async function insertOrder(postgres, status) {
  const orderId = randomUUID();

  await postgres.query(
    `
      insert into orders (
        id,
        "userId",
        currency,
        "totalAmount",
        "itemCount",
        status,
        items
      )
      values ($1, $2, 'USD', 100, 1, $3, $4::jsonb)
    `,
    [
      orderId,
      `e2e-lifecycle-${Date.now()}`,
      status,
      JSON.stringify([
        {
          productId: "e2e-product-1",
          quantity: 1,
          unitPrice: 100
        }
      ])
    ]
  );

  return orderId;
}

async function publishDomainEvent(producer, event) {
  const topic =
    event.eventType.startsWith("OrderRisk") ? RISK_TOPIC : PAYMENT_TOPIC;
  const value = await encodeEvent(SUBJECTS[event.eventType], event);

  await producer.send({
    topic,
    messages: [
      {
        key: event.payload.orderId,
        value
      }
    ]
  });
}

function createRiskApprovedEvent(orderId) {
  return createEnvelope("OrderRiskApproved", {
    orderId,
    amount: 100,
    currency: "USD",
    riskScore: 0.1,
    approvedBy: "e2e-test"
  });
}

function createRiskRejectedEvent(orderId) {
  return createEnvelope("OrderRiskRejected", {
    orderId,
    riskScore: 0.95,
    reason: "e2e_risk_rejected",
    rejectedBy: "e2e-test"
  });
}

function createPaymentAuthorizedEvent(orderId) {
  return createEnvelope("PaymentAuthorized", {
    paymentId: randomUUID(),
    orderId,
    amount: 100,
    currency: "USD",
    provider: "e2e-provider"
  });
}

function createPaymentFailedEvent(orderId) {
  return createEnvelope("PaymentFailed", {
    paymentId: null,
    orderId,
    reason: "e2e_payment_declined",
    provider: "e2e-provider"
  });
}

function createEnvelope(eventType, payload) {
  return {
    eventId: randomUUID(),
    eventType,
    eventVersion: 1,
    occurredAt: new Date().toISOString(),
    correlationId: randomUUID(),
    causationId: randomUUID(),
    producer: "e2e-order-lifecycle",
    payload
  };
}

async function waitForOrderStatus(postgres, orderId, status) {
  return waitFor(async () => {
    const order = await readOrder(postgres, orderId);

    return order?.status === status ? order : null;
  }, `order ${orderId} to reach ${status}`);
}

async function waitForPublishedOutbox(postgres, orderId, eventType) {
  return waitFor(async () => {
    const outbox = await readOrderOutboxEvent(postgres, orderId, eventType);

    return outbox?.status === "PUBLISHED" ? outbox : null;
  }, `${eventType} outbox for order ${orderId} to become PUBLISHED`);
}

async function waitForProcessedCount(postgres, eventId, count) {
  return waitFor(async () => {
    const actual = await readProcessedCount(postgres, eventId);

    return actual === count ? actual : null;
  }, `processed count ${count} for event ${eventId}`);
}

async function readOrder(postgres, orderId) {
  const result = await postgres.query(
    `
      select id, status, "updatedAt" as updated_at
      from orders
      where id = $1
    `,
    [orderId]
  );

  return result.rows[0] ?? null;
}

async function readProcessedCount(postgres, eventId) {
  const result = await postgres.query(
    `
      select count(*)::int as count
      from processed_kafka_events
      where event_id = $1
    `,
    [eventId]
  );

  return result.rows[0]?.count ?? 0;
}

async function readOutboxCount(postgres, orderId, eventType) {
  const result = await postgres.query(
    `
      select count(*)::int as count
      from outbox_events
      where event_type = $2
        and event->'payload'->>'orderId' = $1
    `,
    [orderId, eventType]
  );

  return result.rows[0]?.count ?? 0;
}

function assertEqual(actual, expected, label) {
  if (actual !== expected) {
    throw new Error(
      `${label}: expected ${json(expected)}, got ${json(actual)}`
    );
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
