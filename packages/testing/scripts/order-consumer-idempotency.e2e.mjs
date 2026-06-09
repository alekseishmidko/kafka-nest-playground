import { randomUUID } from "node:crypto";
import {
  connectPostgres,
  createKafkaProducer,
  encodeEvent,
  json,
  sleep,
  waitFor
} from "./lib/e2e-toolkit.mjs";

const PAYMENT_EVENTS_TOPIC = "payment.payment-events";
const PAYMENT_AUTHORIZED_SUBJECT =
  "payment.payment-events-PaymentAuthorized-value";

/**
 * Проверяет идемпотентность consumer-а `order-service`.
 *
 * Одно и то же событие с одинаковым `eventId` публикуется дважды. После первой
 * обработки статус меняется, а после второй `updatedAt` заказа остаётся прежним
 * и в `processed_kafka_events` существует ровно одна запись.
 */
async function main() {
  const postgres = await connectPostgres();
  const producer = await createKafkaProducer(`e2e-idempotency-${Date.now()}`);
  const orderId = randomUUID();
  const eventId = randomUUID();
  const event = createPaymentAuthorizedEvent({ orderId, eventId });

  try {
    await insertIsolatedOrder(postgres, orderId);

    const encodedEvent = await encodeEvent(PAYMENT_AUTHORIZED_SUBJECT, event);

    await publishEvent(producer, orderId, encodedEvent);

    const firstProcessing = await waitFor(
      () => readProcessingState(postgres, orderId, eventId),
      `first processing of event ${eventId}`
    );

    if (
      firstProcessing.order.status !== "PAYMENT_AUTHORIZED" ||
      firstProcessing.processedCount !== 1
    ) {
      throw new Error(`Unexpected first processing state: ${json(firstProcessing)}`);
    }

    const firstUpdatedAt = new Date(firstProcessing.order.updated_at).getTime();

    await publishEvent(producer, orderId, encodedEvent);

    // Consumer processing is asynchronous. A short pause gives the duplicate
    // enough time to reach the idempotency gate before final assertions.
    await sleep(2000);

    const duplicateState = await readProcessingState(postgres, orderId, eventId);
    const duplicateUpdatedAt = new Date(duplicateState.order.updated_at).getTime();

    if (duplicateState.processedCount !== 1) {
      throw new Error(`Duplicate event created extra processed rows: ${json(duplicateState)}`);
    }

    if (duplicateUpdatedAt !== firstUpdatedAt) {
      throw new Error(
        "Duplicate event changed order.updatedAt, so business update was executed twice"
      );
    }

    console.log(
      json({
        ok: true,
        scenario: "order consumer idempotency",
        orderId,
        eventId,
        processedRows: duplicateState.processedCount,
        status: duplicateState.order.status,
        updatedAt: duplicateState.order.updated_at
      })
    );
  } finally {
    await producer.disconnect().catch(() => undefined);
    await postgres.end().catch(() => undefined);
  }
}

function createPaymentAuthorizedEvent({ orderId, eventId }) {
  return {
    eventId,
    eventType: "PaymentAuthorized",
    eventVersion: 1,
    occurredAt: new Date().toISOString(),
    correlationId: randomUUID(),
    causationId: randomUUID(),
    producer: "e2e-idempotency-test",
    payload: {
      paymentId: randomUUID(),
      orderId,
      amount: 100,
      currency: "USD",
      provider: "e2e-provider"
    }
  };
}

async function insertIsolatedOrder(postgres, orderId) {
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
      values ($1, $2, 'USD', 100, 1, 'PENDING', $3::jsonb)
    `,
    [
      orderId,
      `e2e-idempotency-${Date.now()}`,
      JSON.stringify([{ productId: "e2e-product-1", quantity: 1, unitPrice: 100 }])
    ]
  );
}

async function publishEvent(producer, orderId, value) {
  await producer.send({
    topic: PAYMENT_EVENTS_TOPIC,
    messages: [
      {
        key: orderId,
        value
      }
    ]
  });
}

async function readProcessingState(postgres, orderId, eventId) {
  const [orderResult, processedResult] = await Promise.all([
    postgres.query(
      `
        select id, status, "updatedAt" as updated_at
        from orders
        where id = $1
      `,
      [orderId]
    ),
    postgres.query(
      `
        select count(*)::int as count
        from processed_kafka_events
        where event_id = $1
      `,
      [eventId]
    )
  ]);
  const order = orderResult.rows[0] ?? null;
  const processedCount = processedResult.rows[0]?.count ?? 0;

  if (!order || processedCount === 0) {
    return null;
  }

  return {
    order,
    processedCount
  };
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
