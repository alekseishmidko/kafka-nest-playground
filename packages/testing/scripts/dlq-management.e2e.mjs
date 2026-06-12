import { randomUUID } from "node:crypto";
import {
  connectPostgres,
  createKafkaProducer,
  e2eConfig,
  encodeEvent,
  json,
  waitFor
} from "./lib/e2e-toolkit.mjs";

const RISK_TOPIC = "risk.risk-events";
const RISK_APPROVED_SUBJECT =
  "risk.risk-events-OrderRiskApproved-value";
const OPERATOR_API_KEY =
  process.env.E2E_DLQ_OPERATOR_API_KEY ?? "local-dlq-operator-key";

/**
 * Проверяет полный административный цикл DLQ:
 *
 * invalid event -> dead-letter.events -> PostgreSQL -> Admin API reprocess
 * -> transactional outbox -> Kafka -> успешная обработка order-service.
 */
async function main() {
  const postgres = await connectPostgres();
  const producer = await createKafkaProducer(
    `e2e-dlq-management-${Date.now()}`
  );
  const orderId = randomUUID();
  const originalEvent = createInvalidRiskEvent();

  try {
    await assertAdminApiIsReachable();
    await insertPendingOrder(postgres, orderId);
    await publishRiskEvent(producer, originalEvent);

    const dlq = await waitFor(
      () => readDlqByOriginalEventId(postgres, originalEvent.eventId),
      `DLQ row for event ${originalEvent.eventId}`
    );

    assertEqual(dlq.status, "NEW", "initial DLQ status");
    assertEqual(dlq.error_code, "INVALID_ORDER_ID", "DLQ error code");
    assertEqual(
      dlq.original_topic,
      RISK_TOPIC,
      "DLQ original topic"
    );

    const reprocessed = await reprocessDlqEvent(
      dlq.id,
      dlq.version,
      { orderId }
    );

    assertEqual(
      reprocessed.status,
      "REPROCESSED",
      "reprocessed DLQ status"
    );
    assertNotEqual(
      reprocessed.reprocessedEventId,
      originalEvent.eventId,
      "new eventId"
    );
    assertEqual(
      reprocessed.resolvedBy,
      "dlq-operator",
      "operator identity"
    );

    const auditLog = await waitFor(
      () => readAuditLog(postgres, dlq.id),
      `audit log for DLQ row ${dlq.id}`
    );

    assertEqual(auditLog.action, "REPROCESS", "audit action");
    assertEqual(
      auditLog.operator_id,
      "dlq-operator",
      "audit operator"
    );
    assertEqual(
      auditLog.reprocessed_event_id,
      reprocessed.reprocessedEventId,
      "audit reprocessed eventId"
    );

    const outbox = await waitFor(
      async () => {
        const row = await readOutboxByEventId(
          postgres,
          reprocessed.reprocessedEventId
        );

        return row?.status === "PUBLISHED" ? row : null;
      },
      `reprocessed outbox ${reprocessed.reprocessedEventId} to become PUBLISHED`
    );
    const order = await waitFor(
      async () => {
        const row = await readOrder(postgres, orderId);

        return row?.status === "RISK_APPROVED" ? row : null;
      },
      `order ${orderId} to become RISK_APPROVED`
    );

    console.log(
      json({
        scenario: "DLQ management",
        dlqId: dlq.id,
        originalEventId: originalEvent.eventId,
        reprocessedEventId: reprocessed.reprocessedEventId,
        auditAction: auditLog.action,
        outboxStatus: outbox.status,
        orderId,
        orderStatus: order.status
      })
    );
  } finally {
    await producer.disconnect();
    await postgres.end();
  }
}

async function assertAdminApiIsReachable() {
  const response = await fetch(
    new URL("/admin/dlq?limit=1", e2eConfig.orderAdminUrl),
    {
      headers: {
        "x-admin-api-key": OPERATOR_API_KEY
      }
    }
  );

  if (!response.ok) {
    throw new Error(
      `Order Admin API is unavailable: ${response.status} ${await response.text()}`
    );
  }
}

async function insertPendingOrder(postgres, orderId) {
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
      `e2e-dlq-${Date.now()}`,
      JSON.stringify([
        {
          productId: "e2e-product-1",
          quantity: 1,
          unitPrice: 100
        }
      ])
    ]
  );
}

async function publishRiskEvent(producer, event) {
  const value = await encodeEvent(RISK_APPROVED_SUBJECT, event);

  await producer.send({
    topic: RISK_TOPIC,
    messages: [
      {
        key: event.payload.orderId,
        value
      }
    ]
  });
}

function createInvalidRiskEvent() {
  return {
    eventId: randomUUID(),
    eventType: "OrderRiskApproved",
    eventVersion: 1,
    occurredAt: new Date().toISOString(),
    correlationId: randomUUID(),
    causationId: randomUUID(),
    producer: "e2e-dlq-management",
    payload: {
      orderId: `invalid-order-${Date.now()}`,
      amount: 100,
      currency: "USD",
      riskScore: 0.1,
      approvedBy: "e2e-test"
    }
  };
}

/**
 * Отправляет административную команду reprocess.
 *
 * Версия защищает запись от параллельного изменения другим оператором,
 * а комментарий сохраняется в неизменяемом журнале аудита.
 */
async function reprocessDlqEvent(id, version, payload) {
  const response = await fetch(
    new URL(`/admin/dlq/${id}/reprocess`, e2eConfig.orderAdminUrl),
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-admin-api-key": OPERATOR_API_KEY
      },
      body: JSON.stringify({
        payload,
        version,
        comment:
          "Исправление orderId в автоматическом E2E-сценарии"
      })
    }
  );
  const body = await response.text();

  if (!response.ok) {
    throw new Error(
      `DLQ reprocess failed: ${response.status} ${body}`
    );
  }

  return JSON.parse(body);
}

async function readDlqByOriginalEventId(postgres, eventId) {
  const result = await postgres.query(
    `
      select
        id,
        original_event_id,
        original_topic,
        error_code,
        retry_count,
        status,
        version,
        reprocessed_event_id
      from dead_letter_events
      where original_event_id = $1
      order by created_at desc
      limit 1
    `,
    [eventId]
  );

  return result.rows[0] ?? null;
}

async function readOutboxByEventId(postgres, eventId) {
  const result = await postgres.query(
    `
      select event_id, status
      from outbox_events
      where event_id = $1
    `,
    [eventId]
  );

  return result.rows[0] ?? null;
}

/**
 * Читает неизменяемую запись административного действия.
 */
async function readAuditLog(postgres, deadLetterEventId) {
  const result = await postgres.query(
    `
      select action, operator_id, reprocessed_event_id, comment
      from dlq_audit_log
      where dead_letter_event_id = $1
      order by created_at desc
      limit 1
    `,
    [deadLetterEventId]
  );

  return result.rows[0] ?? null;
}

async function readOrder(postgres, orderId) {
  const result = await postgres.query(
    `
      select id, status
      from orders
      where id = $1
    `,
    [orderId]
  );

  return result.rows[0] ?? null;
}

function assertEqual(actual, expected, label) {
  if (actual !== expected) {
    throw new Error(
      `${label}: expected ${json(expected)}, got ${json(actual)}`
    );
  }
}

function assertNotEqual(actual, expected, label) {
  if (actual === expected) {
    throw new Error(
      `${label}: values must differ, received ${json(actual)}`
    );
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
