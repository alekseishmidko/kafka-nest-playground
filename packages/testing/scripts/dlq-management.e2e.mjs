import { randomUUID } from "node:crypto";
import {
  connectPostgres,
  createKafkaProducer,
  e2eConfig,
  encodeEvent,
  json,
  waitForKafkaEvent,
  waitFor
} from "./lib/e2e-toolkit.mjs";

const RISK_TOPIC = "risk.risk-events";
const RETRY_TOPIC = "order.order-events.retry-5s";
const DLQ_TOPIC = "dead-letter.events";
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
  const traceId = randomUUID().replaceAll("-", "");
  const parentSpanId = randomUUID().replaceAll("-", "").slice(0, 16);
  let dlqHeaders;
  let reprocessedHeaders;
  let dlqWatcher;
  let reprocessedWatcher;

  try {
    await assertAdminApiIsReachable();
    await insertPendingOrder(postgres, orderId);
    dlqWatcher = await waitForKafkaEvent({
      topic: DLQ_TOPIC,
      groupId: `e2e-dlq-trace-${Date.now()}`,
      predicate: (event, message) => {
        if (event.causationId !== originalEvent.eventId) {
          return false;
        }

        dlqHeaders = normalizeKafkaHeaders(message.headers);
        return true;
      }
    });

    await publishRiskRetryEvent(
      producer,
      originalEvent,
      traceId,
      parentSpanId
    );
    await dlqWatcher.eventPromise;
    await dlqWatcher.close();
    dlqWatcher = null;

    assertEqual(
      dlqHeaders["x-trace-id"],
      traceId,
      "DLQ traceId"
    );
    assertTraceParent(dlqHeaders.traceparent, traceId, "DLQ");
    assertNotEqual(
      dlqHeaders["x-span-id"],
      parentSpanId,
      "DLQ producer spanId"
    );

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

    reprocessedWatcher = await waitForKafkaEvent({
      topic: RISK_TOPIC,
      groupId: `e2e-reprocessed-trace-${Date.now()}`,
      predicate: (event, message) => {
        if (
          event.producer !== "order-service-dlq-reprocessor" ||
          event.payload.orderId !== orderId
        ) {
          return false;
        }

        reprocessedHeaders = normalizeKafkaHeaders(message.headers);
        return true;
      }
    });
    const reprocessed = await reprocessDlqEvent(
      dlq.id,
      dlq.version,
      { orderId },
      traceId,
      parentSpanId
    );
    await reprocessedWatcher.eventPromise;
    await reprocessedWatcher.close();
    reprocessedWatcher = null;

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
    assertEqual(
      outbox.trace_context["x-trace-id"],
      traceId,
      "outbox traceId"
    );
    assertEqual(
      reprocessedHeaders["x-trace-id"],
      traceId,
      "reprocessed Kafka traceId"
    );
    assertNotEqual(
      reprocessedHeaders["x-span-id"],
      parentSpanId,
      "reprocessed Kafka producer spanId"
    );
    assertTraceParent(
      reprocessedHeaders.traceparent,
      traceId,
      "reprocessed Kafka event"
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
        traceId,
        outboxStatus: outbox.status,
        orderId,
        orderStatus: order.status
      })
    );
  } finally {
    await Promise.allSettled([
      dlqWatcher?.close(),
      reprocessedWatcher?.close()
    ]);
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

/**
 * Публикует событие сразу в первый retry topic с заранее известным trace.
 *
 * Такой вход сокращает E2E до одной реальной retry-задержки и одновременно
 * проверяет восстановление `x-original-topic`.
 */
async function publishRiskRetryEvent(
  producer,
  event,
  traceId,
  spanId
) {
  const value = await encodeEvent(RISK_APPROVED_SUBJECT, event);

  await producer.send({
    topic: RETRY_TOPIC,
    messages: [
      {
        key: event.payload.orderId,
        value,
        headers: {
          traceparent: `00-${traceId}-${spanId}-01`,
          "x-trace-id": traceId,
          "x-span-id": spanId,
          "x-original-topic": RISK_TOPIC,
          "x-retry-count": "1",
          "x-first-failed-at": new Date().toISOString(),
          "x-error-code": "E2E_RETRY_ERROR"
        }
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
async function reprocessDlqEvent(
  id,
  version,
  payload,
  traceId,
  spanId
) {
  const response = await fetch(
    new URL(`/admin/dlq/${id}/reprocess`, e2eConfig.orderAdminUrl),
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-admin-api-key": OPERATOR_API_KEY,
        traceparent: `00-${traceId}-${spanId}-01`
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
      select event_id, status, trace_context
      from outbox_events
      where event_id = $1
    `,
    [eventId]
  );

  return result.rows[0] ?? null;
}

function normalizeKafkaHeaders(headers = {}) {
  return Object.fromEntries(
    Object.entries(headers).map(([name, value]) => [
      name,
      Buffer.isBuffer(value) ? value.toString("utf8") : String(value)
    ])
  );
}

function assertTraceParent(value, expectedTraceId, label) {
  if (!value || value.split("-")[1] !== expectedTraceId) {
    throw new Error(
      `${label} traceparent does not contain traceId ${expectedTraceId}: ${json(value)}`
    );
  }
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
