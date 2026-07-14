import {
  connectPostgres,
  e2eConfig,
  json,
  waitFor
} from "./lib/e2e-toolkit.mjs";

const VIEWER_API_KEY =
  process.env.E2E_DLQ_VIEWER_API_KEY ?? "local-dlq-viewer-key";
const OPERATOR_API_KEY =
  process.env.E2E_DLQ_OPERATOR_API_KEY ?? "local-dlq-operator-key";

/**
 * Проверяет, что общий `/admin/*` audit trail фиксирует не только успешные
 * действия, но и security decisions:
 *
 * - 401: запрос без admin API key;
 * - 403: viewer пытается выполнить dangerous action;
 * - 429: валидный operator превышает process-local rate limit.
 *
 * Это production-критично: denied/limited запросы нужны для расследований так
 * же, как allowed admin actions.
 */
async function main() {
  const postgres = await connectPostgres();

  try {
    await assertAdminApiIsReachable();

    const unauthorized = await sendAdminRequest({
      path: "/admin/audit-events?limit=1",
      requestId: uniqueRequestId("unauthorized")
    });
    assertEqual(unauthorized.status, 401, "unauthorized response status");
    await assertAuditDecision(postgres, {
      requestId: unauthorized.requestId,
      statusCode: 401,
      decision: "DENIED",
      actor: null,
      role: null
    });

    const forbidden = await sendAdminRequest({
      path: "/admin/outbox/retry-failed?limit=1",
      method: "POST",
      requestId: uniqueRequestId("forbidden"),
      apiKey: VIEWER_API_KEY
    });
    assertEqual(forbidden.status, 403, "forbidden response status");
    await assertAuditDecision(postgres, {
      requestId: forbidden.requestId,
      statusCode: 403,
      decision: "DENIED",
      actor: "dlq-viewer",
      role: "ADMIN_VIEWER"
    });

    const limited = await triggerRateLimit();
    await assertAuditDecision(postgres, {
      requestId: limited.requestId,
      statusCode: 429,
      decision: "DENIED",
      actor: "dlq-operator",
      role: "ADMIN_OPERATOR"
    });

    console.log(
      json({
        scenario: "admin audit security decisions",
        unauthorized: {
          requestId: unauthorized.requestId,
          status: unauthorized.status
        },
        forbidden: {
          requestId: forbidden.requestId,
          status: forbidden.status
        },
        rateLimited: {
          requestId: limited.requestId,
          status: limited.status
        }
      })
    );
  } finally {
    await postgres.end();
  }
}

async function assertAdminApiIsReachable() {
  const response = await fetch(
    new URL("/admin/audit-events?limit=1", e2eConfig.orderAdminUrl),
    {
      headers: {
        "x-admin-api-key": VIEWER_API_KEY,
        "x-request-id": uniqueRequestId("readiness")
      }
    }
  );

  if (!response.ok) {
    throw new Error(
      `Order Admin API is unavailable: ${response.status} ${await response.text()}`
    );
  }
}

async function triggerRateLimit() {
  for (let attempt = 1; attempt <= 80; attempt += 1) {
    const result = await sendAdminRequest({
      path: "/admin/audit-events?limit=1",
      requestId: uniqueRequestId(`rate-limit-${attempt}`),
      apiKey: OPERATOR_API_KEY
    });

    if (result.status === 429) {
      return result;
    }
  }

  throw new Error("Admin API did not return 429 after 80 requests");
}

async function sendAdminRequest({
  path,
  method = "GET",
  requestId,
  apiKey
}) {
  const headers = {
    "x-request-id": requestId
  };

  if (apiKey) {
    headers["x-admin-api-key"] = apiKey;
  }

  const response = await fetch(new URL(path, e2eConfig.orderAdminUrl), {
    method,
    headers
  });

  return {
    requestId,
    status: response.status,
    body: await response.text()
  };
}

async function assertAuditDecision(
  postgres,
  { requestId, statusCode, decision, actor, role }
) {
  const audit = await waitFor(
    () => readAdminAuditEvent(postgres, requestId),
    `admin audit event for request ${requestId}`
  );

  assertEqual(audit.status_code, statusCode, "audit status_code");
  assertEqual(audit.decision, decision, "audit decision");
  assertEqual(audit.actor, actor, "audit actor");
  assertEqual(audit.role, role, "audit role");

  return audit;
}

async function readAdminAuditEvent(postgres, requestId) {
  const result = await postgres.query(
    `
      select
        actor,
        role,
        method,
        path,
        action,
        entity_type,
        entity_id,
        decision,
        status_code,
        request_id,
        correlation_id
      from admin_audit_events
      where request_id = $1
      order by created_at desc
      limit 1
    `,
    [requestId]
  );

  return result.rows[0] ?? null;
}

function uniqueRequestId(label) {
  return `e2e-admin-audit-${label}-${Date.now()}-${Math.random()
    .toString(16)
    .slice(2)}`;
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
