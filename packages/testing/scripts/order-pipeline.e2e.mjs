import { Client } from "pg";

const config = {
  gatewayUrl: readEnv("E2E_GATEWAY_URL", "http://localhost:3000"),
  postgres: {
    host: readEnv("E2E_POSTGRES_HOST", readEnv("POSTGRES_HOST", "localhost")),
    port: Number(readEnv("E2E_POSTGRES_PORT", readEnv("POSTGRES_PORT", "5432"))),
    user: readEnv("E2E_POSTGRES_USER", readEnv("POSTGRES_USER", "postgres")),
    password: readEnv(
      "E2E_POSTGRES_PASSWORD",
      readEnv("POSTGRES_PASSWORD", "postgres")
    ),
    database: readEnv(
      "E2E_POSTGRES_DB",
      readEnv("POSTGRES_DB", "kafka_playground")
    )
  },
  timeoutMs: Number(readEnv("E2E_ORDER_PIPELINE_TIMEOUT_MS", "30000")),
  pollIntervalMs: Number(readEnv("E2E_ORDER_PIPELINE_POLL_INTERVAL_MS", "500"))
};

const finalStatuses = new Set([
  "RISK_REJECTED",
  "PAYMENT_AUTHORIZED",
  "PAYMENT_FAILED"
]);

async function main() {
  assertPositiveNumber(config.postgres.port, "E2E_POSTGRES_PORT");
  assertPositiveNumber(config.timeoutMs, "E2E_ORDER_PIPELINE_TIMEOUT_MS");
  assertPositiveNumber(
    config.pollIntervalMs,
    "E2E_ORDER_PIPELINE_POLL_INTERVAL_MS"
  );

  const client = new Client(config.postgres);

  try {
    await connectPostgres(client);
    await assertGatewayIsReachable();

    const order = await createOrder();

    if (!order.id) {
      throw new Error(`Gateway response does not contain order id: ${json(order)}`);
    }

    if (order.status !== "PENDING") {
      throw new Error(
        `Expected newly created order to be PENDING, got ${order.status}: ${json(order)}`
      );
    }

    const finalOrder = await waitForFinalOrderStatus(client, order.id);

    console.log(
      json({
        ok: true,
        flow: "order -> risk -> payment -> order",
        orderId: order.id,
        initialStatus: order.status,
        finalStatus: finalOrder.status,
        updatedAt: finalOrder.updated_at
      })
    );
  } finally {
    await client.end().catch(() => undefined);
  }
}

async function connectPostgres(client) {
  try {
    await client.connect();
  } catch (error) {
    throw new Error(
      [
        "Failed to connect to Postgres for order pipeline e2e test.",
        `Target: ${config.postgres.user}@${config.postgres.host}:${config.postgres.port}/${config.postgres.database}`,
        "Start project infrastructure with `pnpm infra:up`, or override connection settings with E2E_POSTGRES_HOST, E2E_POSTGRES_PORT, E2E_POSTGRES_USER, E2E_POSTGRES_PASSWORD and E2E_POSTGRES_DB."
      ].join("\n"),
      { cause: error }
    );
  }
}

async function assertGatewayIsReachable() {
  const url = new URL("/health", config.gatewayUrl);
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(
      `Gateway health check failed: GET ${url} returned ${response.status}`
    );
  }
}

async function createOrder() {
  const response = await fetch(new URL("/orders", config.gatewayUrl), {
    method: "POST",
    headers: {
      "content-type": "application/json"
    },
    body: JSON.stringify({
      userId: `e2e-user-${Date.now()}`,
      currency: "USD",
      items: [
        {
          productId: "e2e-product-1",
          quantity: 1,
          unitPrice: 100
        }
      ]
    })
  });

  const body = await response.text();

  if (!response.ok) {
    throw new Error(
      `Create order request failed: POST /orders returned ${response.status} ${body}`
    );
  }

  return parseJson(body, "create order response");
}

async function waitForFinalOrderStatus(client, orderId) {
  const startedAt = Date.now();
  let lastOrder = null;

  while (Date.now() - startedAt < config.timeoutMs) {
    const order = await readOrder(client, orderId);

    if (order) {
      lastOrder = order;

      if (finalStatuses.has(order.status)) {
        return order;
      }
    }

    await sleep(config.pollIntervalMs);
  }

  throw new Error(
    `Order ${orderId} did not reach a final status within ${config.timeoutMs}ms. Last row: ${json(lastOrder)}`
  );
}

async function readOrder(client, orderId) {
  const result = await client.query(
    `
      select id, status, updated_at
      from orders
      where id = $1
    `,
    [orderId]
  );

  return result.rows[0] ?? null;
}

function readEnv(name, fallback) {
  const value = process.env[name];
  return value === undefined || value === "" ? fallback : value;
}

function assertPositiveNumber(value, name) {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive number, got ${value}`);
  }
}

function parseJson(value, label) {
  try {
    return JSON.parse(value);
  } catch (error) {
    throw new Error(`Failed to parse ${label} as JSON: ${value}`, {
      cause: error
    });
  }
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function json(value) {
  return JSON.stringify(value, null, 2);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
