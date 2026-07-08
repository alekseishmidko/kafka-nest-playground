import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const gatewayUrl = readEnv("LOAD_GATEWAY_URL", "http://localhost:3000");
const profile = readEnv("LOAD_PROFILE", "smoke");

export const options = buildOptions(profile);

const createOrderErrors = new Rate("order_create_errors");
const createOrderLatency = new Trend("order_create_latency", true);
const ordersCreated = new Counter("orders_created");

/**
 * Нагрузочный сценарий основного order pipeline.
 *
 * k6 создаёт заказы через публичный gateway `POST /orders`. Этот тест измеряет
 * только синхронную часть API: HTTP -> gateway -> gRPC -> order-service ->
 * PostgreSQL transaction -> outbox insert. Асинхронную часть pipeline нужно
 * смотреть в Grafana/Prometheus: outbox backlog, Kafka consumer lag, retry rate
 * и DLQ.
 */
export default function orderPipelineLoadScenario() {
  const response = http.post(
    `${gatewayUrl}/orders`,
    JSON.stringify(createOrderPayload()),
    {
      headers: {
        "content-type": "application/json"
      },
      tags: {
        endpoint: "POST /orders"
      }
    }
  );

  createOrderLatency.add(response.timings.duration);

  const ok = check(response, {
    "POST /orders returns 2xx": (value) =>
      value.status >= 200 && value.status < 300,
    "POST /orders returns order id": (value) =>
      Boolean(parseJson(value.body)?.id)
  });

  createOrderErrors.add(!ok);

  if (ok) {
    ordersCreated.add(1);
  }

  sleep(Number(readEnv("LOAD_SLEEP_SECONDS", "0.2")));
}

function buildOptions(selectedProfile) {
  const thresholds = {
    http_req_failed: [
      `rate<${readEnv("LOAD_HTTP_ERROR_RATE_THRESHOLD", "0.01")}`
    ],
    "http_req_duration{endpoint:POST /orders}": [
      `p(95)<${readEnv("LOAD_HTTP_P95_THRESHOLD_MS", "500")}`,
      `p(99)<${readEnv("LOAD_HTTP_P99_THRESHOLD_MS", "1000")}`
    ],
    order_create_errors: [
      `rate<${readEnv("LOAD_ORDER_ERROR_RATE_THRESHOLD", "0.01")}`
    ]
  };

  if (selectedProfile === "smoke") {
    return {
      scenarios: {
        smoke: {
          executor: "constant-vus",
          vus: Number(readEnv("LOAD_SMOKE_VUS", "2")),
          duration: readEnv("LOAD_SMOKE_DURATION", "30s")
        }
      },
      thresholds
    };
  }

  if (selectedProfile === "stress") {
    return {
      scenarios: {
        stress: {
          executor: "ramping-arrival-rate",
          timeUnit: "1s",
          preAllocatedVUs: Number(readEnv("LOAD_PRE_ALLOCATED_VUS", "100")),
          maxVUs: Number(readEnv("LOAD_MAX_VUS", "500")),
          stages: [
            { target: Number(readEnv("LOAD_STAGE_1_RPS", "25")), duration: "2m" },
            { target: Number(readEnv("LOAD_STAGE_2_RPS", "50")), duration: "2m" },
            { target: Number(readEnv("LOAD_STAGE_3_RPS", "100")), duration: "2m" },
            { target: Number(readEnv("LOAD_STAGE_4_RPS", "200")), duration: "2m" },
            { target: 0, duration: "1m" }
          ]
        }
      },
      thresholds
    };
  }

  if (selectedProfile === "baseline") {
    return {
      scenarios: {
        baseline: {
          executor: "constant-arrival-rate",
          rate: Number(readEnv("LOAD_BASELINE_RPS", "25")),
          timeUnit: "1s",
          duration: readEnv("LOAD_BASELINE_DURATION", "5m"),
          preAllocatedVUs: Number(readEnv("LOAD_PRE_ALLOCATED_VUS", "50")),
          maxVUs: Number(readEnv("LOAD_MAX_VUS", "200"))
        }
      },
      thresholds
    };
  }

  return {
    scenarios: {
      load: {
        executor: "ramping-arrival-rate",
        timeUnit: "1s",
        preAllocatedVUs: Number(readEnv("LOAD_PRE_ALLOCATED_VUS", "50")),
        maxVUs: Number(readEnv("LOAD_MAX_VUS", "200")),
        stages: [
          { target: Number(readEnv("LOAD_STAGE_1_RPS", "10")), duration: "1m" },
          { target: Number(readEnv("LOAD_STAGE_2_RPS", "25")), duration: "2m" },
          { target: Number(readEnv("LOAD_STAGE_3_RPS", "50")), duration: "2m" },
          { target: Number(readEnv("LOAD_STAGE_4_RPS", "100")), duration: "2m" },
          { target: 0, duration: "30s" }
        ]
      }
    },
    thresholds
  };
}

function createOrderPayload() {
  const now = Date.now();
  const sequence = `${__VU}-${__ITER}-${now}`;

  return {
    userId: `load-user-${sequence}`,
    currency: "USD",
    items: [
      {
        productId: `load-product-${__VU % 20}`,
        quantity: 1 + (__ITER % 3),
        unitPrice: 50 + (__ITER % 100)
      }
    ]
  };
}

function parseJson(value) {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function readEnv(name, fallback) {
  return __ENV[name] === undefined || __ENV[name] === "" ? fallback : __ENV[name];
}
