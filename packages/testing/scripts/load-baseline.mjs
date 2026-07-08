import { spawn } from "node:child_process";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, relative as pathRelative, resolve } from "node:path";
import { Client } from "pg";

const repositoryRoot = fileURLToPath(new URL("../../../", import.meta.url));
const k6ScriptPath = resolve(
  repositoryRoot,
  "packages/testing/load/order-pipeline.k6.js"
);
const reportsRoot = resolve(
  repositoryRoot,
  "packages/testing/reports/load-baseline"
);
const baselinePath = resolve(
  repositoryRoot,
  "packages/testing/baselines/order-pipeline.local.json"
);
const timestamp = new Date().toISOString().replaceAll(":", "-");
const reportDirectory = resolve(reportsRoot, timestamp);
const k6SummaryPath = resolve(reportDirectory, "k6-summary.json");
const baselineReportPath = resolve(reportDirectory, "baseline.json");
const mode = process.argv[2] ?? "capture";

const config = {
  gatewayUrl: readEnv("LOAD_GATEWAY_URL", "http://localhost:3000"),
  prometheusUrl: readEnv("LOAD_PROMETHEUS_URL", "http://localhost:9090"),
  profile: readEnv("LOAD_PROFILE", "baseline"),
  postgres: {
    host: readEnv("E2E_POSTGRES_HOST", readEnv("POSTGRES_HOST", "localhost")),
    port: Number(readEnv("E2E_POSTGRES_PORT", readEnv("POSTGRES_PORT", "55432"))),
    user: readEnv("E2E_POSTGRES_USER", readEnv("POSTGRES_USER", "postgres")),
    password: readEnv(
      "E2E_POSTGRES_PASSWORD",
      readEnv("POSTGRES_PASSWORD", "postgres")
    ),
    database: readEnv(
      "E2E_POSTGRES_DB",
      readEnv("POSTGRES_DB", "kafka_playground")
    )
  }
};

/**
 * Captures a reproducible local load baseline.
 *
 * Baseline is not only a k6 result. It combines client-side API metrics with
 * operational metrics from Prometheus and PostgreSQL, so later optimizations can
 * be compared against throughput, latency, lag, resource pressure and DB usage.
 */
async function main() {
  if (!["capture", "compare"].includes(mode)) {
    throw new Error("Usage: node scripts/load-baseline.mjs capture|compare");
  }

  if (!existsSync(k6ScriptPath)) {
    throw new Error(`k6 script does not exist: ${k6ScriptPath}`);
  }

  const previousBaseline = mode === "compare" ? await readExistingBaseline() : null;
  await mkdir(reportDirectory, { recursive: true });
  await assertK6Exists();
  await assertHttpOk(new URL("/health", config.gatewayUrl), "Gateway");
  await assertHttpOk(new URL("/-/ready", config.prometheusUrl), "Prometheus");

  const postgresSampler = createPostgresSampler();
  await postgresSampler.start();

  const startedAt = new Date();
  try {
    await runK6();
  } finally {
    await postgresSampler.stop();
  }
  const finishedAt = new Date();

  const [k6Summary, prometheusSnapshot] = await Promise.all([
    readK6Summary(),
    collectPrometheusSnapshot()
  ]);

  const baseline = {
    capturedAt: finishedAt.toISOString(),
    scenario: "order-pipeline",
    profile: config.profile,
    environment: {
      gatewayUrl: config.gatewayUrl,
      prometheusUrl: config.prometheusUrl,
      postgres: {
        host: config.postgres.host,
        port: config.postgres.port,
        database: config.postgres.database
      }
    },
    window: {
      startedAt: startedAt.toISOString(),
      finishedAt: finishedAt.toISOString(),
      durationSeconds: Math.round((finishedAt.getTime() - startedAt.getTime()) / 1000)
    },
    k6: extractK6Metrics(k6Summary),
    kafka: {
      maxConsumerLag: prometheusSnapshot.kafkaConsumerLag
    },
    asyncPipeline: {
      maxOutboxPending: prometheusSnapshot.outboxPending,
      maxDlqNew: prometheusSnapshot.dlqNew
    },
    resources: {
      maxNodeCpuCores: prometheusSnapshot.nodeCpuCores,
      maxNodeResidentMemoryBytes: prometheusSnapshot.nodeResidentMemoryBytes,
      postgresConnections: postgresSampler.snapshot()
    },
    reportFiles: {
      k6Summary: relative(k6SummaryPath),
      fullReport: relative(baselineReportPath)
    }
  };

  await writeJson(baselineReportPath, baseline);
  if (mode === "capture") {
    await writeJson(baselinePath, baseline);
  }

  printSummary(baseline, previousBaseline);
}

function createPostgresSampler() {
  const state = {
    client: null,
    timer: null,
    samples: [],
    lastError: null
  };

  return {
    async start() {
      state.client = new Client(config.postgres);
      await state.client.connect();
      await samplePostgresConnections(state);
      state.timer = setInterval(() => {
        void samplePostgresConnections(state);
      }, Number(readEnv("LOAD_POSTGRES_SAMPLE_INTERVAL_MS", "5000")));
    },
    async stop() {
      if (state.timer) {
        clearInterval(state.timer);
        state.timer = null;
      }
      if (state.client) {
        await state.client.end().catch(() => undefined);
        state.client = null;
      }
    },
    snapshot() {
      const totals = state.samples.map((sample) => sample.total);
      const active = state.samples.map((sample) => sample.active);
      const idle = state.samples.map((sample) => sample.idle);

      return {
        samples: state.samples.length,
        maxTotal: max(totals),
        maxActive: max(active),
        maxIdle: max(idle),
        lastError: state.lastError
      };
    }
  };
}

async function samplePostgresConnections(state) {
  try {
    const result = await state.client.query(`
      select
        count(*)::int as total,
        count(*) filter (where state = 'active')::int as active,
        count(*) filter (where state = 'idle')::int as idle
      from pg_stat_activity
      where datname = current_database()
    `);

    state.samples.push({
      capturedAt: new Date().toISOString(),
      total: Number(result.rows[0].total),
      active: Number(result.rows[0].active),
      idle: Number(result.rows[0].idle)
    });
    state.lastError = null;
  } catch (error) {
    state.lastError = error instanceof Error ? error.message : String(error);
  }
}

function assertK6Exists() {
  return new Promise((resolvePromise, reject) => {
    const child = spawn("k6", ["version"], { stdio: "ignore" });

    child.on("error", () => {
      reject(
        new Error(
          [
            "k6 executable was not found.",
            "Install it first:",
            "  macOS: brew install k6",
            "  Linux: https://grafana.com/docs/k6/latest/set-up/install-k6/"
          ].join("\n")
        )
      );
    });
    child.on("exit", (code) => {
      if (code === 0) {
        resolvePromise();
        return;
      }

      reject(new Error(`k6 version check failed with exit code ${code}`));
    });
  });
}

async function assertHttpOk(url, name) {
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`${name} is not ready: GET ${url} returned ${response.status}`);
  }
}

function runK6() {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(
      "k6",
      [
        "run",
        "--env",
        `LOAD_PROFILE=${config.profile}`,
        "--summary-export",
        k6SummaryPath,
        k6ScriptPath
      ],
      {
        stdio: "inherit",
        env: process.env
      }
    );

    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolvePromise();
        return;
      }

      reject(new Error(`k6 baseline run failed with exit code ${code}`));
    });
  });
}

async function readK6Summary() {
  return JSON.parse(await readFile(k6SummaryPath, "utf8"));
}

async function collectPrometheusSnapshot() {
  const queries = {
    kafkaConsumerLag: "max(max_over_time(kafka_consumer_lag[10m]))",
    outboxPending: "max(max_over_time(outbox_pending_events[10m]))",
    dlqNew: "max(max_over_time(dlq_new_events[10m]))",
    nodeCpuCores:
      "max(rate(nodejs_process_cpu_user_seconds_total[1m]) + rate(nodejs_process_cpu_system_seconds_total[1m]))",
    nodeResidentMemoryBytes:
      "max(max_over_time(nodejs_process_resident_memory_bytes[10m]))"
  };
  const entries = await Promise.all(
    Object.entries(queries).map(async ([name, query]) => [
      name,
      await queryPrometheus(query)
    ])
  );

  return Object.fromEntries(entries);
}

async function queryPrometheus(query) {
  const url = new URL("/api/v1/query", config.prometheusUrl);
  url.searchParams.set("query", query);

  const response = await fetch(url);
  if (!response.ok) {
    return {
      value: null,
      warning: `Prometheus query failed with HTTP ${response.status}`,
      query
    };
  }

  const body = await response.json();
  const firstResult = body.data?.result?.[0];
  const value = firstResult?.value?.[1];

  return {
    value: value === undefined ? null : Number(value),
    query
  };
}

function extractK6Metrics(summary) {
  const metrics = summary.metrics ?? {};

  return {
    rps: readMetric(metrics, "http_reqs", "rate"),
    requests: readMetric(metrics, "http_reqs", "count"),
    p95LatencyMs: readMetric(metrics, "http_req_duration", "p(95)"),
    p99LatencyMs: readMetric(metrics, "http_req_duration", "p(99)"),
    avgLatencyMs: readMetric(metrics, "http_req_duration", "avg"),
    errorRate: readMetric(metrics, "http_req_failed", "rate"),
    orderCreateErrorRate: readMetric(metrics, "order_create_errors", "rate"),
    ordersCreated: readMetric(metrics, "orders_created", "count")
  };
}

function readMetric(metrics, metricName, valueName) {
  const value = metrics[metricName]?.values?.[valueName];
  return value === undefined ? null : Number(value);
}

async function writeJson(path, value) {
  await mkdir(dirname(path), { recursive: true });
  const temporaryPath = `${path}.tmp`;
  await writeFile(temporaryPath, `${JSON.stringify(value, null, 2)}\n`);
  await rename(temporaryPath, path);
}

async function readExistingBaseline() {
  if (!existsSync(baselinePath)) {
    throw new Error(
      `Baseline does not exist: ${relative(baselinePath)}. Run pnpm test:load:baseline first.`
    );
  }

  return JSON.parse(await readFile(baselinePath, "utf8"));
}

function printSummary(baseline, previousBaseline) {
  console.log("");
  console.log(
    mode === "capture" ? "Load baseline captured" : "Load baseline compared"
  );
  console.log(`- RPS: ${format(baseline.k6.rps)}`);
  console.log(`- p95 latency: ${format(baseline.k6.p95LatencyMs)} ms`);
  console.log(`- p99 latency: ${format(baseline.k6.p99LatencyMs)} ms`);
  console.log(`- error rate: ${format(baseline.k6.errorRate)}`);
  console.log(`- Kafka max lag: ${format(baseline.kafka.maxConsumerLag.value)}`);
  console.log(
    `- Postgres max connections: ${format(
      baseline.resources.postgresConnections.maxTotal
    )}`
  );
  if (previousBaseline) {
    console.log("");
    console.log("Delta vs saved baseline");
    printDelta("RPS", previousBaseline.k6?.rps, baseline.k6.rps);
    printDelta(
      "p95 latency ms",
      previousBaseline.k6?.p95LatencyMs,
      baseline.k6.p95LatencyMs
    );
    printDelta(
      "p99 latency ms",
      previousBaseline.k6?.p99LatencyMs,
      baseline.k6.p99LatencyMs
    );
    printDelta("error rate", previousBaseline.k6?.errorRate, baseline.k6.errorRate);
    printDelta(
      "Kafka max lag",
      previousBaseline.kafka?.maxConsumerLag?.value,
      baseline.kafka.maxConsumerLag.value
    );
    printDelta(
      "Postgres max connections",
      previousBaseline.resources?.postgresConnections?.maxTotal,
      baseline.resources.postgresConnections.maxTotal
    );
  }

  if (mode === "capture") {
    console.log(`- Baseline: ${relative(baselinePath)}`);
  } else {
    console.log(`- Saved baseline: ${relative(baselinePath)}`);
  }
  console.log(`- Full report: ${relative(baselineReportPath)}`);
}

function printDelta(label, before, after) {
  if (!Number.isFinite(before) || !Number.isFinite(after)) {
    console.log(`- ${label}: n/a`);
    return;
  }

  const absolute = after - before;
  const percent = before === 0 ? null : (absolute / before) * 100;
  const sign = absolute > 0 ? "+" : "";
  const percentText = percent === null ? "" : ` (${sign}${percent.toFixed(2)}%)`;

  console.log(
    `- ${label}: ${format(before)} -> ${format(after)} ${sign}${absolute.toFixed(4)}${percentText}`
  );
}

function max(values) {
  const filtered = values.filter((value) => Number.isFinite(value));
  return filtered.length === 0 ? null : Math.max(...filtered);
}

function format(value) {
  return value === null || value === undefined ? "n/a" : String(value);
}

function relative(path) {
  return pathRelative(repositoryRoot, path);
}

function readEnv(name, fallback) {
  return process.env[name] === undefined || process.env[name] === ""
    ? fallback
    : process.env[name];
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
