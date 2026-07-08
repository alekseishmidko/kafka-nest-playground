import { spawn } from "node:child_process";
import { mkdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const repositoryRoot = fileURLToPath(new URL("../../../", import.meta.url));
const logDirectory = resolve(repositoryRoot, "packages/testing/reports/verify-local");
const startedProcesses = [];

const services = [
  {
    name: "order-service",
    command: "pnpm",
    args: ["dev:order"],
    readyUrl: "http://localhost:3003/metrics"
  },
  {
    name: "risk-service",
    command: "pnpm",
    args: ["dev:risk"],
    readyUrl: "http://localhost:3004/metrics"
  },
  {
    name: "payment-service",
    command: "pnpm",
    args: ["dev:payment"],
    readyUrl: "http://localhost:3005/metrics"
  },
  {
    name: "notification-service",
    command: "pnpm",
    args: ["dev:notification"],
    readyUrl: "http://localhost:3006/metrics"
  },
  {
    name: "gateway-service",
    command: "pnpm",
    args: ["dev:gateway"],
    readyUrl: "http://localhost:3000/health"
  }
];

const checks = [
  {
    title: "Build contracts",
    command: "pnpm",
    args: ["--filter", "@kafka-playground/contracts", "build"]
  },
  {
    title: "Build outbox package",
    command: "pnpm",
    args: ["--filter", "@kafka-playground/outbox", "build"]
  },
  {
    title: "Run production e2e gate",
    command: "pnpm",
    args: ["test:e2e:gate"]
  }
];

/**
 * Запускает полный локальный verification flow.
 *
 * Сервисы стартуют как дочерние dev-процессы, после чего сценарий ждёт их
 * technical endpoints и прогоняет production gate. Это снижает риск ситуации,
 * когда новый разработчик запускает проверки на частично поднятом стенде.
 */
async function main() {
  await mkdir(logDirectory, { recursive: true });

  for (const service of services) {
    startService(service);
  }

  try {
    await waitForServices();
    for (const check of checks) {
      await runForeground(check);
    }
  } finally {
    await stopServices();
  }

  console.log("");
  console.log("Local verification passed.");
}

function startService(service) {
  console.log(`Starting ${service.name}...`);
  const child = spawn(service.command, service.args, {
    detached: true,
    stdio: ["ignore", "pipe", "pipe"],
    env: process.env
  });

  child.stdout.on("data", (chunk) => {
    process.stdout.write(`[${service.name}] ${chunk}`);
  });
  child.stderr.on("data", (chunk) => {
    process.stderr.write(`[${service.name}] ${chunk}`);
  });
  child.on("exit", (code, signal) => {
    if (!child.killed && code !== null && code !== 0) {
      process.stderr.write(
        `[${service.name}] exited unexpectedly with code ${code} signal ${signal}\n`
      );
    }
  });

  startedProcesses.push({ service, child });
}

async function waitForServices() {
  await Promise.all(
    services.map((service) =>
      waitForHttpOk(service.readyUrl, `${service.name} readiness`)
    )
  );
}

async function waitForHttpOk(url, description) {
  const timeoutMs = Number(readEnv("VERIFY_LOCAL_READY_TIMEOUT_MS", "120000"));
  const pollIntervalMs = Number(readEnv("VERIFY_LOCAL_POLL_INTERVAL_MS", "1000"));
  const startedAt = Date.now();
  let lastError = "";

  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        console.log(`${description} is ready: ${url}`);
        return;
      }
      lastError = `HTTP ${response.status}`;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }

    await sleep(pollIntervalMs);
  }

  throw new Error(`Timed out waiting for ${description}: ${url}. Last error: ${lastError}`);
}

function runForeground(check) {
  return new Promise((resolve, reject) => {
    console.log("");
    console.log(`==> ${check.title}`);

    const child = spawn(check.command, check.args, {
      stdio: "inherit",
      env: process.env
    });

    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(
        new Error(
          `${check.title} failed: ${check.command} ${check.args.join(" ")} exited with ${code}`
        )
      );
    });
  });
}

async function stopServices() {
  for (const { service, child } of startedProcesses.reverse()) {
    if (child.exitCode !== null) {
      continue;
    }

    console.log(`Stopping ${service.name}...`);
    try {
      process.kill(-child.pid, "SIGTERM");
    } catch {
      child.kill("SIGTERM");
    }
  }

  await sleep(2000);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readEnv(name, fallback) {
  return process.env[name] === undefined || process.env[name] === ""
    ? fallback
    : process.env[name];
}

main().catch(async (error) => {
  console.error(error.message);
  await stopServices();
  process.exitCode = 1;
});
