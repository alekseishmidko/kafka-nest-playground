import { spawn } from "node:child_process";

const steps = [
  {
    title: "Install workspace dependencies",
    command: "pnpm",
    args: ["install"]
  },
  {
    title: "Start Docker infrastructure",
    command: "pnpm",
    args: ["infra:up"]
  },
  {
    title: "Register Avro schemas",
    command: "pnpm",
    args: ["contracts:schemas:register"]
  },
  {
    title: "Run order-service migrations",
    command: "pnpm",
    args: ["--filter", "order-service", "migration:run"]
  }
];

/**
 * Подготавливает локальный стенд одной командой.
 *
 * Этот сценарий намеренно собирает ручные шаги из README в один repeatable
 * bootstrap: зависимости, Docker-инфраструктура, Schema Registry и миграции.
 */
async function main() {
  for (const step of steps) {
    await runStep(step);
  }

  console.log("");
  console.log("Local setup is ready.");
  console.log("Run `pnpm verify:local` to start services and execute the e2e gate.");
}

function runStep(step) {
  return new Promise((resolve, reject) => {
    console.log("");
    console.log(`==> ${step.title}`);

    const child = spawn(step.command, step.args, {
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
          `${step.title} failed: ${step.command} ${step.args.join(" ")} exited with ${code}`
        )
      );
    });
  });
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
