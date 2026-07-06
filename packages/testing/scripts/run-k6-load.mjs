import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const repositoryRoot = fileURLToPath(new URL("../../../", import.meta.url));
const scriptPath = resolve(
  repositoryRoot,
  "packages/testing/load/order-pipeline.k6.js"
);
const profile = process.argv[2] ?? "smoke";

/**
 * Запускает k6 load test с понятной ошибкой, если k6 не установлен.
 *
 * k6 оставлен внешним CLI-инструментом, а не npm-зависимостью: он часто
 * ставится через brew/apt/docker и используется командами performance/ops
 * независимо от Node.js dependency graph проекта.
 */
async function main() {
  if (!existsSync(scriptPath)) {
    throw new Error(`k6 script does not exist: ${scriptPath}`);
  }

  await assertK6Exists();
  await runK6(profile);
}

function assertK6Exists() {
  return new Promise((resolvePromise, reject) => {
    const child = spawn("k6", ["version"], {
      stdio: "ignore"
    });

    child.on("error", () => {
      reject(
        new Error(
          [
            "k6 executable was not found.",
            "Install it first:",
            "  macOS: brew install k6",
            "  Linux: https://grafana.com/docs/k6/latest/set-up/install-k6/",
            "Then run the load test again."
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

function runK6(selectedProfile) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(
      "k6",
      [
        "run",
        "--env",
        `LOAD_PROFILE=${selectedProfile}`,
        scriptPath
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

      reject(new Error(`k6 load test failed with exit code ${code}`));
    });
  });
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
