import { existsSync, readFileSync } from "node:fs";

export type RuntimeEnvironment = "local" | "prod";

export function getRuntimeEnvironment(value = process.env.APP_ENV): RuntimeEnvironment {
  return value === "prod" ? "prod" : "local";
}

export function getServiceEnvFilePaths(
  serviceRoot: string,
  runtimeEnvironment = getRuntimeEnvironment()
): string[] {
  return [
    `${serviceRoot}/.env.${runtimeEnvironment}`,
    `${serviceRoot}/.env`
  ];
}

export function loadServiceEnvFiles(
  serviceRoot: string,
  runtimeEnvironment = getRuntimeEnvironment()
): void {
  const existingEnvironmentKeys = new Set(Object.keys(process.env));

  for (const filePath of getServiceEnvFilePaths(serviceRoot, runtimeEnvironment).reverse()) {
    if (!existsSync(filePath)) {
      continue;
    }

    const content = readFileSync(filePath, "utf8");

    for (const line of content.split(/\r?\n/)) {
      const trimmed = line.trim();

      if (!trimmed || trimmed.startsWith("#")) {
        continue;
      }

      const separatorIndex = trimmed.indexOf("=");

      if (separatorIndex === -1) {
        continue;
      }

      const key = trimmed.slice(0, separatorIndex).trim();
      const value = trimmed.slice(separatorIndex + 1).trim();

      if (!existingEnvironmentKeys.has(key)) {
        process.env[key] = value;
      }
    }
  }
}
