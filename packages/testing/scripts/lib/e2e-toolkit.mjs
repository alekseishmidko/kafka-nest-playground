import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { Kafka, logLevel, Partitioners } from "kafkajs";
import { Client } from "pg";
import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { resolve } from "node:path";

const execFileAsync = promisify(execFile);
const repositoryRoot = fileURLToPath(new URL("../../../../", import.meta.url));

/**
 * Общая конфигурация e2e-тестов.
 *
 * Все значения можно переопределить через `E2E_*`, чтобы тесты не были
 * привязаны к конкретным портам локального Docker Compose.
 */
export const e2eConfig = {
  gatewayUrl: readEnv("E2E_GATEWAY_URL", "http://localhost:3000"),
  kafkaBrokers: readEnv("E2E_KAFKA_BROKERS", "localhost:9092").split(","),
  schemaRegistryUrl: readEnv(
    "E2E_SCHEMA_REGISTRY_URL",
    "http://localhost:8081"
  ),
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
  },
  timeoutMs: Number(readEnv("E2E_TIMEOUT_MS", "60000")),
  pollIntervalMs: Number(readEnv("E2E_POLL_INTERVAL_MS", "500")),
  composeProjectDirectory: readEnv(
    "E2E_COMPOSE_PROJECT_DIRECTORY",
    resolve(repositoryRoot, "infrastructure")
  ),
  composeEnvFile: readEnv(
    "E2E_COMPOSE_ENV_FILE",
    resolve(repositoryRoot, "infrastructure/.env")
  ),
  composeFile: readEnv(
    "E2E_COMPOSE_FILE",
    resolve(repositoryRoot, "infrastructure/docker-compose.yml")
  )
};

/**
 * Создаёт и подключает PostgreSQL client.
 *
 * Вызывающий код обязан закрыть client в `finally`.
 */
export async function connectPostgres() {
  const client = new Client(e2eConfig.postgres);

  await client.connect();

  return client;
}

/**
 * Проверяет, что gateway доступен до начала тестового сценария.
 */
export async function assertGatewayIsReachable() {
  const url = new URL("/health", e2eConfig.gatewayUrl);
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Gateway health check failed: GET ${url} returned ${response.status}`);
  }
}

/**
 * Создаёт заказ через публичный HTTP API и возвращает gateway response.
 */
export async function createOrder(overrides = {}) {
  const response = await fetch(new URL("/orders", e2eConfig.gatewayUrl), {
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
      ],
      ...overrides
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

/**
 * Повторяет асинхронную проверку до получения truthy-результата.
 *
 * Возвращаемое значение predicate используется как результат ожидания, поэтому
 * polling можно применять и как `wait`, и как загрузку итоговой DB-записи.
 */
export async function waitFor(predicate, description, options = {}) {
  const timeoutMs = options.timeoutMs ?? e2eConfig.timeoutMs;
  const pollIntervalMs = options.pollIntervalMs ?? e2eConfig.pollIntervalMs;
  const startedAt = Date.now();
  let lastValue;
  let lastError;

  while (Date.now() - startedAt < timeoutMs) {
    try {
      lastValue = await predicate();
      lastError = undefined;

      if (lastValue) {
        return lastValue;
      }
    } catch (error) {
      lastError = error;
    }

    await sleep(pollIntervalMs);
  }

  throw new Error(
    `Timed out waiting for ${description}. Last value: ${json(lastValue)}`,
    lastError ? { cause: lastError } : undefined
  );
}

/**
 * Читает outbox-запись, связанную с конкретным заказом.
 */
export async function readOrderCreatedOutbox(client, orderId) {
  return readOrderOutboxEvent(client, orderId, "OrderCreated");
}

/**
 * Читает последнее outbox-событие заданного типа для конкретного заказа.
 */
export async function readOrderOutboxEvent(client, orderId, eventType) {
  const result = await client.query(
    `
      select
        id,
        topic,
        message_key,
        event_type,
        event_id,
        event,
        status,
        attempts,
        next_attempt_at,
        published_at,
        last_error,
        created_at,
        updated_at
      from outbox_events
      where event_type = $2
        and event->'payload'->>'orderId' = $1
      order by created_at desc
      limit 1
    `,
    [orderId, eventType]
  );

  return result.rows[0] ?? null;
}

/**
 * Создаёт Kafka producer для отправки событий из e2e-тестов.
 */
export async function createKafkaProducer(clientId) {
  const producer = createKafka(clientId).producer({
    createPartitioner: Partitioners.LegacyPartitioner
  });

  await producer.connect();

  return producer;
}

/**
 * Создаёт promise, который завершается после появления подходящего события.
 *
 * Consumer подключается до тестового действия, поэтому тест не зависит от
 * `fromBeginning` и не сканирует всю историю topic-а.
 */
export async function waitForKafkaEvent({
  topic,
  groupId,
  predicate,
  fromBeginning = false,
  timeoutMs = e2eConfig.timeoutMs
}) {
  const registry = createSchemaRegistry();
  const consumer = createKafka(`e2e-consumer-${Date.now()}`).consumer({ groupId });
  let timeout;

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning });

  const eventPromise = new Promise((resolve, reject) => {
    timeout = setTimeout(() => {
      reject(new Error(`Timed out waiting for Kafka event in ${topic}`));
    }, timeoutMs);

    void consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) {
          return;
        }

        const event = await registry.decode(message.value);

        if (await predicate(event, message)) {
          resolve(event);
        }
      }
    }).catch(reject);
  });

  return {
    eventPromise,
    /**
     * Останавливает consumer независимо от результата теста.
     */
    async close() {
      clearTimeout(timeout);
      await consumer.disconnect();
    }
  };
}

/**
 * Кодирует event envelope по последней Avro-схеме subject-а.
 */
export async function encodeEvent(subject, event) {
  const registry = createSchemaRegistry();
  const schemaId = await registry.getLatestSchemaId(subject);

  return registry.encode(schemaId, event);
}

/**
 * Выполняет Docker Compose команду для управляемого failure-теста.
 */
export async function runCompose(...args) {
  return execFileAsync(
    "docker",
    [
      "compose",
      "--project-directory",
      e2eConfig.composeProjectDirectory,
      "--env-file",
      e2eConfig.composeEnvFile,
      "-f",
      e2eConfig.composeFile,
      ...args
    ],
    {
      cwd: repositoryRoot
    }
  );
}

/**
 * Ожидает восстановления Kafka через короткое admin connection.
 */
export async function waitForKafka() {
  await waitFor(
    async () => {
      const admin = createKafka(`e2e-health-${Date.now()}`).admin();

      try {
        await admin.connect();
        await admin.fetchTopicMetadata();
        return true;
      } finally {
        await admin.disconnect().catch(() => undefined);
      }
    },
    "Kafka to become available"
  );
}

/**
 * Ожидает восстановления Schema Registry HTTP API.
 */
export async function waitForSchemaRegistry() {
  await waitFor(async () => {
    const response = await fetch(new URL("/subjects", e2eConfig.schemaRegistryUrl));

    return response.ok;
  }, "Schema Registry to become available");
}

export function json(value) {
  return JSON.stringify(value, null, 2);
}

export function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function createKafka(clientId) {
  return new Kafka({
    clientId,
    brokers: e2eConfig.kafkaBrokers,
    logLevel: logLevel.NOTHING,
    retry: {
      retries: 3
    }
  });
}

function createSchemaRegistry() {
  return new SchemaRegistry({
    host: e2eConfig.schemaRegistryUrl
  });
}

function readEnv(name, fallback) {
  const value = process.env[name];

  return value === undefined || value === "" ? fallback : value;
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
