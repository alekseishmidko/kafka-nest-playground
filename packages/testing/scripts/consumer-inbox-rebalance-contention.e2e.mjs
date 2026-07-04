import kafkaPackage from "../../kafka/dist/index.js";
import { randomUUID } from "node:crypto";
import {
  connectPostgres,
  e2eConfig,
  json
} from "./lib/e2e-toolkit.mjs";

const {
  KafkaIdempotentEventProcessor,
  KafkaInboxBusyError,
  PostgresKafkaInboxStore
} = kafkaPackage;

/**
 * Проверяет конкурирующую доставку одного события во время активной обработки.
 *
 * Kafka rebalance может привести к повторной доставке сообщения, пока прежний
 * consumer ещё держит lease. Durable inbox должен отклонить параллельную
 * обработку через `KafkaInboxBusyError`, а первый consumer должен завершить
 * side effect ровно один раз.
 */
async function main() {
  const eventId = randomUUID();
  const context = createContext(eventId);
  const first = createProcessor("risk-service");
  const second = createProcessor("risk-service");
  const postgres = await connectPostgres();
  let releaseEffect;
  let processing;
  let sideEffects = 0;
  const effectGate = new Promise((resolve) => {
    releaseEffect = resolve;
  });

  try {
    processing = first.processor.process(
      context,
      () => ({
        eventId: stableEventId(eventId),
        decision: "APPROVED"
      }),
      async () => {
        sideEffects += 1;
        await effectGate;
      }
    );

    await waitForInboxStatus(postgres, "risk-service", eventId, "PREPARED");

    const busyError = await captureError(
      second.processor.process(
        context,
        () => {
          throw new Error("parallel prepare must not run while lease is busy");
        },
        async () => {
          sideEffects += 1;
        }
      )
    );

    if (!(busyError instanceof KafkaInboxBusyError)) {
      throw new Error(`Expected KafkaInboxBusyError, got ${busyError}`);
    }

    releaseEffect();
    await processing;

    const row = await readInboxRow(postgres, "risk-service", eventId);

    if (sideEffects !== 1 || row?.status !== "COMPLETED") {
      throw new Error(
        `Unexpected rebalance contention state: ${json({ sideEffects, row })}`
      );
    }

    console.log(
      json({
        ok: true,
        scenario: "rebalance-style duplicate delivery during processing",
        eventId,
        sideEffects,
        busyError: busyError.name,
        inbox: row
      })
    );
  } finally {
    releaseEffect?.();
    await processing?.catch(() => undefined);
    await processingSettled(first, second);
    await cleanupInbox(postgres, "risk-service", eventId);
    await postgres.end().catch(() => undefined);
  }
}

function createProcessor(serviceName) {
  const store = new PostgresKafkaInboxStore(e2eConfig.postgres);
  const logger = {
    setContext() {},
    info() {},
    warn() {}
  };

  return {
    processor: new KafkaIdempotentEventProcessor(
      {
        clientId: `${serviceName}-rebalance-contention-e2e`,
        serviceName,
        brokers: e2eConfig.kafkaBrokers,
        schemaRegistryUrl: e2eConfig.schemaRegistryUrl
      },
      store,
      logger
    )
  };
}

function createContext(eventId) {
  return {
    topic: "order.order-events",
    partition: 0,
    offset: "300",
    key: "e2e-order",
    headers: {},
    correlationId: randomUUID(),
    event: {
      eventId,
      eventType: "OrderCreated",
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      correlationId: randomUUID(),
      causationId: null,
      producer: "consumer-inbox-rebalance-contention-e2e",
      payload: {}
    }
  };
}

async function waitForInboxStatus(postgres, consumerName, eventId, status) {
  const startedAt = Date.now();

  while (Date.now() - startedAt < 10000) {
    const row = await readInboxRow(postgres, consumerName, eventId);

    if (row?.status === status) {
      return row;
    }

    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  throw new Error(`Timed out waiting for inbox ${eventId} status ${status}`);
}

async function readInboxRow(postgres, consumerName, eventId) {
  const result = await postgres.query(
    `
      select status, attempts
      from kafka_consumer_inbox
      where consumer_name = $1 and event_id = $2
    `,
    [consumerName, eventId]
  );

  return result.rows[0] ?? null;
}

async function cleanupInbox(postgres, consumerName, eventId) {
  await postgres
    .query(
      `
        delete from kafka_consumer_inbox
        where consumer_name = $1 and event_id = $2
      `,
      [consumerName, eventId]
    )
    .catch(() => undefined);
}

async function captureError(promise) {
  try {
    await promise;
  } catch (error) {
    return error;
  }

  return null;
}

async function processingSettled(...processors) {
  for (const { processor } of processors) {
    await processor.onApplicationShutdown().catch(() => undefined);
  }
}

function stableEventId(value) {
  return `00000000-0000-4000-8000-${value.replaceAll("-", "").slice(0, 12)}`;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
