import kafkaPackage from "../../kafka/dist/index.js";
import { randomUUID } from "node:crypto";
import {
  connectPostgres,
  e2eConfig,
  json
} from "./lib/e2e-toolkit.mjs";

const {
  KafkaIdempotentEventProcessor,
  PostgresKafkaInboxStore
} = kafkaPackage;

/**
 * Проверяет crash window между external side effect и `markCompleted`.
 *
 * Первый запуск сохраняет PREPARED result, выполняет effect и падает до
 * `markCompleted`. Второй запуск не должен снова выполнять prepare; он берёт
 * сохранённый result из `kafka_consumer_inbox` и повторяет effect с тем же
 * eventId. Это моделирует "publish happened, process crashed before marking
 * inbox completed" без реального убийства Node.js процесса.
 */
async function main() {
  const eventId = randomUUID();
  const context = createContext(eventId);
  const first = createProcessor("payment-service");
  const second = createProcessor("payment-service");
  const postgres = await connectPostgres();
  const publishedEventIds = [];
  let prepareRuns = 0;

  try {
    await assertRejects(
      first.processor.process(
        context,
        () => {
          prepareRuns += 1;
          return createPreparedResult(eventId);
        },
        async (result) => {
          publishedEventIds.push(result.eventId);
          throw new Error("synthetic crash after publish before markCompleted");
        }
      )
    );
    await first.processor.onApplicationShutdown();

    await second.processor.process(
      context,
      () => {
        throw new Error("prepare must not run after crash recovery");
      },
      async (result) => {
        publishedEventIds.push(result.eventId);
      }
    );

    const row = await readInboxRow(postgres, "payment-service", eventId);

    if (prepareRuns !== 1) {
      throw new Error(`Expected prepare to run once, got ${prepareRuns}`);
    }

    if (
      publishedEventIds.length !== 2 ||
      publishedEventIds[0] !== publishedEventIds[1]
    ) {
      throw new Error(
        `Expected duplicate effect to reuse the same event id: ${json(publishedEventIds)}`
      );
    }

    if (row?.status !== "COMPLETED" || row?.attempts < 2) {
      throw new Error(`Unexpected inbox row after recovery: ${json(row)}`);
    }

    console.log(
      json({
        ok: true,
        scenario: "crash after effect before markCompleted",
        inputEventId: eventId,
        preparedEventId: publishedEventIds[0],
        publishedAttempts: publishedEventIds.length,
        inbox: row
      })
    );
  } finally {
    await first.processor.onApplicationShutdown().catch(() => undefined);
    await second.processor.onApplicationShutdown().catch(() => undefined);
    await cleanupInbox(postgres, "payment-service", eventId);
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
        clientId: `${serviceName}-crash-window-e2e`,
        serviceName,
        brokers: e2eConfig.kafkaBrokers,
        schemaRegistryUrl: e2eConfig.schemaRegistryUrl
      },
      store,
      logger
    )
  };
}

function createPreparedResult(sourceEventId) {
  return {
    eventId: stableEventId(sourceEventId),
    eventType: "PaymentAuthorized",
    payload: {
      orderId: "e2e-order",
      paymentId: randomUUID()
    }
  };
}

function stableEventId(value) {
  return `00000000-0000-4000-8000-${value.replaceAll("-", "").slice(0, 12)}`;
}

function createContext(eventId) {
  return {
    topic: "risk.risk-events",
    partition: 0,
    offset: "200",
    key: "e2e-order",
    headers: {},
    correlationId: randomUUID(),
    event: {
      eventId,
      eventType: "OrderRiskApproved",
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      correlationId: randomUUID(),
      causationId: randomUUID(),
      producer: "consumer-inbox-crash-window-e2e",
      payload: {}
    }
  };
}

async function readInboxRow(postgres, consumerName, eventId) {
  const result = await postgres.query(
    `
      select status, attempts, result
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

async function assertRejects(promise) {
  try {
    await promise;
  } catch {
    return;
  }

  throw new Error("Expected operation to fail");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
