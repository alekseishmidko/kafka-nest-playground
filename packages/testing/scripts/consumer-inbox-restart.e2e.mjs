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
 * Проверяет сохранение идемпотентности между двумя экземплярами процесса.
 *
 * Первый processor завершает событие и закрывает PostgreSQL pool. Второй
 * processor создаётся с новым store, то есть не имеет общей памяти с первым.
 * Единственным источником истины остаётся таблица `kafka_consumer_inbox`.
 */
async function main() {
  const eventId = randomUUID();
  const context = createContext(eventId);
  let sideEffects = 0;
  const first = createProcessor("risk-service");
  const second = createProcessor("risk-service");
  const postgres = await connectPostgres();

  try {
    const initial = await first.processor.process(
      context,
      () => ({
        eventId: randomUUID(),
        decision: "APPROVED"
      }),
      async () => {
        sideEffects += 1;
      }
    );
    await first.processor.onApplicationShutdown();

    const replay = await second.processor.process(
      context,
      () => {
        throw new Error("prepare must not run after process restart");
      },
      async () => {
        sideEffects += 1;
      }
    );
    const row = await postgres.query(
      `
        select status, attempts
        from kafka_consumer_inbox
        where consumer_name = $1 and event_id = $2
      `,
      ["risk-service", eventId]
    );

    if (initial.duplicate || !replay.duplicate || sideEffects !== 1) {
      throw new Error(
        `Inbox restart assertion failed: ${json({
          initial,
          replay,
          sideEffects
        })}`
      );
    }

    if (
      row.rows[0]?.status !== "COMPLETED" ||
      row.rows[0]?.attempts !== 1
    ) {
      throw new Error(`Unexpected inbox row: ${json(row.rows[0])}`);
    }

    console.log(
      json({
        ok: true,
        scenario: "consumer inbox survives process restart",
        eventId,
        sideEffects,
        inbox: row.rows[0]
      })
    );
  } finally {
    await second.processor.onApplicationShutdown().catch(() => undefined);
    await postgres
      .query(
        `
          delete from kafka_consumer_inbox
          where consumer_name = $1 and event_id = $2
        `,
        ["risk-service", eventId]
      )
      .catch(() => undefined);
    await postgres.end().catch(() => undefined);
  }
}

function createProcessor(serviceName) {
  const store = new PostgresKafkaInboxStore(e2eConfig.postgres);
  const logger = {
    setContext() {},
    info() {}
  };

  return {
    processor: new KafkaIdempotentEventProcessor(
      {
        clientId: `${serviceName}-e2e`,
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
    offset: "100",
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
      producer: "consumer-inbox-e2e",
      payload: {}
    }
  };
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
