import outboxPackage from "../../outbox/dist/index.js";
import { randomUUID } from "node:crypto";
import {
  connectPostgres,
  json,
  sleep
} from "./lib/e2e-toolkit.mjs";

const {
  PostgresOutboxStore,
  publishOutboxBatch
} = outboxPackage;

/**
 * Проверяет chaos-сценарий "две реплики publisher-а видят одну outbox-строку".
 *
 * Без lease обе реплики могли бы одновременно прочитать `PENDING` запись и
 * опубликовать один и тот же event два раза. Production-safe поведение:
 * первый publisher атомарно забирает строку через `FOR UPDATE SKIP LOCKED`,
 * второй publisher не получает эту строку и не вызывает transport publish.
 */
async function main() {
  const postgres = await connectPostgres();
  const outboxEvent = createOutboxEvent();
  const publishCalls = [];
  const publisher = {
    async publish(message) {
      publishCalls.push({
        eventId: message.event.eventId,
        topic: message.topic,
        key: message.key
      });
      await sleep(500);
    }
  };
  const first = createPublisherState();
  const second = createPublisherState();

  try {
    await insertOutboxEvent(postgres, outboxEvent);

    await Promise.all([
      publishOutboxBatch({
        store: new PostgresOutboxStore(createRepositoryAdapter(postgres)),
        publisher,
        logger: silentLogger,
        batchSize: 10,
        ownerId: "e2e-outbox-replica-a",
        leaseMs: 30_000,
        isPublishing: () => first.isPublishing,
        setPublishing: (value) => {
          first.isPublishing = value;
        }
      }),
      publishOutboxBatch({
        store: new PostgresOutboxStore(createRepositoryAdapter(postgres)),
        publisher,
        logger: silentLogger,
        batchSize: 10,
        ownerId: "e2e-outbox-replica-b",
        leaseMs: 30_000,
        isPublishing: () => second.isPublishing,
        setPublishing: (value) => {
          second.isPublishing = value;
        }
      })
    ]);

    const row = await readOutboxEvent(postgres, outboxEvent.id);

    if (publishCalls.length !== 1) {
      throw new Error(
        `Expected exactly one publish call, got ${publishCalls.length}: ${json(publishCalls)}`
      );
    }

    if (
      row?.status !== "PUBLISHED" ||
      row?.locked_by !== null ||
      row?.locked_until !== null
    ) {
      throw new Error(`Unexpected outbox row after publish: ${json(row)}`);
    }

    console.log(
      json({
        ok: true,
        scenario: "two outbox publisher replicas do not publish the same row",
        outboxEventId: outboxEvent.id,
        eventId: outboxEvent.event.eventId,
        publishCalls,
        status: row.status,
        publishedAt: row.published_at
      })
    );
  } finally {
    await deleteOutboxEvent(postgres, outboxEvent.id);
    await postgres.end().catch(() => undefined);
  }
}

function createPublisherState() {
  return {
    isPublishing: false
  };
}

function createOutboxEvent() {
  const orderId = randomUUID();
  const event = {
    eventId: randomUUID(),
    eventType: "OrderCreated",
    eventVersion: 1,
    occurredAt: new Date().toISOString(),
    correlationId: randomUUID(),
    causationId: null,
    producer: "outbox-concurrent-publish-e2e",
    payload: {
      orderId,
      userId: `e2e-concurrent-outbox-${Date.now()}`,
      currency: "USD",
      totalAmount: 100,
      itemCount: 1
    }
  };

  return {
    id: randomUUID(),
    topic: "order.order-events",
    messageKey: orderId,
    eventType: event.eventType,
    eventId: event.eventId,
    event
  };
}

async function insertOutboxEvent(postgres, outboxEvent) {
  await postgres.query(
    `
      insert into outbox_events (
        id,
        topic,
        message_key,
        event_type,
        event_id,
        event,
        trace_context,
        status,
        attempts,
        next_attempt_at,
        locked_by,
        locked_until,
        created_at,
        updated_at
      )
      values ($1, $2, $3, $4, $5, $6::jsonb, null, 'PENDING', 0, null, null, null, now(), now())
    `,
    [
      outboxEvent.id,
      outboxEvent.topic,
      outboxEvent.messageKey,
      outboxEvent.eventType,
      outboxEvent.eventId,
      JSON.stringify(outboxEvent.event)
    ]
  );
}

async function readOutboxEvent(postgres, id) {
  const result = await postgres.query(
    `
      select id, status, locked_by, locked_until, published_at
      from outbox_events
      where id = $1
    `,
    [id]
  );

  return result.rows[0] ?? null;
}

async function deleteOutboxEvent(postgres, id) {
  await postgres
    .query(
      `
        delete from outbox_events
        where id = $1
      `,
      [id]
    )
    .catch(() => undefined);
}

function createRepositoryAdapter(postgres) {
  return {
    manager: {
      async query(sql, params) {
        const result = await postgres.query(sql, params);

        return result.rows;
      }
    },
    create(value) {
      return value;
    },
    async update(criteria, values) {
      if (values.status === "PUBLISHED") {
        await postgres.query(
          `
            update outbox_events
            set
              status = 'PUBLISHED',
              published_at = $2,
              last_error = null,
              locked_by = null,
              locked_until = null,
              updated_at = now()
            where id = $1
          `,
          [criteria.id, values.publishedAt]
        );
        return;
      }

      if (values.status === "FAILED") {
        await postgres.query(
          `
            update outbox_events
            set
              status = 'FAILED',
              attempts = $2,
              next_attempt_at = $3,
              last_error = $4,
              locked_by = null,
              locked_until = null,
              updated_at = now()
            where id = $1
          `,
          [
            criteria.id,
            values.attempts,
            values.nextAttemptAt,
            values.lastError
          ]
        );
      }
    }
  };
}

const silentLogger = {
  info() {},
  warn() {}
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
