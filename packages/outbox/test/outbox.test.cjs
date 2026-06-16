const assert = require("node:assert/strict");
const { describe, it } = require("node:test");
const {
  OutboxEventStatus,
  PostgresOutboxStore,
  createOutboxSchemaQueries,
  dropOutboxSchemaQueries,
  publishOutboxBatch
} = require("../dist");

function createEvent(overrides = {}) {
  return {
    eventId: "11111111-1111-4111-8111-111111111111",
    eventType: "InvoiceCreated",
    eventVersion: 1,
    occurredAt: "2026-06-16T00:00:00.000Z",
    correlationId: "correlation-1",
    causationId: null,
    producer: "billing-service",
    payload: {
      invoiceId: "invoice-1"
    },
    ...overrides
  };
}

function createRepository() {
  const created = [];

  return {
    created,
    create(value) {
      const entity = {
        id: "outbox-1",
        attempts: 0,
        nextAttemptAt: null,
        publishedAt: null,
        lastError: null,
        createdAt: new Date("2026-06-16T00:00:00.000Z"),
        updatedAt: new Date("2026-06-16T00:00:00.000Z"),
        ...value
      };
      created.push(entity);
      return entity;
    },
    async find() {
      return created;
    },
    async update() {},
    createQueryBuilder() {
      return {
        select() {
          return this;
        },
        addSelect() {
          return this;
        },
        groupBy() {
          return this;
        },
        async getRawMany() {
          return [];
        }
      };
    }
  };
}

describe("shared outbox package", () => {
  it("создаёт PENDING entity без сохранения вне вызывающей транзакции", () => {
    const repository = createRepository();
    const store = new PostgresOutboxStore(repository);
    const event = createEvent();

    const entity = store.createPending({
      topic: "billing.invoice-events",
      messageKey: "invoice-1",
      event,
      traceContext: {
        traceparent:
          "00-11111111111111111111111111111111-2222222222222222-01"
      }
    });

    assert.equal(entity.topic, "billing.invoice-events");
    assert.equal(entity.messageKey, "invoice-1");
    assert.equal(entity.eventId, event.eventId);
    assert.equal(entity.eventType, "InvoiceCreated");
    assert.equal(entity.status, OutboxEventStatus.Pending);
    assert.equal(repository.created.length, 1);
  });

  it("публикует batch и помечает запись PUBLISHED после успеха", async () => {
    const calls = [];
    let publishing = false;
    const event = createEvent();
    const store = {
      async findPublishable() {
        return [
          {
            id: "outbox-1",
            topic: "billing.invoice-events",
            messageKey: "invoice-1",
            eventType: event.eventType,
            eventId: event.eventId,
            event,
            traceContext: null,
            attempts: 0
          }
        ];
      },
      async markPublished(id) {
        calls.push(["published", id]);
      },
      async markFailed(id, attempts) {
        calls.push(["failed", id, attempts]);
      }
    };
    const publisher = {
      async publish(message) {
        calls.push(["publish", message.topic, message.event.eventId]);
      }
    };

    await publishOutboxBatch({
      store,
      publisher,
      logger: { info() {}, warn() {} },
      metrics: {
        recordOutboxPublish(topic, result) {
          calls.push(["metric", topic, result]);
        }
      },
      batchSize: 25,
      isPublishing: () => publishing,
      setPublishing: (value) => {
        publishing = value;
      }
    });

    assert.deepEqual(calls, [
      ["publish", "billing.invoice-events", event.eventId],
      ["published", "outbox-1"],
      ["metric", "billing.invoice-events", "success"]
    ]);
    assert.equal(publishing, false);
  });

  it("сохраняет FAILED и продолжает batch после ошибки публикации", async () => {
    const calls = [];
    let publishing = false;
    const firstEvent = createEvent({ eventId: "11111111-1111-4111-8111-111111111111" });
    const secondEvent = createEvent({ eventId: "22222222-2222-4222-8222-222222222222" });
    const store = {
      async findPublishable() {
        return [
          {
            id: "outbox-1",
            topic: "billing.invoice-events",
            messageKey: "invoice-1",
            eventType: firstEvent.eventType,
            eventId: firstEvent.eventId,
            event: firstEvent,
            traceContext: null,
            attempts: 0
          },
          {
            id: "outbox-2",
            topic: "billing.invoice-events",
            messageKey: "invoice-2",
            eventType: secondEvent.eventType,
            eventId: secondEvent.eventId,
            event: secondEvent,
            traceContext: null,
            attempts: 0
          }
        ];
      },
      async markPublished(id) {
        calls.push(["published", id]);
      },
      async markFailed(id, attempts) {
        calls.push(["failed", id, attempts]);
      }
    };
    const publisher = {
      async publish(message) {
        calls.push(["publish", message.event.eventId]);
        if (message.event.eventId === firstEvent.eventId) {
          throw new Error("broker is unavailable");
        }
      }
    };

    await publishOutboxBatch({
      store,
      publisher,
      logger: { info() {}, warn() {} },
      metrics: {
        recordOutboxPublish(topic, result) {
          calls.push(["metric", result]);
        }
      },
      batchSize: 25,
      isPublishing: () => publishing,
      setPublishing: (value) => {
        publishing = value;
      }
    });

    assert.deepEqual(calls, [
      ["publish", firstEvent.eventId],
      ["failed", "outbox-1", 1],
      ["metric", "failure"],
      ["publish", secondEvent.eventId],
      ["published", "outbox-2"],
      ["metric", "success"]
    ]);
    assert.equal(publishing, false);
  });

  it("генерирует migration SQL для переиспользования outbox-схемы", () => {
    const createQueries = createOutboxSchemaQueries({
      tableName: "billing_outbox_events"
    });
    const dropQueries = dropOutboxSchemaQueries({
      tableName: "billing_outbox_events"
    });

    assert.equal(createQueries.length, 3);
    assert.match(createQueries[1], /create table if not exists "billing_outbox_events"/);
    assert.match(createQueries[1], /"trace_context" jsonb/);
    assert.deepEqual(dropQueries, [
      'drop index if exists "IDX_outbox_events_publishable"',
      'drop table if exists "billing_outbox_events"',
      'drop type if exists "outbox_events_status_enum"'
    ]);
  });
});
