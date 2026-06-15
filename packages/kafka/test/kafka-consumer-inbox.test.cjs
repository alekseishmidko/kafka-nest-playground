const assert = require("node:assert/strict");
const { describe, it } = require("node:test");
const {
  createDeterministicEventId,
  KafkaIdempotentEventProcessor
} = require("../dist");

function createContext() {
  return {
    topic: "order.order-events",
    partition: 0,
    offset: "42",
    key: "order-1",
    headers: {},
    correlationId: "correlation-1",
    event: {
      eventId: "source-event-1",
      eventType: "OrderCreated",
      eventVersion: 1,
      occurredAt: "2026-06-15T00:00:00.000Z",
      correlationId: "correlation-1",
      causationId: null,
      producer: "test",
      payload: {}
    }
  };
}

function createLogger() {
  return {
    setContext() {},
    info() {}
  };
}

describe("Kafka durable inbox", () => {
  it("создаёт стабильный UUID результата", () => {
    const first = createDeterministicEventId("risk", "event-1");
    const second = createDeterministicEventId("risk", "event-1");
    const another = createDeterministicEventId("payment", "event-1");

    assert.equal(first, second);
    assert.notEqual(first, another);
    assert.match(
      first,
      /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
    );
  });

  it("сохраняет результат до side effect и завершает запись после него", async () => {
    const calls = [];
    const store = {
      async claim() {
        return {
          status: "PROCESSING",
          result: null,
          lockToken: "lock-1"
        };
      },
      async savePrepared({ result }) {
        calls.push(["prepared", result]);
      },
      async markCompleted() {
        calls.push(["completed"]);
      },
      async release() {
        calls.push(["released"]);
      }
    };
    const processor = new KafkaIdempotentEventProcessor(
      { serviceName: "risk-service" },
      store,
      createLogger()
    );

    const result = await processor.process(
      createContext(),
      () => ({ eventId: "result-1" }),
      async (prepared) => {
        calls.push(["effect", prepared]);
      }
    );

    assert.equal(result.duplicate, false);
    assert.deepEqual(calls, [
      ["prepared", { eventId: "result-1" }],
      ["effect", { eventId: "result-1" }],
      ["completed"]
    ]);
  });

  it("не выполняет side effect для COMPLETED события", async () => {
    let effects = 0;
    const store = {
      async claim() {
        return {
          status: "COMPLETED",
          result: { eventId: "result-1" },
          lockToken: null
        };
      },
      async savePrepared() {},
      async markCompleted() {},
      async release() {}
    };
    const processor = new KafkaIdempotentEventProcessor(
      { serviceName: "payment-service" },
      store,
      createLogger()
    );

    const result = await processor.process(
      createContext(),
      () => {
        throw new Error("prepare must not be called");
      },
      async () => {
        effects += 1;
      }
    );

    assert.equal(result.duplicate, true);
    assert.equal(effects, 0);
  });

  it("повторно использует PREPARED результат после падения", async () => {
    let prepareCalls = 0;
    let published;
    const store = {
      async claim() {
        return {
          status: "PREPARED",
          result: { eventId: "stable-result" },
          lockToken: "lock-2"
        };
      },
      async savePrepared() {
        throw new Error("prepared result must not be overwritten");
      },
      async markCompleted() {},
      async release() {}
    };
    const processor = new KafkaIdempotentEventProcessor(
      { serviceName: "risk-service" },
      store,
      createLogger()
    );

    await processor.process(
      createContext(),
      () => {
        prepareCalls += 1;
        return { eventId: "new-result" };
      },
      async (result) => {
        published = result;
      }
    );

    assert.equal(prepareCalls, 0);
    assert.deepEqual(published, { eventId: "stable-result" });
  });
});
