const assert = require("node:assert/strict");
const { describe, it } = require("node:test");
const { KAFKA_TOPICS } = require("@kafka-playground/contracts");
const {
  KAFKA_HEADER_NAMES,
  KafkaRetryDispatcher,
  KafkaRetryPolicy
} = require("../dist");

function createOrderCreatedEvent() {
  return {
    eventId: "event-1",
    eventType: "OrderCreated",
    eventVersion: 1,
    occurredAt: "2026-06-12T09:00:00.000Z",
    correlationId: "correlation-1",
    causationId: null,
    producer: "order-service",
    payload: {
      orderId: "order-1",
      userId: "user-1",
      currency: "USD",
      totalAmount: 125,
      itemCount: 2
    }
  };
}

function createContext(overrides = {}) {
  return {
    topic: KAFKA_TOPICS.orderOrderEvents,
    partition: 2,
    offset: "42",
    key: "order-1",
    headers: {},
    event: createOrderCreatedEvent(),
    correlationId: "correlation-1",
    ...overrides
  };
}

function createDispatcher() {
  const published = [];
  const producer = {
    async publish(message) {
      published.push(message);
    }
  };
  const dispatcher = new KafkaRetryDispatcher(
    new KafkaRetryPolicy(),
    producer,
    {
      clientId: "test-client",
      serviceName: "risk-service",
      brokers: ["localhost:9092"],
      schemaRegistryUrl: "http://localhost:8081"
    }
  );

  return { dispatcher, published };
}

describe("KafkaRetryDispatcher", () => {
  it("повторно публикует исходное событие с тем же eventId", async () => {
    const { dispatcher, published } = createDispatcher();
    const context = createContext();

    const decision = await dispatcher.dispatch({
      context,
      error: new Error("temporary failure")
    });

    assert.equal(decision.terminal, false);
    assert.equal(published.length, 1);
    assert.equal(
      published[0].topic,
      KAFKA_TOPICS.orderOrderEventsRetry5s
    );
    assert.equal(published[0].event, context.event);
    assert.equal(published[0].event.eventId, "event-1");
    assert.equal(
      published[0].headers[KAFKA_HEADER_NAMES.retryCount],
      "1"
    );
    assert.equal(
      published[0].headers[KAFKA_HEADER_NAMES.originalTopic],
      KAFKA_TOPICS.orderOrderEvents
    );
    assert.equal(
      published[0].headers[KAFKA_HEADER_NAMES.errorCode],
      "ERROR"
    );
  });

  it("после retry-5m публикует DeadLetterEvent с контекстом ошибки", async () => {
    const { dispatcher, published } = createDispatcher();
    const firstFailedAt = "2026-06-12T09:01:00.000Z";
    const context = createContext({
      topic: KAFKA_TOPICS.orderOrderEventsRetry5m,
      headers: {
        [KAFKA_HEADER_NAMES.retryCount]: "3",
        [KAFKA_HEADER_NAMES.originalTopic]:
          KAFKA_TOPICS.orderOrderEvents,
        [KAFKA_HEADER_NAMES.firstFailedAt]: firstFailedAt
      }
    });

    const decision = await dispatcher.dispatch({
      context,
      error: new RangeError("permanent failure")
    });

    assert.equal(decision.terminal, true);
    assert.equal(published.length, 1);
    assert.equal(published[0].topic, KAFKA_TOPICS.deadLetterEvents);
    assert.equal(published[0].event.eventType, "DeadLetterEvent");
    assert.equal(published[0].event.causationId, context.event.eventId);
    assert.equal(
      published[0].event.payload.originalTopic,
      KAFKA_TOPICS.orderOrderEvents
    );
    assert.equal(published[0].event.payload.originalPartition, 2);
    assert.equal(published[0].event.payload.originalOffset, "42");
    assert.equal(
      published[0].event.payload.errorMessage,
      "permanent failure"
    );
    assert.equal(
      published[0].headers[KAFKA_HEADER_NAMES.retryCount],
      "4"
    );
    assert.equal(
      published[0].headers[KAFKA_HEADER_NAMES.firstFailedAt],
      firstFailedAt
    );
    assert.equal(
      published[0].headers[KAFKA_HEADER_NAMES.errorCode],
      "RANGE_ERROR"
    );
  });
});
