const test = require("node:test");
const assert = require("node:assert/strict");
const { KAFKA_TOPICS } = require("@kafka-playground/contracts");
const {
  KAFKA_HEADER_NAMES,
  KafkaConsumerRunner
} = require("../dist");

function createEvent(eventType, eventId) {
  return {
    eventId,
    eventType,
    eventVersion: 1,
    occurredAt: "2026-06-12T10:00:00.000Z",
    correlationId: "correlation-1",
    causationId: null,
    producer: "test",
    payload: {}
  };
}

function createRunner() {
  const subscribedTopics = [];
  let eachMessage;
  let runCalls = 0;
  const consumer = {
    async subscribe({ topic }) {
      subscribedTopics.push(topic);
    },
    async run(options) {
      runCalls += 1;
      eachMessage = options.eachMessage;
    }
  };
  const codec = {
    async deserialize(value) {
      return JSON.parse(value.toString("utf8"));
    }
  };
  const logger = {
    logConsumed() {},
    logFailed() {},
    logConsumerStartFailed() {}
  };
  const retryPolicy = {
    getSubscriptionTopics(topic) {
      return [topic];
    },
    getDelayMs() {
      return 0;
    },
    supports() {
      return false;
    }
  };
  const retryDispatcher = {
    async dispatch() {
      throw new Error("dispatcher must not be called");
    }
  };
  const runner = new KafkaConsumerRunner(
    consumer,
    codec,
    logger,
    retryPolicy,
    retryDispatcher
  );

  return {
    runner,
    subscribedTopics,
    getEachMessage: () => eachMessage,
    getRunCalls: () => runCalls
  };
}

test("обслуживает несколько зарегистрированных handler-ов одним Kafka loop", async () => {
  const fixture = createRunner();
  const handled = [];

  await fixture.runner.subscribe(
    { topic: KAFKA_TOPICS.riskRiskEvents },
    async ({ event }) => {
      handled.push(`risk:${event.eventId}`);
    }
  );
  await fixture.runner.subscribe(
    { topic: KAFKA_TOPICS.deadLetterEvents },
    async ({ event }) => {
      handled.push(`dlq:${event.eventId}`);
    }
  );

  fixture.runner.onApplicationBootstrap();
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(fixture.getRunCalls(), 1);
  assert.deepEqual(fixture.subscribedTopics, [
    KAFKA_TOPICS.riskRiskEvents,
    KAFKA_TOPICS.deadLetterEvents
  ]);

  await consume(
    fixture.getEachMessage(),
    KAFKA_TOPICS.riskRiskEvents,
    createEvent("OrderRiskApproved", "event-risk")
  );
  await consume(
    fixture.getEachMessage(),
    KAFKA_TOPICS.deadLetterEvents,
    createEvent("DeadLetterEvent", "event-dlq"),
    {
      [KAFKA_HEADER_NAMES.originalTopic]:
        KAFKA_TOPICS.riskRiskEvents
    }
  );

  assert.deepEqual(handled, [
    "risk:event-risk",
    "dlq:event-dlq"
  ]);
});

test("маршрутизирует общий retry topic по x-original-topic", async () => {
  const fixture = createRunner();
  const handled = [];

  await fixture.runner.subscribe(
    { topic: KAFKA_TOPICS.riskRiskEvents },
    async ({ event }) => {
      handled.push(`risk:${event.eventId}`);
    }
  );
  await fixture.runner.subscribe(
    { topic: KAFKA_TOPICS.paymentPaymentEvents },
    async ({ event }) => {
      handled.push(`payment:${event.eventId}`);
    }
  );

  fixture.runner.onApplicationBootstrap();
  await new Promise((resolve) => setImmediate(resolve));

  await consume(
    fixture.getEachMessage(),
    KAFKA_TOPICS.orderOrderEventsRetry5s,
    createEvent("PaymentFailed", "event-payment"),
    {
      [KAFKA_HEADER_NAMES.originalTopic]:
        KAFKA_TOPICS.paymentPaymentEvents
    }
  );

  assert.deepEqual(handled, ["payment:event-payment"]);
});

test("отклоняет неоднозначную повторную регистрацию topic", async () => {
  const fixture = createRunner();

  await fixture.runner.subscribe(
    { topic: KAFKA_TOPICS.riskRiskEvents },
    async () => {}
  );

  await assert.rejects(
    fixture.runner.subscribe(
      { topic: KAFKA_TOPICS.riskRiskEvents },
      async () => {}
    ),
    /already registered/
  );
});

async function consume(eachMessage, topic, event, headers = {}) {
  await eachMessage({
    topic,
    partition: 0,
    heartbeat: async () => {},
    message: {
      key: Buffer.from("key"),
      value: Buffer.from(JSON.stringify(event)),
      offset: "1",
      headers
    }
  });
}
