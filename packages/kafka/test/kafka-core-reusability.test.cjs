const assert = require("node:assert/strict");
const { describe, it } = require("node:test");
const {
  ConfigurableKafkaRetryPolicy,
  KAFKA_HEADER_NAMES
} = require("../dist/core");
const { KafkaProducerService } = require("../dist");

function createEvent() {
  return {
    eventId: "invoice-event-1",
    eventType: "InvoiceCreated",
    eventVersion: 1,
    occurredAt: "2026-06-16T00:00:00.000Z",
    correlationId: "correlation-1",
    causationId: null,
    producer: "billing-service",
    payload: {
      invoiceId: "invoice-1"
    }
  };
}

describe("Kafka core reusability", () => {
  it("строит retry-цепочку для произвольных topics без playground contracts", () => {
    const policy = new ConfigurableKafkaRetryPolicy({
      sourceTopics: ["billing.invoice-events"],
      stages: [
        {
          topic: "billing.invoice-events.retry-10s",
          delayMs: 10_000
        },
        {
          topic: "billing.invoice-events.retry-1m",
          delayMs: 60_000
        }
      ],
      deadLetterTopic: "billing.dead-letter"
    });

    const firstDecision = policy.decideFailure({
      currentTopic: "billing.invoice-events",
      headers: undefined,
      error: new TypeError("temporary failure"),
      now: new Date("2026-06-16T00:01:00.000Z")
    });
    const terminalDecision = policy.decideFailure({
      currentTopic: "billing.invoice-events.retry-1m",
      headers: {
        [KAFKA_HEADER_NAMES.originalTopic]: "billing.invoice-events",
        [KAFKA_HEADER_NAMES.retryCount]: "2",
        [KAFKA_HEADER_NAMES.firstFailedAt]: firstDecision.firstFailedAt
      },
      error: new Error("still broken")
    });

    assert.deepEqual(policy.getSubscriptionTopics("billing.invoice-events"), [
      "billing.invoice-events",
      "billing.invoice-events.retry-10s",
      "billing.invoice-events.retry-1m"
    ]);
    assert.equal(policy.getDelayMs("billing.invoice-events.retry-10s"), 10_000);
    assert.equal(firstDecision.destinationTopic, "billing.invoice-events.retry-10s");
    assert.equal(firstDecision.errorCode, "TYPE_ERROR");
    assert.equal(terminalDecision.destinationTopic, "billing.dead-letter");
    assert.equal(terminalDecision.terminal, true);
  });

  it("позволяет проекту задать собственный resolver Schema Registry subject", async () => {
    const sent = [];
    const serialized = [];
    const producer = {
      async send(message) {
        sent.push(message);
        return [
          {
            topic: message.topic,
            partition: 0,
            offset: "1"
          }
        ];
      }
    };
    const codec = {
      async serialize(subject, payload) {
        serialized.push({ subject, payload });
        return Buffer.from(JSON.stringify(payload));
      }
    };
    const logger = {
      logProduced() {}
    };
    const service = new KafkaProducerService(
      {
        serviceName: "billing-service",
        clientId: "billing-service",
        brokers: ["localhost:9092"],
        schemaRegistryUrl: "http://localhost:8081",
        subjectResolver: ({ topic, eventType }) => `${topic}.${eventType}.v1`
      },
      producer,
      codec,
      logger
    );

    await service.publish({
      topic: "billing.invoice-events",
      key: "invoice-1",
      event: createEvent()
    });

    assert.equal(serialized[0].subject, "billing.invoice-events.InvoiceCreated.v1");
    assert.equal(sent[0].messages[0].headers[KAFKA_HEADER_NAMES.eventId], "invoice-event-1");
  });
});
