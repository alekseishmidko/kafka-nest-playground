const test = require("node:test");
const assert = require("node:assert/strict");
const { KAFKA_TOPICS } = require("@kafka-playground/contracts");
const {
  createReprocessedEvent
} = require("../dist/modules/dlq/dlq-reprocess.factory.js");

const occurredAt = "2026-06-12T10:00:00.000Z";
const originalEvent = {
  eventId: "00000000-0000-4000-8000-000000000001",
  eventType: "OrderRiskApproved",
  eventVersion: 1,
  occurredAt: "2026-06-12T09:00:00.000Z",
  correlationId: "00000000-0000-4000-8000-000000000002",
  causationId: "00000000-0000-4000-8000-000000000003",
  producer: "risk-service",
  payload: {
    orderId: "test-order-id-2",
    amount: 125,
    currency: "USD",
    riskScore: 0.1,
    approvedBy: "risk-service"
  }
};

test("создаёт новую связанную копию после исправления payload", () => {
  const correctedOrderId =
    "00000000-0000-4000-8000-000000000004";
  const result = createReprocessedEvent(
    originalEvent,
    {
      orderId: correctedOrderId
    },
    KAFKA_TOPICS.riskRiskEvents,
    new Date(occurredAt)
  );

  assert.notEqual(result.event.eventId, originalEvent.eventId);
  assert.equal(result.event.causationId, originalEvent.eventId);
  assert.equal(result.event.occurredAt, occurredAt);
  assert.equal(result.event.correlationId, originalEvent.correlationId);
  assert.equal(result.event.producer, "order-service-dlq-reprocessor");
  assert.equal(result.event.payload.orderId, correctedOrderId);
  assert.equal(result.event.payload.riskScore, 0.1);
  assert.equal(result.topic, KAFKA_TOPICS.riskRiskEvents);
  assert.equal(result.messageKey, correctedOrderId);
});

test("отклоняет reprocess, пока orderId не исправлен", () => {
  assert.throws(
    () =>
      createReprocessedEvent(
        originalEvent,
        {},
        KAFKA_TOPICS.riskRiskEvents
      ),
    /payload\.orderId must be a UUID/
  );
});

test("запрещает публикацию event type в несоответствующий topic", () => {
  assert.throws(
    () =>
      createReprocessedEvent(
        originalEvent,
        {
          orderId: "00000000-0000-4000-8000-000000000004"
        },
        KAFKA_TOPICS.paymentPaymentEvents
      ),
    /must be published to risk\.risk-events/
  );
});
