const test = require("node:test");
const assert = require("node:assert/strict");
const {
  parseOriginalEvent
} = require("../dist/modules/dlq/dlq-event.parser.js");

const event = {
  eventId: "00000000-0000-4000-8000-000000000001",
  eventType: "PaymentFailed",
  eventVersion: 1,
  occurredAt: "2026-06-12T09:00:00.000Z",
  correlationId: "00000000-0000-4000-8000-000000000002",
  causationId: null,
  producer: "payment-service",
  payload: {
    paymentId: null,
    orderId: "invalid",
    reason: "declined",
    provider: "test"
  }
};

test("разбирает корректный исходный event envelope", () => {
  assert.deepEqual(parseOriginalEvent(JSON.stringify(event)), event);
});

test("возвращает null для повреждённого JSON и неполного envelope", () => {
  assert.equal(parseOriginalEvent("{broken"), null);
  assert.equal(parseOriginalEvent(JSON.stringify({ eventId: "event-1" })), null);
  assert.equal(parseOriginalEvent(null), null);
});
