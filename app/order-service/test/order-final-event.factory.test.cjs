const test = require("node:test");
const assert = require("node:assert/strict");
const {
  createFinalOrderEvent
} = require("../dist/modules/orders/order-final-event.factory.js");

const order = {
  id: "00000000-0000-4000-8000-000000000001",
  userId: "user-1",
  currency: "USD",
  totalAmount: "125.50"
};
const occurredAt = "2026-06-11T10:00:00.000Z";

test("создаёт OrderConfirmed с tracing metadata исходного payment event", () => {
  const event = createFinalOrderEvent(
    order,
    {
      eventId: "00000000-0000-4000-8000-000000000002",
      eventType: "PaymentAuthorized",
      eventVersion: 1,
      occurredAt,
      correlationId: "00000000-0000-4000-8000-000000000003",
      causationId: null,
      producer: "payment-service",
      payload: {
        paymentId: "payment-1",
        orderId: order.id,
        amount: 125.5,
        currency: "USD",
        provider: "test"
      }
    },
    occurredAt
  );

  assert.equal(event.eventType, "OrderConfirmed");
  assert.equal(event.causationId, "00000000-0000-4000-8000-000000000002");
  assert.equal(event.payload.totalAmount, 125.5);
  assert.equal(event.payload.paymentId, "payment-1");
  assert.equal(event.payload.confirmedAt, occurredAt);
});

test("указывает источник отмены для risk и payment отказов", () => {
  const riskEvent = createFinalOrderEvent(
    order,
    {
      eventId: "00000000-0000-4000-8000-000000000004",
      eventType: "OrderRiskRejected",
      eventVersion: 1,
      occurredAt,
      correlationId: "00000000-0000-4000-8000-000000000005",
      causationId: null,
      producer: "risk-service",
      payload: {
        orderId: order.id,
        riskScore: 0.9,
        reason: "risk_rejected",
        rejectedBy: "risk-service"
      }
    },
    occurredAt
  );
  const paymentEvent = createFinalOrderEvent(
    order,
    {
      eventId: "00000000-0000-4000-8000-000000000006",
      eventType: "PaymentFailed",
      eventVersion: 1,
      occurredAt,
      correlationId: "00000000-0000-4000-8000-000000000007",
      causationId: null,
      producer: "payment-service",
      payload: {
        paymentId: null,
        orderId: order.id,
        reason: "payment_declined",
        provider: "test"
      }
    },
    occurredAt
  );

  assert.equal(riskEvent.eventType, "OrderCancelled");
  assert.equal(riskEvent.payload.cancelledBy, "risk");
  assert.equal(paymentEvent.eventType, "OrderCancelled");
  assert.equal(paymentEvent.payload.cancelledBy, "payment");
});
