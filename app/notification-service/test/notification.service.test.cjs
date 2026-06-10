const test = require("node:test");
const assert = require("node:assert/strict");
const {
  NotificationService
} = require("../dist/modules/notification/notification.service.js");

function createService() {
  const deliveries = [];
  const config = {
    get: () => "customer@example.com"
  };
  const delivery = {
    deliver: async (request) => {
      deliveries.push(request);
    }
  };
  const logger = {
    setContext: () => undefined,
    warn: () => undefined
  };

  return {
    service: new NotificationService(config, delivery, logger),
    deliveries
  };
}

test("формирует уведомление OrderConfirmed", async () => {
  const { service, deliveries } = createService();

  await service.handleOrderConfirmed({
    eventId: "event-confirmed",
    eventType: "OrderConfirmed",
    eventVersion: 1,
    occurredAt: "2026-06-11T10:00:00.000Z",
    correlationId: "correlation-1",
    causationId: "payment-event",
    producer: "order-service",
    payload: {
      orderId: "order-1",
      userId: "user-1",
      currency: "USD",
      totalAmount: 100,
      paymentId: "payment-1",
      confirmedAt: "2026-06-11T10:00:00.000Z"
    }
  });

  assert.equal(deliveries.length, 1);
  assert.equal(deliveries[0].template, "order.confirmed");
  assert.equal(deliveries[0].notificationId, "order-confirmed-order-1");
  assert.equal(deliveries[0].data.paymentId, "payment-1");
});

test("формирует уведомление OrderCancelled", async () => {
  const { service, deliveries } = createService();

  await service.handleOrderCancelled({
    eventId: "event-cancelled",
    eventType: "OrderCancelled",
    eventVersion: 1,
    occurredAt: "2026-06-11T10:00:00.000Z",
    correlationId: "correlation-2",
    causationId: "risk-event",
    producer: "order-service",
    payload: {
      orderId: "order-2",
      userId: "user-2",
      reason: "risk_rejected",
      cancelledBy: "risk",
      cancelledAt: "2026-06-11T10:00:00.000Z"
    }
  });

  assert.equal(deliveries.length, 1);
  assert.equal(deliveries[0].template, "order.cancelled");
  assert.equal(deliveries[0].notificationId, "order-cancelled-order-2");
  assert.equal(deliveries[0].data.cancelledBy, "risk");
});
