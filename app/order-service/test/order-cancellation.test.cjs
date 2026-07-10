const test = require("node:test");
const assert = require("node:assert/strict");
const {
  createOrderCancellationRejectedEvent,
  createOrderCancellationRequestedEvent,
  createUserOrderCancelledEvent,
  decideOrderCancellation
} = require("../dist/modules/orders/order-cancellation.js");
const {
  OrderStatus
} = require("../dist/modules/orders/entities/order.entity.js");

const order = {
  id: "00000000-0000-4000-8000-000000000001",
  userId: "user-1",
  status: OrderStatus.Pending
};
const command = {
  orderId: order.id,
  reason: "Пользователь отменил заказ до оплаты",
  requestedBy: "user"
};
const occurredAt = "2026-07-10T10:00:00.000Z";

test("разрешает отмену pending и risk-approved заказа", () => {
  assert.deepEqual(decideOrderCancellation(OrderStatus.Pending), {
    accepted: true,
    from: OrderStatus.Pending,
    to: OrderStatus.Cancelled
  });
  assert.deepEqual(decideOrderCancellation(OrderStatus.RiskApproved), {
    accepted: true,
    from: OrderStatus.RiskApproved,
    to: OrderStatus.Cancelled
  });
});

test("отклоняет отмену confirmed и уже cancelled заказа", () => {
  assert.deepEqual(decideOrderCancellation(OrderStatus.Confirmed), {
    accepted: false,
    from: OrderStatus.Confirmed,
    rejectedReason: "already_confirmed"
  });
  assert.deepEqual(decideOrderCancellation(OrderStatus.Cancelled), {
    accepted: false,
    from: OrderStatus.Cancelled,
    rejectedReason: "already_cancelled"
  });
});

test("создаёт request, cancelled и rejected события одной correlation chain", () => {
  const requested = createOrderCancellationRequestedEvent(
    order,
    command,
    occurredAt,
    "00000000-0000-4000-8000-000000000002"
  );
  const cancelled = createUserOrderCancelledEvent(
    order,
    command,
    requested,
    occurredAt
  );
  const rejected = createOrderCancellationRejectedEvent(
    {
      ...order,
      status: OrderStatus.Confirmed
    },
    command,
    requested,
    "already_confirmed",
    occurredAt
  );

  assert.equal(requested.eventType, "OrderCancellationRequested");
  assert.equal(requested.payload.requestedBy, "user");
  assert.equal(cancelled.eventType, "OrderCancelled");
  assert.equal(cancelled.causationId, requested.eventId);
  assert.equal(cancelled.correlationId, requested.correlationId);
  assert.equal(cancelled.payload.cancelledBy, "user");
  assert.equal(rejected.eventType, "OrderCancellationRejected");
  assert.equal(rejected.causationId, requested.eventId);
  assert.equal(rejected.payload.currentStatus, OrderStatus.Confirmed);
  assert.equal(rejected.payload.rejectedReason, "already_confirmed");
});
