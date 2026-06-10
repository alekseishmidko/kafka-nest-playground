const test = require("node:test");
const assert = require("node:assert/strict");
const {
  decideOrderTransition
} = require("../dist/modules/orders/order-state-machine.js");
const {
  OrderStatus
} = require("../dist/modules/orders/entities/order.entity.js");

test("разрешает полный успешный путь заказа", () => {
  assert.deepEqual(
    decideOrderTransition(OrderStatus.Pending, "OrderRiskApproved"),
    {
      allowed: true,
      from: OrderStatus.Pending,
      to: OrderStatus.RiskApproved,
      finalEventType: null
    }
  );

  assert.deepEqual(
    decideOrderTransition(OrderStatus.RiskApproved, "PaymentAuthorized"),
    {
      allowed: true,
      from: OrderStatus.RiskApproved,
      to: OrderStatus.Confirmed,
      finalEventType: "OrderConfirmed"
    }
  );
});

test("отменяет pending-заказ после risk rejection", () => {
  assert.deepEqual(
    decideOrderTransition(OrderStatus.Pending, "OrderRiskRejected"),
    {
      allowed: true,
      from: OrderStatus.Pending,
      to: OrderStatus.Cancelled,
      finalEventType: "OrderCancelled"
    }
  );
});

test("отменяет risk-approved заказ после payment failure", () => {
  assert.deepEqual(
    decideOrderTransition(OrderStatus.RiskApproved, "PaymentFailed"),
    {
      allowed: true,
      from: OrderStatus.RiskApproved,
      to: OrderStatus.Cancelled,
      finalEventType: "OrderCancelled"
    }
  );
});

test("отклоняет payment event, пришедший раньше risk approval", () => {
  assert.deepEqual(
    decideOrderTransition(OrderStatus.Pending, "PaymentAuthorized"),
    {
      allowed: false,
      from: OrderStatus.Pending,
      eventType: "PaymentAuthorized",
      reason: "invalid_transition"
    }
  );
});

test("не изменяет терминальные состояния", () => {
  for (const status of [OrderStatus.Confirmed, OrderStatus.Cancelled]) {
    for (const eventType of [
      "OrderRiskApproved",
      "OrderRiskRejected",
      "PaymentAuthorized",
      "PaymentFailed"
    ]) {
      assert.equal(
        decideOrderTransition(status, eventType).allowed,
        false,
        `${status} must reject ${eventType}`
      );
    }
  }
});
