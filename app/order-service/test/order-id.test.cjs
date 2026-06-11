const test = require("node:test");
const assert = require("node:assert/strict");
const {
  assertValidOrderId
} = require("../dist/modules/orders/order-id.js");

test("принимает корректный UUID заказа", () => {
  assert.doesNotThrow(() => {
    assertValidOrderId("00000000-0000-4000-8000-000000000001");
  });
});

test("отклоняет строковый test id как неисправимую Kafka-ошибку", () => {
  assert.throws(
    () => {
      assertValidOrderId("test-order-id-2");
    },
    (error) => {
      assert.equal(error.name, "KafkaNonRetryableError");
      assert.equal(error.errorCode, "INVALID_ORDER_ID");
      assert.match(error.message, /test-order-id-2/);
      return true;
    }
  );
});
