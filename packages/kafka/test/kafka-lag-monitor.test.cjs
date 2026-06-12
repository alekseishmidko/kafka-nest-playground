const test = require("node:test");
const assert = require("node:assert/strict");
const { calculateConsumerLag } = require("../dist");

test("вычисляет lag относительно committed offset", () => {
  assert.equal(
    calculateConsumerLag({
      high: "125",
      low: "10",
      committed: "100"
    }),
    25
  );
});

test("для новой consumer group считает lag от low offset", () => {
  assert.equal(
    calculateConsumerLag({
      high: "125",
      low: "10",
      committed: "-1"
    }),
    115
  );
});

test("не возвращает отрицательный lag", () => {
  assert.equal(
    calculateConsumerLag({
      high: "100",
      low: "10",
      committed: "101"
    }),
    0
  );
});
