const test = require("node:test");
const assert = require("node:assert/strict");
const { ApplicationMetrics } = require("../dist");

test("экспортирует прикладные метрики в Prometheus format", async () => {
  const metrics = new ApplicationMetrics({
    serviceName: "test-service",
    collectDefaultMetrics: false
  });

  metrics.recordKafkaConsumed("orders", "OrderCreated");
  metrics.recordKafkaFailed("orders", "OrderCreated", "TYPE_ERROR");
  metrics.recordKafkaRetry({
    originalTopic: "orders",
    destinationTopic: "orders.retry-5s",
    stage: "retry-5s"
  });
  metrics.recordKafkaDlq("orders", "TYPE_ERROR");
  metrics.observeKafkaProcessing({
    topic: "orders",
    eventType: "OrderCreated",
    result: "failure",
    durationSeconds: 0.25
  });
  metrics.setOutboxCount("PENDING", 3);
  metrics.setDlqNewCount(2);
  metrics.setConsumerLag({
    group: "orders",
    topic: "orders",
    partition: 0,
    lag: 7
  });

  const output = await metrics.render();

  assert.match(output, /kafka_events_consumed_total\{.*service="test-service".*\} 1/);
  assert.match(output, /kafka_events_failed_total\{.*error_code="TYPE_ERROR".*\} 1/);
  assert.match(output, /outbox_events\{.*status="PENDING".*\} 3/);
  assert.match(output, /outbox_pending_events\{.*service="test-service".*\} 3/);
  assert.match(output, /dlq_new_events\{.*service="test-service".*\} 2/);
  assert.match(output, /kafka_consumer_lag\{.*partition="0".*\} 7/);
});
