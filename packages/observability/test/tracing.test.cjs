const test = require("node:test");
const assert = require("node:assert/strict");
const { ROOT_CONTEXT, trace } = require("@opentelemetry/api");
const {
  extractTraceContext,
  injectTraceContext,
  isTechnicalEndpoint
} = require("../dist/tracing.js");

const TRACE_ID = "11111111111111111111111111111111";
const SPAN_ID = "2222222222222222";

test("сериализует traceId и spanId в W3C и диагностические headers", () => {
  const sourceContext = trace.setSpanContext(ROOT_CONTEXT, {
    traceId: TRACE_ID,
    spanId: SPAN_ID,
    traceFlags: 1
  });
  const carrier = injectTraceContext({}, sourceContext);

  assert.equal(
    carrier.traceparent,
    `00-${TRACE_ID}-${SPAN_ID}-01`
  );
  assert.equal(carrier["x-trace-id"], TRACE_ID);
  assert.equal(carrier["x-span-id"], SPAN_ID);
});

test("восстанавливает remote parent из Kafka headers с Buffer", () => {
  const restored = extractTraceContext({
    traceparent: Buffer.from(
      `00-${TRACE_ID}-${SPAN_ID}-01`,
      "utf8"
    )
  });
  const spanContext = trace.getSpanContext(restored);

  assert.equal(spanContext.traceId, TRACE_ID);
  assert.equal(spanContext.spanId, SPAN_ID);
  assert.equal(spanContext.isRemote, true);
});

test("отклоняет повреждённый traceparent", () => {
  const restored = extractTraceContext({
    traceparent: "invalid-trace-parent"
  });

  assert.equal(trace.getSpanContext(restored), undefined);
});

test("исключает metrics и health endpoints из tracing", () => {
  assert.equal(isTechnicalEndpoint("/metrics"), true);
  assert.equal(isTechnicalEndpoint("/metrics?format=prometheus"), true);
  assert.equal(isTechnicalEndpoint("/health/live"), true);
  assert.equal(isTechnicalEndpoint("/orders"), false);
});
