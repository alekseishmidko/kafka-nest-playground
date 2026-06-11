const assert = require("node:assert/strict");
const { describe, it } = require("node:test");
const { KAFKA_TOPICS } = require("@kafka-playground/contracts");
const {
  KAFKA_HEADER_NAMES,
  KafkaNonRetryableError,
  KafkaRetryPolicy
} = require("../dist");

describe("KafkaRetryPolicy", () => {
  const policy = new KafkaRetryPolicy();
  const firstFailedAt = "2026-06-12T10:00:00.000Z";

  it("подписывает consumer на основной topic и всю retry-цепочку", () => {
    assert.deepEqual(
      policy.getSubscriptionTopics(KAFKA_TOPICS.orderOrderEvents),
      [
        KAFKA_TOPICS.orderOrderEvents,
        KAFKA_TOPICS.orderOrderEventsRetry5s,
        KAFKA_TOPICS.orderOrderEventsRetry30s,
        KAFKA_TOPICS.orderOrderEventsRetry5m
      ]
    );
  });

  it("направляет первый сбой в retry-5s и создает retry headers", () => {
    const decision = policy.decideFailure({
      currentTopic: KAFKA_TOPICS.orderOrderEvents,
      headers: undefined,
      error: new TypeError("temporary error"),
      now: new Date(firstFailedAt)
    });

    assert.deepEqual(decision, {
      destinationTopic: KAFKA_TOPICS.orderOrderEventsRetry5s,
      originalTopic: KAFKA_TOPICS.orderOrderEvents,
      retryCount: 1,
      firstFailedAt,
      errorCode: "TYPE_ERROR",
      terminal: false
    });
  });

  it("проходит все retry-этапы и сохраняет время первого сбоя", () => {
    const commonHeaders = {
      [KAFKA_HEADER_NAMES.originalTopic]:
        KAFKA_TOPICS.orderOrderEvents,
      [KAFKA_HEADER_NAMES.firstFailedAt]: firstFailedAt
    };
    const retry30s = policy.decideFailure({
      currentTopic: KAFKA_TOPICS.orderOrderEventsRetry5s,
      headers: {
        ...commonHeaders,
        [KAFKA_HEADER_NAMES.retryCount]: "1"
      },
      error: new Error("second failure")
    });
    const retry5m = policy.decideFailure({
      currentTopic: KAFKA_TOPICS.orderOrderEventsRetry30s,
      headers: {
        ...commonHeaders,
        [KAFKA_HEADER_NAMES.retryCount]: "2"
      },
      error: new Error("third failure")
    });
    const deadLetter = policy.decideFailure({
      currentTopic: KAFKA_TOPICS.orderOrderEventsRetry5m,
      headers: {
        ...commonHeaders,
        [KAFKA_HEADER_NAMES.retryCount]: "3"
      },
      error: new Error("terminal failure")
    });

    assert.equal(
      retry30s.destinationTopic,
      KAFKA_TOPICS.orderOrderEventsRetry30s
    );
    assert.equal(retry30s.retryCount, 2);
    assert.equal(retry30s.firstFailedAt, firstFailedAt);

    assert.equal(
      retry5m.destinationTopic,
      KAFKA_TOPICS.orderOrderEventsRetry5m
    );
    assert.equal(retry5m.retryCount, 3);

    assert.equal(
      deadLetter.destinationTopic,
      KAFKA_TOPICS.deadLetterEvents
    );
    assert.equal(deadLetter.retryCount, 4);
    assert.equal(deadLetter.terminal, true);
    assert.equal(deadLetter.firstFailedAt, firstFailedAt);
  });

  it("возвращает задержку, соответствующую retry topic", () => {
    assert.equal(policy.getDelayMs(KAFKA_TOPICS.orderOrderEvents), 0);
    assert.equal(
      policy.getDelayMs(KAFKA_TOPICS.orderOrderEventsRetry5s),
      5_000
    );
    assert.equal(
      policy.getDelayMs(KAFKA_TOPICS.orderOrderEventsRetry30s),
      30_000
    );
    assert.equal(
      policy.getDelayMs(KAFKA_TOPICS.orderOrderEventsRetry5m),
      300_000
    );
  });

  it("не применяет order retry policy к постороннему topic", () => {
    assert.equal(
      policy.supports(KAFKA_TOPICS.analyticsDomainEvents),
      false
    );
    assert.throws(
      () =>
        policy.decideFailure({
          currentTopic: KAFKA_TOPICS.analyticsDomainEvents,
          headers: undefined,
          error: new Error("failure")
        }),
      /Retry policy is not configured/
    );
  });

  it("не доверяет x-original-topic для сообщения из чужого topic", () => {
    const headers = {
      [KAFKA_HEADER_NAMES.originalTopic]:
        KAFKA_TOPICS.orderOrderEvents
    };

    assert.equal(
      policy.supports(KAFKA_TOPICS.paymentPaymentEvents, headers),
      false
    );
    assert.throws(
      () =>
        policy.decideFailure({
          currentTopic: KAFKA_TOPICS.paymentPaymentEvents,
          headers,
          error: new Error("failure")
        }),
      /Retry policy is not configured/
    );
  });

  it("подключает общую retry-цепочку к risk и payment topics", () => {
    for (const topic of [
      KAFKA_TOPICS.riskRiskEvents,
      KAFKA_TOPICS.paymentPaymentEvents
    ]) {
      assert.deepEqual(policy.getSubscriptionTopics(topic), [
        topic,
        KAFKA_TOPICS.orderOrderEventsRetry5s,
        KAFKA_TOPICS.orderOrderEventsRetry30s,
        KAFKA_TOPICS.orderOrderEventsRetry5m
      ]);
      assert.equal(policy.supports(topic), true);
    }
  });

  it("направляет неисправимую ошибку сразу в DLQ", () => {
    const decision = policy.decideFailure({
      currentTopic: KAFKA_TOPICS.paymentPaymentEvents,
      headers: undefined,
      error: new KafkaNonRetryableError(
        "INVALID_ORDER_ID",
        "invalid order id"
      ),
      now: new Date(firstFailedAt)
    });

    assert.deepEqual(decision, {
      destinationTopic: KAFKA_TOPICS.deadLetterEvents,
      originalTopic: KAFKA_TOPICS.paymentPaymentEvents,
      retryCount: 1,
      firstFailedAt,
      errorCode: "INVALID_ORDER_ID",
      terminal: true
    });
  });
});
