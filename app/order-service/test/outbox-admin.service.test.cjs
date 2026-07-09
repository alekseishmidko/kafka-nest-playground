const test = require("node:test");
const assert = require("node:assert/strict");
const {
  OutboxEventStatus
} = require("@kafka-playground/outbox");
const {
  OutboxAdminService
} = require("../dist/modules/outbox-admin/outbox-admin.service.js");

function createService(entity, bulkRetried = 0) {
  const saved = [];
  const publisher = {
    calls: 0,
    publishPending() {
      this.calls += 1;
      return Promise.resolve();
    }
  };
  const manager = {
    async findOne() {
      return entity;
    },
    async save(value) {
      saved.push({ ...value });
      return value;
    }
  };
  const dataSource = {
    transaction(callback) {
      return callback(manager);
    }
  };
  const repository = {
    makeFailedReadyForRetry() {
      return Promise.resolve(bulkRetried);
    }
  };

  return {
    service: new OutboxAdminService(repository, dataSource, publisher),
    publisher,
    saved
  };
}

test("retryOne снимает backoff только с FAILED outbox event", async () => {
  const entity = {
    id: "00000000-0000-4000-8000-000000000001",
    status: OutboxEventStatus.Failed,
    nextAttemptAt: new Date(),
    lockedBy: "publisher-1",
    lockedUntil: new Date()
  };
  const { service, publisher, saved } = createService(entity);

  const result = await service.retryOne(entity.id);

  assert.equal(result.status, OutboxEventStatus.Failed);
  assert.equal(result.nextAttemptAt, null);
  assert.equal(result.lockedBy, null);
  assert.equal(result.lockedUntil, null);
  assert.equal(saved.length, 1);
  assert.equal(publisher.calls, 1);
});

test("retryOne отклоняет PUBLISHED outbox event", async () => {
  const { service } = createService({
    id: "00000000-0000-4000-8000-000000000002",
    status: OutboxEventStatus.Published
  });

  await assert.rejects(
    () => service.retryOne("00000000-0000-4000-8000-000000000002"),
    /Only FAILED outbox events can be retried/
  );
});

test("ignore переводит PENDING или FAILED в IGNORED", async () => {
  for (const status of [
    OutboxEventStatus.Pending,
    OutboxEventStatus.Failed
  ]) {
    const entity = {
      id: "00000000-0000-4000-8000-000000000003",
      status,
      nextAttemptAt: new Date(),
      lockedBy: "publisher-1",
      lockedUntil: new Date(),
      lastError: "publish failed"
    };
    const { service } = createService(entity);

    const result = await service.ignore(entity.id, {
      operatorId: "dlq-operator",
      reason: "manual investigation completed"
    });

    assert.equal(result.status, OutboxEventStatus.Ignored);
    assert.equal(result.nextAttemptAt, null);
    assert.equal(result.lockedBy, null);
    assert.equal(result.lockedUntil, null);
    assert.match(result.lastError, /manual investigation completed/);
  }
});

test("ignore не меняет уже опубликованное событие", async () => {
  const { service } = createService({
    id: "00000000-0000-4000-8000-000000000004",
    status: OutboxEventStatus.Published
  });

  await assert.rejects(
    () =>
      service.ignore("00000000-0000-4000-8000-000000000004", {
        operatorId: "dlq-operator",
        reason: "manual investigation completed"
      }),
    /Only PENDING or FAILED outbox events can be ignored/
  );
});

test("retryFailed запускает publisher только если есть подготовленные записи", async () => {
  const withRows = createService(null, 3);
  const withoutRows = createService(null, 0);

  assert.deepEqual(await withRows.service.retryFailed(100), {
    retried: 3,
    limit: 100
  });
  assert.equal(withRows.publisher.calls, 1);

  assert.deepEqual(await withoutRows.service.retryFailed(100), {
    retried: 0,
    limit: 100
  });
  assert.equal(withoutRows.publisher.calls, 0);
});
