const test = require("node:test");
const assert = require("node:assert/strict");
const {
  AdminAuditService
} = require("../dist/modules/admin-audit/admin-audit.service.js");
const {
  AdminAuditDecision
} = require("../dist/modules/admin-audit/entities/admin-audit-event.entity.js");

function createService(events = []) {
  const calls = {
    findAndCount: [],
    findOneBy: []
  };
  const repository = {
    async insert() {},
    async findAndCount(options) {
      calls.findAndCount.push(options);
      return [events, events.length];
    },
    async findOneBy(criteria) {
      calls.findOneBy.push(criteria);
      return events.find((event) => event.id === criteria.id) ?? null;
    }
  };
  const logger = {
    setContext() {},
    warn() {}
  };

  return {
    service: new AdminAuditService(repository, logger),
    calls
  };
}

test("findPage возвращает audit events с exact-match фильтрами", async () => {
  const auditEvent = {
    id: "00000000-0000-4000-8000-000000000001",
    action: "outbox.retry",
    decision: AdminAuditDecision.Allowed
  };
  const { service, calls } = createService([auditEvent]);

  const result = await service.findPage({
    actor: "admin@example.com",
    role: "ADMIN_OPERATOR",
    method: "POST",
    path: "/admin/outbox/00000000-0000-4000-8000-000000000001/retry",
    action: "outbox.retry",
    entityType: "outbox_event",
    entityId: "00000000-0000-4000-8000-000000000001",
    decision: AdminAuditDecision.Allowed,
    limit: 25,
    offset: 50
  });

  assert.deepEqual(result, {
    items: [auditEvent],
    total: 1,
    limit: 25,
    offset: 50
  });
  assert.deepEqual(calls.findAndCount[0], {
    where: {
      actor: "admin@example.com",
      role: "ADMIN_OPERATOR",
      method: "POST",
      path: "/admin/outbox/00000000-0000-4000-8000-000000000001/retry",
      action: "outbox.retry",
      entityType: "outbox_event",
      entityId: "00000000-0000-4000-8000-000000000001",
      decision: AdminAuditDecision.Allowed
    },
    order: {
      createdAt: "DESC"
    },
    take: 25,
    skip: 50
  });
});

test("findOne возвращает audit event по id", async () => {
  const auditEvent = {
    id: "00000000-0000-4000-8000-000000000002"
  };
  const { service, calls } = createService([auditEvent]);

  assert.equal(await service.findOne(auditEvent.id), auditEvent);
  assert.deepEqual(calls.findOneBy[0], { id: auditEvent.id });
});

test("findOne возвращает 404 для неизвестного audit event", async () => {
  const { service } = createService();

  await assert.rejects(
    () => service.findOne("00000000-0000-4000-8000-000000000003"),
    /Admin audit event 00000000-0000-4000-8000-000000000003 was not found/
  );
});
