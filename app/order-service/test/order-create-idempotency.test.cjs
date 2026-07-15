const test = require("node:test");
const assert = require("node:assert/strict");
const {
  OrdersService
} = require("../dist/modules/orders/orders.service.js");
const {
  OrderStatus
} = require("../dist/modules/orders/entities/order.entity.js");

const dto = {
  userId: "user-1",
  currency: "USD",
  items: [
    {
      productId: "product-1",
      quantity: 2,
      unitPrice: 10
    }
  ]
};

function createService(repository) {
  const publisher = {
    calls: 0,
    publishPending() {
      this.calls += 1;
      return Promise.resolve();
    }
  };
  const logger = {
    setContext() {},
    info() {},
    warn() {}
  };

  return {
    service: new OrdersService(repository, publisher, logger),
    publisher
  };
}

test("createOrder сохраняет первый Idempotency-Key запрос и запускает publisher", async () => {
  const response = {
    id: "00000000-0000-4000-8000-000000000001",
    status: OrderStatus.Pending,
    userId: "user-1",
    currency: "USD",
    totalAmount: 20,
    itemCount: 2,
    createdAt: "2026-07-15T00:00:00.000Z"
  };
  const repository = {
    async createPendingOrderWithOutboxIdempotently(params) {
      assert.deepEqual(params.idempotency, {
        key: "create-order-key",
        requestHash: "request-hash"
      });

      return {
        replayed: false,
        order: {
          ...response,
          totalAmount: "20.00"
        },
        event: {
          eventId: "00000000-0000-4000-8000-000000000101",
          eventType: "OrderCreated",
          correlationId: "00000000-0000-4000-8000-000000000201"
        },
        response
      };
    }
  };
  const { service, publisher } = createService(repository);

  assert.deepEqual(
    await service.createOrder(dto, {
      idempotencyKey: "create-order-key",
      requestHash: "request-hash"
    }),
    response
  );
  assert.equal(publisher.calls, 1);
});

test("createOrder возвращает сохраненный response при повторе того же Idempotency-Key", async () => {
  const response = {
    id: "00000000-0000-4000-8000-000000000002",
    status: OrderStatus.Pending,
    userId: "user-1",
    currency: "USD",
    totalAmount: 20,
    itemCount: 2,
    createdAt: "2026-07-15T00:00:00.000Z"
  };
  const repository = {
    async createPendingOrderWithOutboxIdempotently() {
      return {
        replayed: true,
        response
      };
    }
  };
  const { service, publisher } = createService(repository);

  assert.deepEqual(
    await service.createOrder(dto, {
      idempotencyKey: "create-order-key",
      requestHash: "request-hash"
    }),
    response
  );
  assert.equal(publisher.calls, 0);
});

test("createOrder требует key и hash вместе", async () => {
  const { service } = createService({});

  await assert.rejects(
    () =>
      service.createOrder(dto, {
        idempotencyKey: "create-order-key"
      }),
    /Both Idempotency-Key and request hash metadata are required/
  );
});
