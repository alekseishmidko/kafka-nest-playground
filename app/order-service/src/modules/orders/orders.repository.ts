import { ConflictException, Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { EntityManager, Repository } from "typeorm";
import {
  captureActiveTraceContext,
  runInTraceSpan,
  SpanKind
} from "@kafka-playground/observability";
import {
  EVENT_TOPIC_MAP,
  type KafkaTopicName,
  type OrderCancellationRejectedEvent,
  type OrderCancellationRequestedEvent,
  type OrderCancelledEvent,
  type OrderCreatedEvent
} from "@kafka-playground/contracts";
import {
  createOutboxEventEntity
} from "@kafka-playground/outbox";
import { OrderEntity, OrderStatus, type OrderItemSnapshot } from "./entities/order.entity";
import {
  createFinalOrderEvent,
  type FinalOrderEvent,
  type FinalOrderSourceEvent
} from "./order-final-event.factory";
import {
  decideOrderTransition,
  type OrderLifecycleEvent
} from "./order-state-machine";
import {
  createOrderCancellationRejectedEvent,
  createOrderCancellationRequestedEvent,
  createUserOrderCancelledEvent,
  decideOrderCancellation,
  type OrderCancellationCommand
} from "./order-cancellation";

export interface CreatePendingOrderParams {
  userId: string;
  currency: string;
  totalAmount: number;
  itemCount: number;
  items: OrderItemSnapshot[];
}

/**
 * Параметры для создания заказа через transactional outbox.
 *
 * `createEvent` вызывается уже после сохранения заказа, потому что `orderId`
 * генерируется базой данных и нужен внутри `OrderCreated.payload`.
 */
export interface CreatePendingOrderWithOutboxParams extends CreatePendingOrderParams {
  createEvent(order: OrderEntity): OrderCreatedEvent;
}

export interface CreateOrderIdempotencyParams {
  key: string;
  requestHash: string;
}

export interface CreateOrderResponseSnapshot {
  id: string;
  status: OrderStatus;
  userId: string;
  currency: string;
  totalAmount: number;
  itemCount: number;
  createdAt: string;
}

export type CreatePendingOrderWithOutboxResult =
  | {
      replayed: false;
      order: OrderEntity;
      event: OrderCreatedEvent;
      response: CreateOrderResponseSnapshot;
    }
  | {
      replayed: true;
      response: CreateOrderResponseSnapshot;
    };

/**
 * Параметры идемпотентной обработки lifecycle-события заказа.
 */
export interface ProcessLifecycleEventParams {
  orderId: string;
  event: OrderLifecycleEvent;
  sourceTopic: KafkaTopicName;
  sourceOffset: string;
}

/**
 * Результат транзакционной обработки входящего lifecycle-события.
 */
export type OrderLifecycleProcessingResult =
  | {
      outcome: "APPLIED";
      previousStatus: OrderStatus;
      status: OrderStatus;
      finalEvent: FinalOrderEvent | null;
    }
  | {
      outcome: "DUPLICATE_EVENT";
    }
  | {
      outcome: "UNKNOWN_ORDER";
    }
  | {
      outcome: "INVALID_TRANSITION";
      currentStatus: OrderStatus;
    };

export type OrderCancellationProcessingResult =
  | {
      outcome: "ACCEPTED";
      previousStatus: OrderStatus;
      status: OrderStatus.Cancelled;
      requestedEvent: OrderCancellationRequestedEvent;
      finalEvent: OrderCancelledEvent;
    }
  | {
      outcome: "REJECTED";
      status: OrderStatus;
      requestedEvent: OrderCancellationRequestedEvent;
      rejectedEvent: OrderCancellationRejectedEvent;
    }
  | {
      outcome: "UNKNOWN_ORDER";
    };

@Injectable()
export class OrdersRepository {
  constructor(
    @InjectRepository(OrderEntity)
    private readonly repository: Repository<OrderEntity>
  ) {}

  async createPendingOrder(params: CreatePendingOrderParams): Promise<OrderEntity> {
    const order = this.repository.create({
      userId: params.userId,
      currency: params.currency,
      totalAmount: params.totalAmount.toFixed(2),
      itemCount: params.itemCount,
      status: OrderStatus.Pending,
      items: params.items
    });

    return this.repository.save(order);
  }

  /**
   * Создаёт заказ и outbox-запись `OrderCreated` в одной DB-транзакции.
   *
   * Это закрывает главный риск последовательности "сохранил заказ -> отправил
   * Kafka": если процесс упадёт после commit, outbox-запись останется в БД и
   * будет опубликована фоновым publisher-ом после рестарта.
   */
  async createPendingOrderWithOutbox(
    params: CreatePendingOrderWithOutboxParams
  ): Promise<{ order: OrderEntity; event: OrderCreatedEvent }> {
    return runInTraceSpan(
      "postgres transaction create order",
      {
        kind: SpanKind.INTERNAL,
        attributes: {
          "db.system": "postgresql",
          "db.operation.name": "transaction",
          "outbox.event.type": "OrderCreated"
        }
      },
      () =>
        this.repository.manager.transaction(async (manager) => {
          return this.persistPendingOrderWithOutbox(manager, params);
        })
    );
  }

  /**
   * Идемпотентно создаёт заказ для публичного `POST /orders`.
   *
   * `idempotency_key` вставляется до создания заказа. PostgreSQL unique index
   * сериализует параллельные запросы с одним ключом: один запрос создаёт заказ,
   * остальные читают сохранённый response и не пишут второй `OrderCreated`.
   */
  async createPendingOrderWithOutboxIdempotently(
    params: CreatePendingOrderWithOutboxParams & {
      idempotency: CreateOrderIdempotencyParams;
    }
  ): Promise<CreatePendingOrderWithOutboxResult> {
    return runInTraceSpan(
      "postgres transaction create order idempotently",
      {
        kind: SpanKind.INTERNAL,
        attributes: {
          "db.system": "postgresql",
          "db.operation.name": "transaction",
          "outbox.event.type": "OrderCreated",
          "idempotency.key": params.idempotency.key
        }
      },
      () =>
        this.repository.manager.transaction(async (manager) => {
          const inserted = await manager.query(
            `
              insert into order_create_idempotency_keys (
                idempotency_key,
                request_hash
              )
              values ($1, $2)
              on conflict (idempotency_key) do nothing
              returning idempotency_key
            `,
            [params.idempotency.key, params.idempotency.requestHash]
          );

          if (inserted.length === 0) {
            const existing = await readIdempotencyRow(
              manager,
              params.idempotency.key
            );

            if (!existing) {
              throw new ConflictException(
                "Idempotency key exists but could not be read"
              );
            }

            if (existing.request_hash !== params.idempotency.requestHash) {
              throw new ConflictException(
                "Idempotency-Key was already used with a different request body"
              );
            }

            if (!existing.response) {
              throw new ConflictException(
                "Idempotency-Key request is still being processed"
              );
            }

            return {
              replayed: true,
              response: existing.response as CreateOrderResponseSnapshot
            };
          }

          const { order, event } = await this.persistPendingOrderWithOutbox(
            manager,
            params
          );
          const response = createOrderResponseSnapshot(order);

          await manager.query(
            `
              update order_create_idempotency_keys
              set
                response = $2::jsonb,
                order_id = $3,
                updated_at = now()
              where idempotency_key = $1
            `,
            [params.idempotency.key, JSON.stringify(response), order.id]
          );

          return {
            replayed: false,
            order,
            event,
            response
          };
        })
    );
  }

  /**
   * Идемпотентно применяет lifecycle-событие и при необходимости создаёт
   * финальное событие в transactional outbox.
   *
   * Вся последовательность выполняется в одной PostgreSQL-транзакции:
   *
   * 1. Регистрируем входной `eventId`.
   * 2. Блокируем строку заказа через `pessimistic_write`.
   * 3. Проверяем переход чистой state machine.
   * 4. Обновляем статус.
   * 5. Сохраняем `OrderConfirmed`/`OrderCancelled` в outbox.
   *
   * Row lock сериализует разные события одного заказа. Без него два consumer
   * callback-а могли бы одновременно прочитать старый статус и оба принять
   * несовместимые решения.
   */
  async processLifecycleEvent(
    params: ProcessLifecycleEventParams
  ): Promise<OrderLifecycleProcessingResult> {
    return runInTraceSpan(
      "postgres transaction process order event",
      {
        kind: SpanKind.INTERNAL,
        attributes: {
          "db.system": "postgresql",
          "db.operation.name": "transaction",
          "event.type": params.event.eventType,
          "event.id": params.event.eventId,
          "order.id": params.orderId
        }
      },
      () => this.repository.manager.transaction(async (manager) => {
      const inserted = await manager.query(
        `
          insert into processed_kafka_events (
            event_id,
            event_type,
            source_topic,
            source_offset
          )
          values ($1, $2, $3, $4)
          on conflict (event_id) do nothing
          returning id
        `,
        [
          params.event.eventId,
          params.event.eventType,
          params.sourceTopic,
          params.sourceOffset
        ]
      );

      if (inserted.length === 0) {
        return {
          outcome: "DUPLICATE_EVENT"
        };
      }

      const order = await manager.findOne(OrderEntity, {
        where: {
          id: params.orderId
        },
        lock: {
          mode: "pessimistic_write"
        }
      });

      if (!order) {
        return {
          outcome: "UNKNOWN_ORDER"
        };
      }

      const decision = decideOrderTransition(
        order.status,
        params.event.eventType
      );

      if (!decision.allowed) {
        return {
          outcome: "INVALID_TRANSITION",
          currentStatus: order.status
        };
      }

      const previousStatus = order.status;
      order.status = decision.to;
      const updatedOrder = await manager.save(order);
      const finalEvent =
        decision.finalEventType === null
          ? null
          : createFinalOrderEvent(
              updatedOrder,
              asFinalOrderSourceEvent(params.event)
            );

      if (finalEvent) {
        await manager.save(
          createOutboxEventEntity({
            topic: EVENT_TOPIC_MAP[finalEvent.eventType],
            messageKey: updatedOrder.id,
            event: finalEvent,
            traceContext: captureActiveTraceContext()
          })
        );
      }

      return {
        outcome: "APPLIED",
        previousStatus,
        status: updatedOrder.status,
        finalEvent
      };
      })
    );
  }

  /**
   * Обрабатывает пользовательскую/операторскую команду отмены заказа.
   *
   * Запрос отмены всегда фиксируется событием `OrderCancellationRequested`.
   * Если текущий статус разрешает отмену, статус заказа меняется на
   * `CANCELLED` и сохраняется `OrderCancelled`. Если переход запрещён,
   * сохраняется `OrderCancellationRejected`. Все изменения выполняются в одной
   * транзакции с outbox-записями.
   */
  async cancelOrder(
    command: OrderCancellationCommand
  ): Promise<OrderCancellationProcessingResult> {
    return runInTraceSpan(
      "postgres transaction cancel order",
      {
        kind: SpanKind.INTERNAL,
        attributes: {
          "db.system": "postgresql",
          "db.operation.name": "transaction",
          "order.id": command.orderId,
          "order.cancellation.requested_by": command.requestedBy
        }
      },
      () => this.repository.manager.transaction(async (manager) => {
        const order = await manager.findOne(OrderEntity, {
          where: { id: command.orderId },
          lock: { mode: "pessimistic_write" }
        });

        if (!order) {
          return { outcome: "UNKNOWN_ORDER" };
        }

        const occurredAt = new Date().toISOString();
        const requestedEvent = createOrderCancellationRequestedEvent(
          order,
          command,
          occurredAt
        );
        const decision = decideOrderCancellation(order.status);

        await manager.save(
          createOutboxEventEntity({
            topic: EVENT_TOPIC_MAP.OrderCancellationRequested,
            messageKey: order.id,
            event: requestedEvent,
            traceContext: captureActiveTraceContext()
          })
        );

        if (!decision.accepted) {
          const rejectedEvent = createOrderCancellationRejectedEvent(
            order,
            command,
            requestedEvent,
            decision.rejectedReason,
            occurredAt
          );

          await manager.save(
            createOutboxEventEntity({
              topic: EVENT_TOPIC_MAP.OrderCancellationRejected,
              messageKey: order.id,
              event: rejectedEvent,
              traceContext: captureActiveTraceContext()
            })
          );

          return {
            outcome: "REJECTED",
            status: order.status,
            requestedEvent,
            rejectedEvent
          };
        }

        const previousStatus = order.status;
        order.status = decision.to;
        const updatedOrder = await manager.save(order);
        const finalEvent = createUserOrderCancelledEvent(
          updatedOrder,
          command,
          requestedEvent,
          occurredAt
        );

        await manager.save(
          createOutboxEventEntity({
            topic: EVENT_TOPIC_MAP.OrderCancelled,
            messageKey: updatedOrder.id,
            event: finalEvent,
            traceContext: captureActiveTraceContext()
          })
        );

        return {
          outcome: "ACCEPTED",
          previousStatus,
          status: OrderStatus.Cancelled,
          requestedEvent,
          finalEvent
        };
      })
    );
  }

  private async persistPendingOrderWithOutbox(
    manager: EntityManager,
    params: CreatePendingOrderWithOutboxParams
  ): Promise<{ order: OrderEntity; event: OrderCreatedEvent }> {
    const order = manager.create(OrderEntity, {
      userId: params.userId,
      currency: params.currency,
      totalAmount: params.totalAmount.toFixed(2),
      itemCount: params.itemCount,
      status: OrderStatus.Pending,
      items: params.items
    });
    const savedOrder = await manager.save(order);
    const event = params.createEvent(savedOrder);

    await manager.save(
      createOutboxEventEntity({
        topic: EVENT_TOPIC_MAP.OrderCreated,
        messageKey: savedOrder.id,
        event,
        traceContext: captureActiveTraceContext()
      })
    );

    return {
      order: savedOrder,
      event
    };
  }
}

async function readIdempotencyRow(
  manager: EntityManager,
  key: string
): Promise<{ request_hash: string; response: unknown | null } | null> {
  const result = await manager.query(
    `
      select request_hash, response
      from order_create_idempotency_keys
      where idempotency_key = $1
      for update
    `,
    [key]
  );

  return result[0] ?? null;
}

function createOrderResponseSnapshot(
  order: OrderEntity
): CreateOrderResponseSnapshot {
  return {
    id: order.id,
    status: order.status,
    userId: order.userId,
    currency: order.currency,
    totalAmount: Number(order.totalAmount),
    itemCount: order.itemCount,
    createdAt: order.createdAt.toISOString()
  };
}

/**
 * Сужает lifecycle union до событий, которые действительно завершают заказ.
 *
 * Проверка остаётся рядом с транзакционной логикой, поэтому добавление нового
 * финального перехода потребует явного обновления этого guard-а.
 */
function asFinalOrderSourceEvent(
  event: OrderLifecycleEvent
): FinalOrderSourceEvent {
  if (
    event.eventType === "OrderRiskRejected" ||
    event.eventType === "PaymentAuthorized" ||
    event.eventType === "PaymentFailed"
  ) {
    return event;
  }

  throw new Error(
    `Event ${event.eventType} cannot produce a final order event`
  );
}
