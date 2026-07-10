import type {
  OrderCancellationRejectedEvent,
  OrderCancellationRequestedEvent,
  OrderCancelledEvent
} from "@kafka-playground/contracts";
import { randomUUID } from "node:crypto";
import { OrderEntity, OrderStatus } from "./entities/order.entity";

export type OrderCancellationRequester = "user" | "operator";

export interface OrderCancellationCommand {
  orderId: string;
  reason: string;
  requestedBy: OrderCancellationRequester;
}

export interface AcceptedOrderCancellationDecision {
  accepted: true;
  from: OrderStatus;
  to: OrderStatus.Cancelled;
}

export interface RejectedOrderCancellationDecision {
  accepted: false;
  from: OrderStatus;
  rejectedReason: "already_cancelled" | "already_confirmed" | "invalid_status";
}

export type OrderCancellationDecision =
  | AcceptedOrderCancellationDecision
  | RejectedOrderCancellationDecision;

/**
 * Чистое правило пользовательской/операторской отмены заказа.
 *
 * Пока в проекте нет fulfillment-модели, `SHIPPED` отсутствует как статус. Для
 * будущего shipping-flow правило останется тем же: после подтверждения заказа
 * синхронная отмена запрещена, дальше нужен отдельный refund/return process.
 */
export function decideOrderCancellation(
  currentStatus: OrderStatus
): OrderCancellationDecision {
  if (
    currentStatus === OrderStatus.Pending ||
    currentStatus === OrderStatus.RiskApproved
  ) {
    return {
      accepted: true,
      from: currentStatus,
      to: OrderStatus.Cancelled
    };
  }

  if (currentStatus === OrderStatus.Cancelled) {
    return {
      accepted: false,
      from: currentStatus,
      rejectedReason: "already_cancelled"
    };
  }

  if (currentStatus === OrderStatus.Confirmed) {
    return {
      accepted: false,
      from: currentStatus,
      rejectedReason: "already_confirmed"
    };
  }

  return {
    accepted: false,
    from: currentStatus,
    rejectedReason: "invalid_status"
  };
}

/**
 * Создаёт событие факта получения команды отмены.
 */
export function createOrderCancellationRequestedEvent(
  order: OrderEntity,
  command: OrderCancellationCommand,
  occurredAt: string,
  correlationId = randomUUID()
): OrderCancellationRequestedEvent {
  return {
    eventId: randomUUID(),
    eventType: "OrderCancellationRequested",
    eventVersion: 1,
    occurredAt,
    correlationId,
    causationId: null,
    producer: "order-service",
    payload: {
      orderId: order.id,
      userId: order.userId,
      reason: command.reason,
      requestedBy: command.requestedBy,
      requestedAt: occurredAt
    }
  };
}

/**
 * Создаёт финальное событие успешной отмены заказа.
 */
export function createUserOrderCancelledEvent(
  order: OrderEntity,
  command: OrderCancellationCommand,
  requestedEvent: OrderCancellationRequestedEvent,
  occurredAt: string
): OrderCancelledEvent {
  return {
    eventId: randomUUID(),
    eventType: "OrderCancelled",
    eventVersion: 1,
    occurredAt,
    correlationId: requestedEvent.correlationId,
    causationId: requestedEvent.eventId,
    producer: "order-service",
    payload: {
      orderId: order.id,
      userId: order.userId,
      reason: command.reason,
      cancelledBy: command.requestedBy,
      cancelledAt: occurredAt
    }
  };
}

/**
 * Создаёт событие отказа в отмене, если state machine не разрешила переход.
 */
export function createOrderCancellationRejectedEvent(
  order: OrderEntity,
  command: OrderCancellationCommand,
  requestedEvent: OrderCancellationRequestedEvent,
  rejectedReason: string,
  occurredAt: string
): OrderCancellationRejectedEvent {
  return {
    eventId: randomUUID(),
    eventType: "OrderCancellationRejected",
    eventVersion: 1,
    occurredAt,
    correlationId: requestedEvent.correlationId,
    causationId: requestedEvent.eventId,
    producer: "order-service",
    payload: {
      orderId: order.id,
      userId: order.userId,
      reason: command.reason,
      requestedBy: command.requestedBy,
      currentStatus: order.status,
      rejectedReason,
      rejectedAt: occurredAt
    }
  };
}
