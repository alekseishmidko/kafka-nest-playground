import type {
  OrderCancelledEvent,
  OrderConfirmedEvent,
  OrderRiskRejectedEvent,
  PaymentAuthorizedEvent,
  PaymentFailedEvent
} from "@kafka-playground/contracts";
import { randomUUID } from "node:crypto";
import type { OrderEntity } from "./entities/order.entity";

export type FinalOrderEvent = OrderConfirmedEvent | OrderCancelledEvent;
export type FinalOrderSourceEvent =
  | OrderRiskRejectedEvent
  | PaymentAuthorizedEvent
  | PaymentFailedEvent;

/**
 * Создаёт финальное доменное событие заказа из принятого входящего события.
 *
 * Фабрика сохраняет `correlationId` всей бизнес-цепочки и записывает исходный
 * `eventId` в `causationId`. Благодаря этому в логах и tracing можно установить,
 * какое risk/payment событие стало непосредственной причиной завершения заказа.
 */
export function createFinalOrderEvent(
  order: OrderEntity,
  source: FinalOrderSourceEvent,
  occurredAt = new Date().toISOString()
): FinalOrderEvent {
  const envelope = {
    eventId: randomUUID(),
    eventVersion: 1,
    occurredAt,
    correlationId: source.correlationId,
    causationId: source.eventId,
    producer: "order-service"
  };

  if (source.eventType === "PaymentAuthorized") {
    return {
      ...envelope,
      eventType: "OrderConfirmed",
      payload: {
        orderId: order.id,
        userId: order.userId,
        currency: order.currency,
        totalAmount: Number(order.totalAmount),
        paymentId: source.payload.paymentId,
        confirmedAt: occurredAt
      }
    };
  }

  return {
    ...envelope,
    eventType: "OrderCancelled",
    payload: {
      orderId: order.id,
      userId: order.userId,
      reason: source.payload.reason,
      cancelledBy:
        source.eventType === "OrderRiskRejected" ? "risk" : "payment",
      cancelledAt: occurredAt
    }
  };
}
