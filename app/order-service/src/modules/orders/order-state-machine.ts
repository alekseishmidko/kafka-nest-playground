import type {
  OrderRiskApprovedEvent,
  OrderRiskRejectedEvent,
  PaymentAuthorizedEvent,
  PaymentFailedEvent
} from "@kafka-playground/contracts";
import { OrderStatus } from "./entities/order.entity";

export type OrderLifecycleEvent =
  | OrderRiskApprovedEvent
  | OrderRiskRejectedEvent
  | PaymentAuthorizedEvent
  | PaymentFailedEvent;

export type OrderLifecycleEventType = OrderLifecycleEvent["eventType"];

export interface AllowedOrderTransition {
  allowed: true;
  from: OrderStatus;
  to: OrderStatus;
  finalEventType: "OrderConfirmed" | "OrderCancelled" | null;
}

export interface RejectedOrderTransition {
  allowed: false;
  from: OrderStatus;
  eventType: OrderLifecycleEventType;
  reason: "invalid_transition";
}

export type OrderTransitionDecision =
  | AllowedOrderTransition
  | RejectedOrderTransition;

/**
 * Описывает допустимые переходы жизненного цикла заказа.
 *
 * Функция является чистой: она не обращается к базе данных, Kafka и системному
 * времени. Один и тот же набор аргументов всегда возвращает одинаковый ответ,
 * поэтому правила удобно проверять модульными тестами.
 *
 * Финальные состояния `CONFIRMED` и `CANCELLED` намеренно не имеют исходящих
 * переходов. Позднее или пришедшее не по порядку событие будет отклонено и не
 * сможет вернуть завершённый заказ в промежуточное состояние.
 */
export function decideOrderTransition(
  currentStatus: OrderStatus,
  eventType: OrderLifecycleEventType
): OrderTransitionDecision {
  if (
    currentStatus === OrderStatus.Pending &&
    eventType === "OrderRiskApproved"
  ) {
    return allowed(currentStatus, OrderStatus.RiskApproved, null);
  }

  if (
    currentStatus === OrderStatus.Pending &&
    eventType === "OrderRiskRejected"
  ) {
    return allowed(
      currentStatus,
      OrderStatus.Cancelled,
      "OrderCancelled"
    );
  }

  if (
    currentStatus === OrderStatus.RiskApproved &&
    eventType === "PaymentAuthorized"
  ) {
    return allowed(
      currentStatus,
      OrderStatus.Confirmed,
      "OrderConfirmed"
    );
  }

  if (
    currentStatus === OrderStatus.RiskApproved &&
    eventType === "PaymentFailed"
  ) {
    return allowed(
      currentStatus,
      OrderStatus.Cancelled,
      "OrderCancelled"
    );
  }

  return {
    allowed: false,
    from: currentStatus,
    eventType,
    reason: "invalid_transition"
  };
}

function allowed(
  from: OrderStatus,
  to: OrderStatus,
  finalEventType: AllowedOrderTransition["finalEventType"]
): AllowedOrderTransition {
  return {
    allowed: true,
    from,
    to,
    finalEventType
  };
}
