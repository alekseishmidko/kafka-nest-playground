import { Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import type {
  NotificationCommandEvent,
  OrderCancelledEvent,
  OrderConfirmedEvent
} from "@kafka-playground/contracts";
import { PinoLogger } from "@kafka-playground/observability";
import { NotificationDeliveryService } from "./notification-delivery.service";

@Injectable()
export class NotificationService {
  private readonly defaultRecipient: string;

  constructor(
    config: ConfigService,
    private readonly delivery: NotificationDeliveryService,
    private readonly logger: PinoLogger
  ) {
    this.defaultRecipient =
      config.get<string>("NOTIFICATION_DEFAULT_RECIPIENT") ?? "ops@example.com";
    this.logger.setContext(NotificationService.name);
  }

  async handleNotificationCommand(event: NotificationCommandEvent): Promise<void> {
    await this.delivery.deliver({
      notificationId: event.payload.notificationId,
      recipient: event.payload.recipient,
      channel: normalizeChannel(event.payload.channel),
      template: event.payload.template,
      data: parseDataJson(event.payload.dataJson, this.logger, event.eventId),
      correlationId: event.correlationId,
      causationId: event.eventId
    });
  }

  /**
   * Отправляет уведомление только после того, как order-service зафиксировал
   * заказ в терминальном состоянии `CONFIRMED`.
   */
  async handleOrderConfirmed(event: OrderConfirmedEvent): Promise<void> {
    await this.delivery.deliver({
      notificationId: `order-confirmed-${event.payload.orderId}`,
      recipient: this.defaultRecipient,
      channel: "email",
      template: "order.confirmed",
      data: {
        orderId: event.payload.orderId,
        userId: event.payload.userId,
        currency: event.payload.currency,
        totalAmount: event.payload.totalAmount,
        paymentId: event.payload.paymentId,
        confirmedAt: event.payload.confirmedAt
      },
      correlationId: event.correlationId,
      causationId: event.eventId
    });
  }

  /**
   * Отправляет уведомление о бизнес-отмене заказа независимо от того, была она
   * вызвана risk rejection или отказом платёжного провайдера.
   */
  async handleOrderCancelled(event: OrderCancelledEvent): Promise<void> {
    await this.delivery.deliver({
      notificationId: `order-cancelled-${event.payload.orderId}`,
      recipient: this.defaultRecipient,
      channel: "email",
      template: "order.cancelled",
      data: {
        orderId: event.payload.orderId,
        reason: event.payload.reason,
        cancelledBy: event.payload.cancelledBy,
        cancelledAt: event.payload.cancelledAt
      },
      correlationId: event.correlationId,
      causationId: event.eventId
    });
  }
}

function normalizeChannel(value: string): "email" | "push" | "webhook" {
  if (value === "push" || value === "webhook") {
    return value;
  }

  return "email";
}

function parseDataJson(
  value: string,
  logger: PinoLogger,
  eventId: string
): Record<string, unknown> {
  try {
    const parsed = JSON.parse(value) as unknown;

    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
  } catch (error) {
    logger.warn(
      {
        eventId,
        error
      },
      "Notification command dataJson is not valid JSON object"
    );
  }

  return {};
}
