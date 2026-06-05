import { Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import type {
  NotificationCommandEvent,
  OrderCreatedEvent,
  PaymentAuthorizedEvent,
  PaymentFailedEvent
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

  async handleOrderCreated(event: OrderCreatedEvent): Promise<void> {
    await this.delivery.deliver({
      notificationId: `order-created-${event.payload.orderId}`,
      recipient: this.defaultRecipient,
      channel: "email",
      template: "order.created",
      data: {
        orderId: event.payload.orderId,
        userId: event.payload.userId,
        currency: event.payload.currency,
        totalAmount: event.payload.totalAmount,
        itemCount: event.payload.itemCount
      },
      correlationId: event.correlationId,
      causationId: event.eventId
    });
  }

  async handlePaymentAuthorized(event: PaymentAuthorizedEvent): Promise<void> {
    await this.delivery.deliver({
      notificationId: `payment-authorized-${event.payload.orderId}`,
      recipient: this.defaultRecipient,
      channel: "email",
      template: "payment.authorized",
      data: {
        orderId: event.payload.orderId,
        paymentId: event.payload.paymentId,
        amount: event.payload.amount,
        currency: event.payload.currency,
        provider: event.payload.provider
      },
      correlationId: event.correlationId,
      causationId: event.eventId
    });
  }

  async handlePaymentFailed(event: PaymentFailedEvent): Promise<void> {
    await this.delivery.deliver({
      notificationId: `payment-failed-${event.payload.orderId}`,
      recipient: this.defaultRecipient,
      channel: "email",
      template: "payment.failed",
      data: {
        orderId: event.payload.orderId,
        paymentId: event.payload.paymentId,
        reason: event.payload.reason,
        provider: event.payload.provider
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
