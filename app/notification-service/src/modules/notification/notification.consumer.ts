import { Injectable, OnModuleInit } from "@nestjs/common";
import {
  EVENT_TOPIC_MAP,
  type NotificationCommandEvent,
  type OrderCancelledEvent,
  type OrderConfirmedEvent
} from "@kafka-playground/contracts";
import { KafkaConsumerRunner } from "@kafka-playground/kafka";
import { PinoLogger } from "@kafka-playground/observability";
import { NotificationService } from "./notification.service";

type NotificationInputEvent =
  | NotificationCommandEvent
  | OrderConfirmedEvent
  | OrderCancelledEvent;

/**
 * Подписывает notification-service на явные notification-команды и финальные
 * события заказа.
 *
 * Сервис намеренно не реагирует на `PaymentAuthorized`/`PaymentFailed`: это
 * технические события payment-домена. Пользовательское уведомление должно
 * опираться на итоговое решение владельца заказа.
 */
@Injectable()
export class NotificationConsumer implements OnModuleInit {
  constructor(
    private readonly consumerRunner: KafkaConsumerRunner,
    private readonly notificationService: NotificationService,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(NotificationConsumer.name);
  }

  async onModuleInit(): Promise<void> {
    await this.consumerRunner.subscribeMany<NotificationInputEvent>(
      {
        topics: [
          EVENT_TOPIC_MAP.NotificationCommand,
          EVENT_TOPIC_MAP.OrderConfirmed
        ]
      },
      async ({ event }) => {
        switch (event.eventType) {
          case "NotificationCommand":
            await this.notificationService.handleNotificationCommand(event);
            return;
          case "OrderConfirmed":
            await this.notificationService.handleOrderConfirmed(event);
            return;
          case "OrderCancelled":
            await this.notificationService.handleOrderCancelled(event);
            return;
        }
      }
    );
  }
}
