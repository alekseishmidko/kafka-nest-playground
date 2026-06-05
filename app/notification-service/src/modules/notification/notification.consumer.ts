import { Injectable, OnModuleInit } from "@nestjs/common";
import {
  EVENT_TOPIC_MAP,
  type NotificationCommandEvent,
  type OrderCreatedEvent,
  type PaymentAuthorizedEvent,
  type PaymentFailedEvent
} from "@kafka-playground/contracts";
import { KafkaConsumerRunner } from "@kafka-playground/kafka";
import { PinoLogger } from "@kafka-playground/observability";
import { NotificationService } from "./notification.service";

type NotificationInputEvent =
  | NotificationCommandEvent
  | OrderCreatedEvent
  | PaymentAuthorizedEvent
  | PaymentFailedEvent;

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
          EVENT_TOPIC_MAP.OrderCreated,
          EVENT_TOPIC_MAP.PaymentAuthorized
        ]
      },
      async ({ event }) => {
        switch (event.eventType) {
          case "NotificationCommand":
            await this.notificationService.handleNotificationCommand(event);
            return;
          case "OrderCreated":
            await this.notificationService.handleOrderCreated(event);
            return;
          case "PaymentAuthorized":
            await this.notificationService.handlePaymentAuthorized(event);
            return;
          case "PaymentFailed":
            await this.notificationService.handlePaymentFailed(event);
            return;
        }
      }
    );
  }
}
