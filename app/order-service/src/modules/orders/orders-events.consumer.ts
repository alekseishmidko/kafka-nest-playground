import { Injectable, OnModuleInit } from "@nestjs/common";
import {
  EVENT_TOPIC_MAP,
  type OrderRiskApprovedEvent,
  type OrderRiskRejectedEvent,
  type PaymentAuthorizedEvent,
  type PaymentFailedEvent
} from "@kafka-playground/contracts";
import { KafkaConsumerRunner } from "@kafka-playground/kafka";
import { PinoLogger } from "@kafka-playground/observability";
import { OrdersService } from "./orders.service";

type OrderStatusEvent =
  | OrderRiskApprovedEvent
  | OrderRiskRejectedEvent
  | PaymentAuthorizedEvent
  | PaymentFailedEvent;

@Injectable()
export class OrdersEventsConsumer implements OnModuleInit {
  constructor(
    private readonly consumerRunner: KafkaConsumerRunner,
    private readonly ordersService: OrdersService,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(OrdersEventsConsumer.name);
  }

  async onModuleInit(): Promise<void> {
    await this.consumerRunner.subscribeMany<OrderStatusEvent>(
      {
        topics: [EVENT_TOPIC_MAP.OrderRiskApproved, EVENT_TOPIC_MAP.PaymentAuthorized]
      },
      async ({ event }) => {
        switch (event.eventType) {
          case "OrderRiskApproved":
            await this.ordersService.handleOrderRiskApproved(event);
            return;
          case "OrderRiskRejected":
            await this.ordersService.handleOrderRiskRejected(event);
            return;
          case "PaymentAuthorized":
            await this.ordersService.handlePaymentAuthorized(event);
            return;
          case "PaymentFailed":
            await this.ordersService.handlePaymentFailed(event);
            return;
        }
      }
    );
  }
}
