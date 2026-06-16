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

type OrderStatusTopic =
  | typeof EVENT_TOPIC_MAP.OrderRiskApproved
  | typeof EVENT_TOPIC_MAP.PaymentAuthorized;

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
    await this.consumerRunner.subscribeMany<OrderStatusEvent, OrderStatusTopic>(
      {
        topics: [EVENT_TOPIC_MAP.OrderRiskApproved, EVENT_TOPIC_MAP.PaymentAuthorized]
      },
      async ({ event, topic, offset }) => {
        const source = { topic, offset };

        switch (event.eventType) {
          case "OrderRiskApproved":
            await this.ordersService.handleOrderRiskApproved(event, source);
            return;
          case "OrderRiskRejected":
            await this.ordersService.handleOrderRiskRejected(event, source);
            return;
          case "PaymentAuthorized":
            await this.ordersService.handlePaymentAuthorized(event, source);
            return;
          case "PaymentFailed":
            await this.ordersService.handlePaymentFailed(event, source);
            return;
        }
      }
    );
  }
}
