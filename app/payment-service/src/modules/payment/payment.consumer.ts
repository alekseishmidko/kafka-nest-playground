import { Injectable, OnModuleInit } from "@nestjs/common";
import {
  EVENT_TOPIC_MAP,
  type OrderRiskApprovedEvent,
  type OrderRiskRejectedEvent
} from "@kafka-playground/contracts";
import { KafkaConsumerRunner } from "@kafka-playground/kafka";
import { PinoLogger } from "nestjs-pino";
import { PaymentService } from "./payment.service";

@Injectable()
export class PaymentConsumer implements OnModuleInit {
  constructor(
    private readonly consumerRunner: KafkaConsumerRunner,
    private readonly paymentService: PaymentService,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(PaymentConsumer.name);
  }

  async onModuleInit(): Promise<void> {
    await this.consumerRunner.subscribe<OrderRiskApprovedEvent | OrderRiskRejectedEvent>(
      {
        topic: EVENT_TOPIC_MAP.OrderRiskApproved
      },
      async ({ event }) => {
        if (event.eventType === "OrderRiskApproved") {
          await this.paymentService.handleOrderRiskApproved(event);
          return;
        }

        this.logger.debug(
          {
            eventType: event.eventType,
            eventId: event.eventId,
            orderId: event.payload.orderId
          },
          "Skipping unsupported risk event"
        );
      }
    );
  }
}
