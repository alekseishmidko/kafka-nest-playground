import { Injectable, OnModuleInit } from "@nestjs/common";
import {
  EVENT_TOPIC_MAP,
  type OrderRiskApprovedEvent,
  type OrderRiskRejectedEvent
} from "@kafka-playground/contracts";
import {
  KafkaConsumerRunner,
  KafkaIdempotentEventProcessor
} from "@kafka-playground/kafka";
import { PinoLogger } from "@kafka-playground/observability";
import { PaymentService } from "./payment.service";

@Injectable()
export class PaymentConsumer implements OnModuleInit {
  constructor(
    private readonly consumerRunner: KafkaConsumerRunner,
    private readonly idempotentProcessor: KafkaIdempotentEventProcessor,
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
      async (context) => {
        const { event } = context;
        if (event.eventType === "OrderRiskApproved") {
          await this.idempotentProcessor.process(
            context,
            () => this.paymentService.preparePaymentEvent(event),
            (paymentEvent) =>
              this.paymentService.publishPaymentEvent(paymentEvent)
          );
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
