import { Injectable, OnModuleInit } from "@nestjs/common";
import {
  EVENT_TOPIC_MAP,
  type OrderCreatedEvent
} from "@kafka-playground/contracts";
import {
  KafkaConsumerRunner,
  KafkaIdempotentEventProcessor
} from "@kafka-playground/kafka";
import { PinoLogger } from "@kafka-playground/observability";
import { RiskService } from "./risk.service";

@Injectable()
export class RiskConsumer implements OnModuleInit {
  constructor(
    private readonly consumerRunner: KafkaConsumerRunner,
    private readonly idempotentProcessor: KafkaIdempotentEventProcessor,
    private readonly riskService: RiskService,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(RiskConsumer.name);
  }

  async onModuleInit(): Promise<void> {
    // onModuleInit вызывается NestJS после создания dependency graph.
    // Здесь удобно стартовать Kafka subscription: все зависимости уже готовы.
    await this.consumerRunner.subscribe<OrderCreatedEvent>(
      {
        // Topic берется из общего contracts-пакета, чтобы не дублировать строковые константы.
        topic: EVENT_TOPIC_MAP.OrderCreated
      },
      async (context) => {
        const { event } = context;
        // В topic order.order-events могут появиться и другие order-события.
        // Сейчас сервис умеет обрабатывать только OrderCreated.
        if (event.eventType === "OrderCreated") {
          await this.idempotentProcessor.process(
            context,
            () => this.riskService.prepareRiskEvent(event),
            (riskEvent) => this.riskService.publishRiskEvent(riskEvent)
          );
        } else {
          this.logger.debug(
            {
              eventType: event.eventType,
              eventId: event.eventId
            },
            "Skipping unsupported order event"
          );
        }
      }
    );
  }
}
