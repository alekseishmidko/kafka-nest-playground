import { Injectable, OnModuleInit } from "@nestjs/common";
import {
  EVENT_TOPIC_MAP,
  type OrderCreatedEvent
} from "@kafka-playground/contracts";
import { KafkaConsumerRunner } from "@kafka-playground/kafka";
import { PinoLogger } from "nestjs-pino";
import { RiskService } from "./risk.service";

@Injectable()
export class RiskConsumer implements OnModuleInit {
  constructor(
    private readonly consumerRunner: KafkaConsumerRunner,
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
      async ({ event }) => {
        // В topic order.order-events могут появиться и другие order-события.
        // Сейчас сервис умеет обрабатывать только OrderCreated.
        if (event.eventType === "OrderCreated") {
          // Вся доменная работа вынесена в RiskService, consumer только доставляет событие.
          await this.riskService.handleOrderCreated(event);
        } else {
          this.logger.debug(
              {
                eventType: event.eventType,
                eventId: event.eventId
              },
              "Skipping unsupported order event"
          );
          return;
        }


      }
    );
  }
}
