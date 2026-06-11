import { Injectable, type OnModuleInit } from "@nestjs/common";
import {
  KAFKA_TOPICS,
  type DeadLetterEvent
} from "@kafka-playground/contracts";
import { KafkaConsumerRunner } from "@kafka-playground/kafka";
import { DlqService } from "./dlq.service";

/**
 * Отдельный feature-consumer для platform topic `dead-letter.events`.
 *
 * Класс отвечает только за Kafka routing. Идемпотентность, парсинг и хранение
 * находятся в `DlqService`/`DlqRepository`.
 */
@Injectable()
export class DlqConsumer implements OnModuleInit {
  constructor(
    private readonly consumerRunner: KafkaConsumerRunner,
    private readonly dlqService: DlqService
  ) {}

  async onModuleInit(): Promise<void> {
    await this.consumerRunner.subscribe<DeadLetterEvent>(
      {
        topic: KAFKA_TOPICS.deadLetterEvents
      },
      async (context) => {
        if (context.event.eventType !== "DeadLetterEvent") {
          return;
        }

        await this.dlqService.capture(context);
      }
    );
  }
}
