import { Inject, Injectable } from "@nestjs/common";
import type { DomainEvent } from "@kafka-playground/contracts";
import { KAFKA_CONSUMER_CLIENT, SCHEMA_REGISTRY_CODEC } from "./kafka.tokens";
import { readHeader, KAFKA_HEADER_NAMES } from "./kafka-headers";
import { KafkaEventLogger } from "./kafka-logger";
import type {
  KafkaConsumeHandler,
  KafkaConsumerClient,
  KafkaConsumerMessageContext,
  SchemaRegistryCodec
} from "./types";

@Injectable()
export class KafkaConsumerRunner {
  constructor(
    @Inject(KAFKA_CONSUMER_CLIENT)
    private readonly consumer: KafkaConsumerClient,
    @Inject(SCHEMA_REGISTRY_CODEC)
    private readonly codec: SchemaRegistryCodec,
    private readonly logger: KafkaEventLogger
  ) {}

  async subscribe<TEvent extends DomainEvent>(
    options: {
      topic: KafkaConsumerMessageContext<TEvent>["topic"];
      fromBeginning?: boolean;
    },
    handler: KafkaConsumeHandler<TEvent>
  ): Promise<void> {
    await this.consumer.subscribe({
      topic: options.topic,
      fromBeginning: options.fromBeginning
    });

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        let event: TEvent | undefined;

        try {
          if (!message.value) {
            throw new Error("Kafka message value is empty");
          }

          const decodedEvent = await this.codec.deserialize<TEvent>(message.value);
          event = decodedEvent;

          const context: KafkaConsumerMessageContext<TEvent> = {
            topic,
            partition,
            offset: message.offset,
            key: message.key?.toString("utf8") ?? null,
            headers: message.headers ?? {},
            event: decodedEvent,
            correlationId:
              readHeader(message.headers, KAFKA_HEADER_NAMES.correlationId) ??
              decodedEvent.correlationId
          };

          this.logger.logConsumed({
            topic,
            partition,
            offset: message.offset,
            event: decodedEvent
          });

          await handler(context);
        } catch (error) {
          this.logger.logFailed({
            topic,
            partition,
            offset: message.offset,
            eventType:
              event?.eventType ?? readHeader(message.headers, KAFKA_HEADER_NAMES.eventType),
            eventId: event?.eventId ?? readHeader(message.headers, KAFKA_HEADER_NAMES.eventId),
            error
          });

          throw error;
        }
      }
    });
  }
}
