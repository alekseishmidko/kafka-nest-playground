import { Inject, Injectable } from "@nestjs/common";
import type { DomainEvent } from "@kafka-playground/contracts";
import { KAFKA_CONSUMER_CLIENT, SCHEMA_REGISTRY_CODEC } from "./kafka.tokens";
import { readHeader, KAFKA_HEADER_NAMES } from "./kafka-headers";
import { KafkaEventLogger } from "./kafka-logger";
import { KafkaRetryDispatcher } from "./kafka-retry-dispatcher";
import { KafkaRetryPolicy } from "./kafka-retry-policy";
import type {
  KafkaConsumeHandler,
  KafkaConsumerClient,
  KafkaConsumerMessageContext,
  SchemaRegistryCodec
} from "./types";

@Injectable()
export class KafkaConsumerRunner {
  private readonly retryInMs = 5000;

  constructor(
    @Inject(KAFKA_CONSUMER_CLIENT)
    private readonly consumer: KafkaConsumerClient,
    @Inject(SCHEMA_REGISTRY_CODEC)
    private readonly codec: SchemaRegistryCodec,
    private readonly logger: KafkaEventLogger,
    private readonly retryPolicy: KafkaRetryPolicy,
    private readonly retryDispatcher: KafkaRetryDispatcher
  ) {}

  async subscribe<TEvent extends DomainEvent>(
    options: {
      topic: KafkaConsumerMessageContext<TEvent>["topic"];
      fromBeginning?: boolean;
    },
    handler: KafkaConsumeHandler<TEvent>
  ): Promise<void> {
    await this.subscribeMany(
      {
        topics: [options.topic],
        fromBeginning: options.fromBeginning
      },
      handler
    );
  }

  async subscribeMany<TEvent extends DomainEvent>(
    options: {
      topics: Array<KafkaConsumerMessageContext<TEvent>["topic"]>;
      fromBeginning?: boolean;
    },
    handler: KafkaConsumeHandler<TEvent>
  ): Promise<void> {
    void this.startWithRetry(options, handler);
  }

  private async startWithRetry<TEvent extends DomainEvent>(
    options: {
      topics: Array<KafkaConsumerMessageContext<TEvent>["topic"]>;
      fromBeginning?: boolean;
    },
    handler: KafkaConsumeHandler<TEvent>
  ): Promise<void> {
    try {
      const subscriptionTopics = [
        ...new Set(
          options.topics.flatMap((topic) =>
            this.retryPolicy.getSubscriptionTopics(topic)
          )
        )
      ];

      for (const topic of subscriptionTopics) {
        await this.consumer.subscribe({
          topic,
          fromBeginning: options.fromBeginning
        });
      }

      await this.consumer.run({
        eachMessage: async ({ topic, partition, heartbeat, message }) => {
          let event: TEvent | undefined;

          try {
            await delayWithHeartbeat(
              this.retryPolicy.getDelayMs(topic),
              heartbeat
            );

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

            if (!event) {
              // Без декодированного события невозможно безопасно выбрать Avro
              // subject для retry. Ошибка пробрасывается KafkaJS и offset не
              // фиксируется, чтобы сообщение не было молча потеряно.
              throw error;
            }

            if (!this.retryPolicy.supports(topic, message.headers)) {
              throw error;
            }

            const context: KafkaConsumerMessageContext<TEvent> = {
              topic,
              partition,
              offset: message.offset,
              key: message.key?.toString("utf8") ?? null,
              headers: message.headers ?? {},
              event,
              correlationId:
                readHeader(
                  message.headers,
                  KAFKA_HEADER_NAMES.correlationId
                ) ?? event.correlationId
            };

            await this.retryDispatcher.dispatch({
              context,
              error
            });
          }
        }
      });
    } catch (error) {
      this.logger.logConsumerStartFailed({
        topics: options.topics,
        retryInMs: this.retryInMs,
        error
      });

      setTimeout(() => {
        void this.startWithRetry(options, handler);
      }, this.retryInMs);
    }
  }
}

/**
 * Ожидает retry delay и поддерживает heartbeat consumer group.
 *
 * Обычный `setTimeout(5 минут)` внутри `eachMessage` может превысить session
 * timeout: broker исключит consumer из группы, а сообщение будет обработано
 * повторно после rebalance. Короткие интервалы позволяют регулярно вызывать
 * heartbeat во время ожидания.
 */
async function delayWithHeartbeat(
  delayMs: number,
  heartbeat: () => Promise<void>
): Promise<void> {
  const heartbeatIntervalMs = 3_000;
  let remainingMs = delayMs;

  while (remainingMs > 0) {
    const waitMs = Math.min(heartbeatIntervalMs, remainingMs);

    await sleep(waitMs);
    remainingMs -= waitMs;
    await heartbeat();
  }
}

function sleep(delayMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, delayMs);
  });
}
