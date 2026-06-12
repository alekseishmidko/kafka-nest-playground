import {
  Inject,
  Injectable,
  type OnApplicationBootstrap
} from "@nestjs/common";
import {
  KAFKA_TOPICS,
  type DomainEvent,
  type KafkaTopicName
} from "@kafka-playground/contracts";
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

interface KafkaConsumerRegistration {
  topics: KafkaTopicName[];
  fromBeginning: boolean;
  handler: KafkaConsumeHandler;
}

/**
 * Координирует все Kafka-подписки одного NestJS-процесса.
 *
 * KafkaJS допускает только один активный `consumer.run()` для экземпляра
 * consumer-а. Поэтому feature-consumers регистрируют свои handler-ы во время
 * `onModuleInit`, а runner запускает единый loop после инициализации всех
 * модулей. Это позволяет добавлять независимые consumers без гонок запуска.
 */
@Injectable()
export class KafkaConsumerRunner implements OnApplicationBootstrap {
  private readonly retryInMs = 5000;
  private readonly registrations: KafkaConsumerRegistration[] = [];
  private started = false;

  constructor(
    @Inject(KAFKA_CONSUMER_CLIENT)
    private readonly consumer: KafkaConsumerClient,
    @Inject(SCHEMA_REGISTRY_CODEC)
    private readonly codec: SchemaRegistryCodec,
    private readonly logger: KafkaEventLogger,
    private readonly retryPolicy: KafkaRetryPolicy,
    private readonly retryDispatcher: KafkaRetryDispatcher
  ) {}

  /**
   * Запускает единый KafkaJS loop после регистрации NestJS feature-consumers.
   */
  onApplicationBootstrap(): void {
    if (this.registrations.length === 0 || this.started) {
      return;
    }

    this.started = true;
    void this.startWithRetry();
  }

  /**
   * Регистрирует handler одного основного topic.
   */
  async subscribe<TEvent extends DomainEvent>(
    options: {
      topic: KafkaConsumerMessageContext<TEvent>["topic"];
      fromBeginning?: boolean;
    },
    handler: KafkaConsumeHandler<TEvent>
  ): Promise<void> {
    this.register(
      [options.topic],
      options.fromBeginning,
      handler
    );
  }

  /**
   * Регистрирует один handler для нескольких основных topics.
   *
   * Метод намеренно не вызывает `consumer.run()`: запуск до завершения
   * инициализации NestJS не позволил бы безопасно добавить второй consumer.
   */
  async subscribeMany<TEvent extends DomainEvent>(
    options: {
      topics: Array<KafkaConsumerMessageContext<TEvent>["topic"]>;
      fromBeginning?: boolean;
    },
    handler: KafkaConsumeHandler<TEvent>
  ): Promise<void> {
    this.register(
      options.topics,
      options.fromBeginning,
      handler
    );
  }

  private register<TEvent extends DomainEvent>(
    topics: KafkaTopicName[],
    fromBeginning: boolean | undefined,
    handler: KafkaConsumeHandler<TEvent>
  ): void {
    if (this.started) {
      throw new Error(
        "Kafka subscriptions must be registered before application bootstrap"
      );
    }

    const duplicateTopic = topics.find((topic) =>
      this.registrations.some((registration) =>
        registration.topics.includes(topic)
      )
    );

    if (duplicateTopic) {
      throw new Error(
        `Kafka handler is already registered for topic ${duplicateTopic}`
      );
    }

    this.registrations.push({
      topics,
      fromBeginning: fromBeginning ?? false,
      handler: handler as KafkaConsumeHandler
    });
  }

  private async startWithRetry(): Promise<void> {
    const sourceTopics = [
      ...new Set(
        this.registrations.flatMap((registration) => registration.topics)
      )
    ];

    try {
      const subscriptionTopics = [
        ...new Set(
          sourceTopics.flatMap((topic) =>
            this.retryPolicy.getSubscriptionTopics(topic)
          )
        )
      ];

      for (const topic of subscriptionTopics) {
        const fromBeginning = this.registrations.some(
          (registration) =>
            registration.fromBeginning &&
            registration.topics.includes(topic)
        );

        await this.consumer.subscribe({
          topic,
          fromBeginning
        });
      }

      await this.consumer.run({
        eachMessage: async ({ topic, partition, heartbeat, message }) => {
          let event: DomainEvent | undefined;

          try {
            await delayWithHeartbeat(
              this.retryPolicy.getDelayMs(topic),
              heartbeat
            );

            if (!message.value) {
              throw new Error("Kafka message value is empty");
            }

            const decodedEvent =
              await this.codec.deserialize<DomainEvent>(message.value);
            event = decodedEvent;

            const context: KafkaConsumerMessageContext = {
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

            const handler = this.resolveHandler(topic, message.headers);

            if (!handler) {
              throw new Error(
                `Kafka handler is not registered for topic ${topic}`
              );
            }

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

            const context: KafkaConsumerMessageContext = {
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
        topics: sourceTopics,
        retryInMs: this.retryInMs,
        error
      });

      setTimeout(() => {
        void this.startWithRetry();
      }, this.retryInMs);
    }
  }

  /**
   * Находит feature-handler для основного или retry topic.
   *
   * Retry topics общие для нескольких источников. Заголовок
   * `x-original-topic` возвращает сообщение к handler-у того topic, где
   * произошла первая ошибка.
   */
  private resolveHandler(
    currentTopic: KafkaTopicName,
    headers: KafkaConsumerMessageContext["headers"] | undefined
  ): KafkaConsumeHandler | undefined {
    const directRegistration = this.registrations.find(
      (registration) => registration.topics.includes(currentTopic)
    );

    if (directRegistration) {
      return directRegistration.handler;
    }

    const originalTopic = readHeader(
      headers,
      KAFKA_HEADER_NAMES.originalTopic
    );
    const handlerTopic =
      originalTopic && isKafkaTopicName(originalTopic)
        ? originalTopic
        : currentTopic;

    return this.registrations.find((registration) =>
      registration.topics.includes(handlerTopic)
    )?.handler;
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

function isKafkaTopicName(value: string): value is KafkaTopicName {
  return Object.values(KAFKA_TOPICS).includes(value as KafkaTopicName);
}
