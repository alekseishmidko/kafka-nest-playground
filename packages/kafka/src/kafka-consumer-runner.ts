import {
  Inject,
  Injectable,
  Optional,
  type OnApplicationBootstrap,
  type OnApplicationShutdown
} from "@nestjs/common";
import {
  KAFKA_TOPICS,
  type DomainEvent,
  type KafkaTopicName
} from "@kafka-playground/contracts";
import {
  ApplicationMetrics,
  extractTraceContext,
  getActiveTraceLogFields,
  markActiveSpanAsFailed,
  runInTraceSpan,
  SpanKind
} from "@kafka-playground/observability";
import { KafkaNonRetryableError } from "./kafka-errors";
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
export class KafkaConsumerRunner
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly retryInMs = 5000;
  private readonly registrations: KafkaConsumerRegistration[] = [];
  private started = false;
  private stopping = false;

  constructor(
    @Inject(KAFKA_CONSUMER_CLIENT)
    private readonly consumer: KafkaConsumerClient,
    @Inject(SCHEMA_REGISTRY_CODEC)
    private readonly codec: SchemaRegistryCodec,
    private readonly logger: KafkaEventLogger,
    private readonly retryPolicy: KafkaRetryPolicy,
    private readonly retryDispatcher: KafkaRetryDispatcher,
    @Optional()
    private readonly metrics?: ApplicationMetrics
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
   * Штатно закрывает Kafka consumer, чтобы broker быстрее завершил rebalance,
   * а KafkaJS зафиксировал уже обработанные offsets.
   */
  async onApplicationShutdown(): Promise<void> {
    this.stopping = true;
    await this.consumer.disconnect?.();
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

  /**
   * Возвращает фактически используемые topics для lag collector-а.
   */
  getSubscriptionTopics(): KafkaTopicName[] {
    return [
      ...new Set(
        this.registrations.flatMap((registration) =>
          registration.topics.flatMap((topic) =>
            this.retryPolicy.getSubscriptionTopics(topic)
          )
        )
      )
    ];
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
          const parentContext = extractTraceContext(message.headers);

          await runInTraceSpan(
            `${topic} process`,
            {
              kind: SpanKind.CONSUMER,
              parentContext,
              attributes: {
                "messaging.system": "kafka",
                "messaging.destination.name": topic,
                "messaging.operation.name": "process",
                "messaging.kafka.destination.partition": partition,
                "messaging.kafka.message.offset": message.offset,
                "messaging.kafka.message.key":
                  message.key?.toString("utf8") ?? ""
              }
            },
            async () => {
              let event: DomainEvent | undefined;
              let startedAt: bigint | undefined;

              try {
                await delayWithHeartbeat(
                  this.retryPolicy.getDelayMs(topic),
                  heartbeat
                );
                startedAt = process.hrtime.bigint();

                if (!message.value) {
                  throw new Error("Kafka message value is empty");
                }

                const decodedEvent =
                  await this.codec.deserialize<DomainEvent>(message.value);
                event = decodedEvent;
                const traceFields = getActiveTraceLogFields();
                const consumerContext: KafkaConsumerMessageContext = {
                  topic,
                  partition,
                  offset: message.offset,
                  key: message.key?.toString("utf8") ?? null,
                  headers: message.headers ?? {},
                  event: decodedEvent,
                  correlationId:
                    readHeader(
                      message.headers,
                      KAFKA_HEADER_NAMES.correlationId
                    ) ?? decodedEvent.correlationId,
                  ...traceFields
                };

                this.logger.logConsumed({
                  topic,
                  partition,
                  offset: message.offset,
                  event: decodedEvent
                });

                const handler = this.resolveHandler(
                  topic,
                  message.headers
                );

                if (!handler) {
                  throw new Error(
                    `Kafka handler is not registered for topic ${topic}`
                  );
                }

                await handler(consumerContext);

                this.metrics?.recordKafkaConsumed(
                  topic,
                  decodedEvent.eventType
                );
                this.metrics?.observeKafkaProcessing({
                  topic,
                  eventType: decodedEvent.eventType,
                  result: "success",
                  durationSeconds: elapsedSeconds(startedAt)
                });
              } catch (error) {
                markActiveSpanAsFailed(error);
                const eventType =
                  event?.eventType ??
                  readHeader(
                    message.headers,
                    KAFKA_HEADER_NAMES.eventType
                  ) ??
                  "UNKNOWN_EVENT";

                this.metrics?.recordKafkaFailed(
                  topic,
                  eventType,
                  getErrorCode(error)
                );
                this.metrics?.observeKafkaProcessing({
                  topic,
                  eventType,
                  result: "failure",
                  durationSeconds: elapsedSeconds(startedAt)
                });
                this.logger.logFailed({
                  topic,
                  partition,
                  offset: message.offset,
                  eventType:
                    event?.eventType ??
                    readHeader(
                      message.headers,
                      KAFKA_HEADER_NAMES.eventType
                    ),
                  eventId:
                    event?.eventId ??
                    readHeader(
                      message.headers,
                      KAFKA_HEADER_NAMES.eventId
                    ),
                  error
                });

                if (!event) {
                  throw error;
                }

                if (!this.retryPolicy.supports(topic, message.headers)) {
                  throw error;
                }

                const traceFields = getActiveTraceLogFields();
                const consumerContext: KafkaConsumerMessageContext = {
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
                    ) ?? event.correlationId,
                  ...traceFields
                };

                await this.retryDispatcher.dispatch({
                  context: consumerContext,
                  error
                });
              }
            }
          );
        }
      });
    } catch (error) {
      if (this.stopping) {
        return;
      }

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

function elapsedSeconds(startedAt: bigint | undefined): number {
  if (!startedAt) {
    return 0;
  }

  return Number(process.hrtime.bigint() - startedAt) / 1_000_000_000;
}

function getErrorCode(error: unknown): string {
  if (error instanceof KafkaNonRetryableError) {
    return error.errorCode;
  }

  if (error instanceof Error && error.name) {
    return error.name
      .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
      .replace(/[^a-zA-Z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "")
      .toUpperCase();
  }

  return "UNKNOWN_ERROR";
}
