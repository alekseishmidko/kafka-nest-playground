export type KafkaHeaderValue = string | Buffer;
export type KafkaHeaders = Record<string, KafkaHeaderValue>;
export type KafkaHeaderInput = Record<string, KafkaHeaderValue | undefined>;

/**
 * Имя Kafka topic.
 *
 * Внутри playground-сервисов сюда передаются значения из
 * `@kafka-playground/contracts`, но сам kafka-пакет намеренно принимает любую
 * строку. Благодаря этому пакет можно использовать в другом проекте со своим
 * каталогом topics без fork-а исходного кода.
 */
export type KafkaTopicName = string;

/**
 * Минимальный event envelope, который нужен Kafka-инфраструктуре.
 *
 * Пакет не знает бизнесовый payload и конкретный список eventType. Сервис может
 * передавать более узкие типы событий через generic-параметры, если у него есть
 * собственный contracts-пакет.
 */
export interface KafkaDomainEvent<TPayload = unknown, TEventType extends string = string> {
  eventId: string;
  eventType: TEventType;
  eventVersion: number;
  occurredAt: string;
  correlationId: string;
  causationId: string | null;
  producer: string;
  payload: TPayload;
}

/**
 * Функция выбора Schema Registry subject для события.
 *
 * По умолчанию используется `${topic}-${eventType}-value`, но проект может
 * передать свой resolver для совместимости с уже существующими subject names.
 */
export interface KafkaSubjectResolver {
  (options: {
    topic: KafkaTopicName;
    eventType: string;
    event: KafkaDomainEvent;
  }): string;
}

export interface KafkaModuleOptions {
  clientId: string;
  serviceName: string;
  brokers: string[];
  schemaRegistryUrl: string;
  /** Consumer group нужна lag collector-у; producer-only сервисы могут не задавать её. */
  consumerGroupId?: string;
  /** Topic для terminal retry-сообщений. По умолчанию используется `dead-letter.events`. */
  deadLetterTopic?: KafkaTopicName;
  /** Позволяет проекту задать собственные имена Avro subjects. */
  subjectResolver?: KafkaSubjectResolver;
}

export interface KafkaProducerClient {
  send(message: KafkaProducerClientMessage): Promise<KafkaProducerRecordMetadata[]>;
}

export interface KafkaProducerClientMessage {
  topic: KafkaTopicName;
  messages: KafkaProducerClientMessageItem[];
}

export interface KafkaProducerClientMessageItem {
  key: string;
  value: Buffer;
  headers?: KafkaHeaderInput;
}

export interface KafkaProducerRecordMetadata {
  topicName?: string;
  topic?: string;
  partition?: number;
  offset?: string;
}

export interface KafkaConsumerClient {
  subscribe(options: { topic: KafkaTopicName; fromBeginning?: boolean }): Promise<void>;
  run(options: {
    eachMessage(message: KafkaEachMessagePayload): Promise<void>;
  }): Promise<void>;
  /** Корректно покидает consumer group и фиксирует обработанные offsets. */
  disconnect?(): Promise<void>;
}

export interface KafkaEachMessagePayload {
  topic: KafkaTopicName;
  partition: number;
  heartbeat(): Promise<void>;
  message: {
    key: Buffer | null;
    value: Buffer | null;
    offset: string;
    headers?: KafkaHeaders;
  };
}

export interface KafkaPublishOptions<TEvent extends KafkaDomainEvent = KafkaDomainEvent> {
  topic: KafkaTopicName;
  key: string;
  event: TEvent;
  correlationId?: string;
  causationId?: string;
  headers?: KafkaHeaders;
}

export interface KafkaConsumeHandler<
  TEvent extends KafkaDomainEvent = KafkaDomainEvent,
  TTopic extends KafkaTopicName = KafkaTopicName
> {
  (context: KafkaConsumerMessageContext<TEvent, TTopic>): Promise<void>;
}

export interface KafkaConsumerMessageContext<
  TEvent extends KafkaDomainEvent = KafkaDomainEvent,
  TTopic extends KafkaTopicName = KafkaTopicName
> {
  topic: TTopic;
  partition: number;
  offset: string;
  key: string | null;
  headers: KafkaHeaders;
  event: TEvent;
  correlationId: string;
  /** Trace id активного consumer span для логов и диагностических API. */
  traceId?: string;
  /** Span id активного consumer span. */
  spanId?: string;
}

export interface SchemaRegistryCodec {
  serialize(subject: string, payload: unknown): Promise<Buffer>;
  deserialize<T>(payload: Buffer): Promise<T>;
}
