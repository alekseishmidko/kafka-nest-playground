import type {
  KafkaDomainEvent,
  KafkaTopicName
} from "@kafka-playground/kafka";
import type { TraceCarrier } from "@kafka-playground/observability";
import type {
  OutboxEventEntity,
  OutboxEventStatus
} from "./outbox-event.entity";

/**
 * Данные для постановки сообщения в transactional outbox.
 */
export interface CreateOutboxEventParams<
  TEvent extends KafkaDomainEvent = KafkaDomainEvent
> {
  /** Topic, определённый контрактами события или локальной routing policy. */
  topic: KafkaTopicName;
  /** Message key, обычно id агрегата, по которому нужен ordering. */
  messageKey: string;
  /** Полный domain envelope, который будет опубликован без пересборки. */
  event: TEvent;
  /** Технический trace context исходной операции. */
  traceContext?: TraceCarrier | null;
}

/**
 * Хранилище сообщений, записываемых в одной транзакции с бизнес-данными.
 *
 * Это producer-side пара к consumer-side inbox. Сервис сначала меняет свою БД
 * и сохраняет outbox-запись, затем отдельный publisher доставляет сообщение в
 * broker. Такой контракт переносим между доменами: order, billing или catalog
 * отличаются только event envelope и topic names.
 */
export interface TransactionalMessageStore<
  TMessage extends OutboxEventEntity = OutboxEventEntity
> {
  createPending(params: CreateOutboxEventParams): TMessage;
  findPublishable(limit: number): Promise<TMessage[]>;
  markPublished(id: string): Promise<void>;
  markFailed(id: string, attempts: number, error: unknown): Promise<void>;
  countByStatuses(): Promise<Record<OutboxEventStatus, number>>;
}

/**
 * Transport-agnostic publisher для уже сохранённого outbox-события.
 *
 * Основной проект использует Kafka, но контракт не заставляет outbox знать о
 * KafkaJS, Schema Registry или NestJS producer service. Для другого проекта
 * можно реализовать adapter под RabbitMQ, NATS или HTTP webhook.
 */
export interface MessagePublisher {
  publish(message: {
    topic: KafkaTopicName;
    key: string;
    event: KafkaDomainEvent;
    correlationId?: string;
    causationId?: string;
  }): Promise<void>;
}
