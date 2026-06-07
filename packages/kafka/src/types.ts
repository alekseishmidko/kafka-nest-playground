import type { DomainEvent, KafkaTopicName } from "@kafka-playground/contracts";

export type KafkaHeaderValue = string | Buffer;
export type KafkaHeaders = Record<string, KafkaHeaderValue>;
export type KafkaHeaderInput = Record<string, KafkaHeaderValue | undefined>;

export interface KafkaModuleOptions {
  clientId: string;
  serviceName: string;
  brokers: string[];
  schemaRegistryUrl: string;
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
}

export interface KafkaEachMessagePayload {
  topic: KafkaTopicName;
  partition: number;
  message: {
    key: Buffer | null;
    value: Buffer | null;
    offset: string;
    headers?: KafkaHeaders;
  };
}

export interface KafkaPublishOptions {
  topic: KafkaTopicName;
  key: string;
  event: DomainEvent;
  correlationId?: string;
  causationId?: string;
  headers?: KafkaHeaders;
}

export interface KafkaConsumeHandler<TEvent extends DomainEvent = DomainEvent> {
  (context: KafkaConsumerMessageContext<TEvent>): Promise<void>;
}

export interface KafkaConsumerMessageContext<TEvent extends DomainEvent = DomainEvent> {
  topic: KafkaTopicName;
  partition: number;
  offset: string;
  key: string | null;
  headers: KafkaHeaders;
  event: TEvent;
  correlationId: string;
}

export interface SchemaRegistryCodec {
  serialize(subject: string, payload: unknown): Promise<Buffer>;
  deserialize<T>(payload: Buffer): Promise<T>;
}
