/**
 * Универсальное ядро kafka-пакета.
 *
 * Этот entrypoint не требует NestJS-модуля приложения и не зависит от
 * PostgreSQL-адаптера. Его можно импортировать в unit-тестах, CLI-скриптах или
 * другом проекте, которому нужны только типы, headers, ошибки и retry-policy.
 */
export * from "../kafka-errors";
export * from "../kafka-headers";
export * from "../retry/configurable-kafka-retry-policy";
export type {
  KafkaConsumeHandler,
  KafkaConsumerClient,
  KafkaConsumerMessageContext,
  KafkaDomainEvent,
  KafkaHeaderInput,
  KafkaHeaders,
  KafkaHeaderValue,
  KafkaModuleOptions,
  KafkaProducerClient,
  KafkaProducerClientMessage,
  KafkaProducerClientMessageItem,
  KafkaProducerRecordMetadata,
  KafkaPublishOptions,
  KafkaSubjectResolver,
  KafkaTopicName,
  SchemaRegistryCodec
} from "../types";
