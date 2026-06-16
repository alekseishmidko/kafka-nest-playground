import { KafkaNonRetryableError } from "../kafka-errors";
import { KAFKA_HEADER_NAMES, readHeader } from "../kafka-headers";
import type { KafkaHeaders, KafkaTopicName } from "../types";

/**
 * Описывает один retry topic и задержку перед повторной обработкой.
 */
export interface KafkaRetryStage {
  topic: KafkaTopicName;
  delayMs: number;
}

/**
 * Конфигурация retry-цепочки для группы исходных topics.
 *
 * Несколько source topics могут использовать одни и те же retry topics. В таком
 * случае исходный topic сохраняется в header `x-original-topic`, а consumer
 * после задержки возвращает сообщение правильному handler-у.
 */
export interface KafkaRetryRouteConfig {
  sourceTopics: readonly KafkaTopicName[];
  stages: readonly KafkaRetryStage[];
  deadLetterTopic: KafkaTopicName;
}

/**
 * Содержит результат расчета маршрута после ошибки consumer handler-а.
 */
export interface KafkaRetryDecision {
  destinationTopic: KafkaTopicName;
  originalTopic: KafkaTopicName;
  retryCount: number;
  firstFailedAt: string;
  errorCode: string;
  terminal: boolean;
}

/**
 * Чистая, переиспользуемая политика маршрутизации Kafka retry.
 *
 * Класс не зависит от NestJS, KafkaJS, PostgreSQL и contracts текущего
 * playground-проекта. Его можно использовать в другом сервисе, передав свои
 * topic names и retry stages:
 *
 * ```ts
 * new ConfigurableKafkaRetryPolicy({
 *   sourceTopics: ["billing.invoice-events"],
 *   stages: [
 *     { topic: "billing.invoice-events.retry-10s", delayMs: 10_000 }
 *   ],
 *   deadLetterTopic: "dead-letter.events"
 * });
 * ```
 */
export class ConfigurableKafkaRetryPolicy {
  private readonly sourceTopics: Set<KafkaTopicName>;
  private readonly retryTopics: Set<KafkaTopicName>;

  constructor(private readonly config: KafkaRetryRouteConfig) {
    this.assertValidConfig(config);
    this.sourceTopics = new Set(config.sourceTopics);
    this.retryTopics = new Set(config.stages.map((stage) => stage.topic));
  }

  /**
   * Проверяет, относится ли текущий topic к настроенной retry-цепочке.
   */
  supports(topic: KafkaTopicName, headers?: KafkaHeaders): boolean {
    const originalTopic = this.resolveOriginalTopic(topic, headers);

    return (
      this.sourceTopics.has(originalTopic) &&
      (topic === originalTopic || this.retryTopics.has(topic))
    );
  }

  /**
   * Возвращает основной topic и все связанные с ним retry topics.
   */
  getSubscriptionTopics(topic: KafkaTopicName): KafkaTopicName[] {
    if (!this.sourceTopics.has(topic)) {
      return [topic];
    }

    return [topic, ...this.config.stages.map((stage) => stage.topic)];
  }

  /**
   * Возвращает задержку текущего retry topic.
   *
   * Для основного topic и неизвестных topic задержка равна нулю.
   */
  getDelayMs(topic: KafkaTopicName): number {
    return this.config.stages.find((stage) => stage.topic === topic)?.delayMs ?? 0;
  }

  /**
   * Строит решение после неуспешной обработки сообщения.
   */
  decideFailure(params: {
    currentTopic: KafkaTopicName;
    headers: KafkaHeaders | undefined;
    error: unknown;
    now?: Date;
  }): KafkaRetryDecision {
    const originalTopic = this.resolveOriginalTopic(
      params.currentTopic,
      params.headers
    );

    if (!this.supports(params.currentTopic, params.headers)) {
      throw new Error(
        `Retry policy is not configured for topic ${params.currentTopic}`
      );
    }

    const currentStageIndex = this.config.stages.findIndex(
      (stage) => stage.topic === params.currentTopic
    );
    const nextStageIndex = currentStageIndex + 1;
    const nextStage = this.config.stages[nextStageIndex];
    const previousRetryCount = readRetryCount(params.headers);
    const retryCount = Math.max(previousRetryCount + 1, nextStageIndex + 1);
    const firstFailedAt =
      readHeader(params.headers, KAFKA_HEADER_NAMES.firstFailedAt) ??
      (params.now ?? new Date()).toISOString();
    const terminal =
      params.error instanceof KafkaNonRetryableError ||
      nextStage === undefined;

    return {
      destinationTopic: terminal ? this.config.deadLetterTopic : nextStage.topic,
      originalTopic,
      retryCount,
      firstFailedAt,
      errorCode: getErrorCode(params.error),
      terminal
    };
  }

  /**
   * Определяет исходный topic для сообщения, которое уже находится в retry.
   */
  private resolveOriginalTopic(
    currentTopic: KafkaTopicName,
    headers: KafkaHeaders | undefined
  ): KafkaTopicName {
    const headerValue = readHeader(headers, KAFKA_HEADER_NAMES.originalTopic);

    if (headerValue && this.sourceTopics.has(headerValue)) {
      return headerValue;
    }

    if (this.retryTopics.has(currentTopic)) {
      return this.config.sourceTopics[0];
    }

    return currentTopic;
  }

  private assertValidConfig(config: KafkaRetryRouteConfig): void {
    if (config.sourceTopics.length === 0) {
      throw new Error("Kafka retry policy requires at least one source topic");
    }

    if (config.stages.length === 0) {
      throw new Error("Kafka retry policy requires at least one retry stage");
    }

    const duplicateStage = findDuplicate(config.stages.map((stage) => stage.topic));

    if (duplicateStage) {
      throw new Error(`Kafka retry stage is duplicated: ${duplicateStage}`);
    }
  }
}

/**
 * Безопасно читает номер попытки из Kafka headers.
 */
function readRetryCount(headers: KafkaHeaders | undefined): number {
  const rawValue = readHeader(headers, KAFKA_HEADER_NAMES.retryCount);
  const value = Number(rawValue ?? "0");

  return Number.isInteger(value) && value >= 0 ? value : 0;
}

/**
 * Преобразует тип ошибки JavaScript в стабильный технический код.
 */
function getErrorCode(error: unknown): string {
  if (error instanceof KafkaNonRetryableError) {
    return error.errorCode;
  }

  if (error instanceof Error && error.name) {
    return normalizeErrorCode(error.name);
  }

  return "UNKNOWN_ERROR";
}

/**
 * Нормализует имя ошибки в формат `UPPER_SNAKE_CASE`.
 */
function normalizeErrorCode(value: string): string {
  return value
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/[^a-zA-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toUpperCase();
}

function findDuplicate(values: readonly string[]): string | undefined {
  const seen = new Set<string>();

  for (const value of values) {
    if (seen.has(value)) {
      return value;
    }

    seen.add(value);
  }

  return undefined;
}
