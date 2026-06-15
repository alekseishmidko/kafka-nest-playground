import {
  KAFKA_TOPICS,
  type KafkaTopicName
} from "@kafka-playground/contracts";
import { Injectable } from "@nestjs/common";
import { KafkaNonRetryableError } from "../kafka-errors";
import { KAFKA_HEADER_NAMES, readHeader } from "../kafka-headers";
import type { KafkaHeaders } from "../types";

/**
 * Описывает один этап повторной обработки сообщения.
 */
export interface KafkaRetryStage {
  topic: KafkaTopicName;
  delayMs: number;
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

const ORDER_EVENTS_RETRY_STAGES: readonly KafkaRetryStage[] = [
  {
    topic: KAFKA_TOPICS.orderOrderEventsRetry5s,
    delayMs: 5_000
  },
  {
    topic: KAFKA_TOPICS.orderOrderEventsRetry30s,
    delayMs: 30_000
  },
  {
    topic: KAFKA_TOPICS.orderOrderEventsRetry5m,
    delayMs: 5 * 60_000
  }
];

const ORDER_FLOW_SOURCE_TOPICS: readonly KafkaTopicName[] = [
  KAFKA_TOPICS.orderOrderEvents,
  KAFKA_TOPICS.riskRiskEvents,
  KAFKA_TOPICS.paymentPaymentEvents
];

/**
 * Описывает детерминированный маршрут повторной обработки Kafka-сообщений.
 *
 * Policy ничего не публикует и не использует таймеры. Она только отвечает на
 * три вопроса:
 *
 * 1. На какие retry topics нужно подписаться вместе с основным topic.
 * 2. Как долго ждать перед обработкой сообщения из конкретного retry topic.
 * 3. Куда направить сообщение после очередной ошибки.
 *
 * Такое разделение оставляет маршрутизацию чистой бизнес-логикой, которую можно
 * проверить модульными тестами без Kafka broker-а.
 */
@Injectable()
export class KafkaRetryPolicy {
  /**
   * Проверяет, относится ли текущий topic к настроенной retry-цепочке.
   */
  supports(
    topic: KafkaTopicName,
    headers?: KafkaHeaders
  ): boolean {
    const originalTopic = this.resolveOriginalTopic(topic, headers);
    const stages = this.getStagesForOriginalTopic(originalTopic);

    return (
      stages.length > 0 &&
      (topic === originalTopic ||
        stages.some((stage) => stage.topic === topic))
    );
  }

  /**
   * Возвращает основной topic и все связанные с ним retry topics.
   */
  getSubscriptionTopics(topic: KafkaTopicName): KafkaTopicName[] {
    const stages = this.getStagesForOriginalTopic(topic);

    return [topic, ...stages.map((stage) => stage.topic)];
  }

  /**
   * Возвращает задержку текущего retry topic.
   *
   * Для основного topic и неизвестных topic задержка равна нулю.
   */
  getDelayMs(topic: KafkaTopicName): number {
    return (
      ORDER_EVENTS_RETRY_STAGES.find((stage) => stage.topic === topic)
        ?.delayMs ?? 0
    );
  }

  /**
   * Строит решение после неуспешной обработки сообщения.
   *
   * Первый отказ отправляет сообщение в `retry-5s`, следующий в `retry-30s`,
   * затем в `retry-5m`. Ошибка на последнем этапе завершает цепочку публикацией
   * `DeadLetterEvent` в `dead-letter.events`.
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
    const stages = this.getStagesForOriginalTopic(originalTopic);

    if (
      stages.length === 0 ||
      (params.currentTopic !== originalTopic &&
        !stages.some((stage) => stage.topic === params.currentTopic))
    ) {
      throw new Error(
        `Retry policy is not configured for topic ${params.currentTopic}`
      );
    }
    const currentStageIndex = stages.findIndex(
      (stage) => stage.topic === params.currentTopic
    );
    const nextStageIndex = currentStageIndex + 1;
    const nextStage = stages[nextStageIndex];
    const previousRetryCount = readRetryCount(params.headers);
    const retryCount = Math.max(previousRetryCount + 1, nextStageIndex + 1);
    const firstFailedAt =
      readHeader(params.headers, KAFKA_HEADER_NAMES.firstFailedAt) ??
      (params.now ?? new Date()).toISOString();
    const terminal =
      params.error instanceof KafkaNonRetryableError ||
      nextStage === undefined;

    return {
      destinationTopic:
        terminal
          ? KAFKA_TOPICS.deadLetterEvents
          : nextStage.topic,
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
    const headerValue = readHeader(
      headers,
      KAFKA_HEADER_NAMES.originalTopic
    );

    if (headerValue && isKafkaTopicName(headerValue)) {
      return headerValue;
    }

    if (
      ORDER_EVENTS_RETRY_STAGES.some(
        (stage) => stage.topic === currentTopic
      )
    ) {
      return KAFKA_TOPICS.orderOrderEvents;
    }

    return currentTopic;
  }

  private getStagesForOriginalTopic(
    topic: KafkaTopicName
  ): readonly KafkaRetryStage[] {
    return ORDER_FLOW_SOURCE_TOPICS.includes(topic)
      ? ORDER_EVENTS_RETRY_STAGES
      : [];
  }
}

/**
 * Безопасно читает номер попытки из Kafka headers.
 *
 * Поврежденное, отрицательное или дробное значение считается нулем: технический
 * header не должен ломать consumer и создавать бесконечную retry-цепочку.
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

function isKafkaTopicName(value: string): value is KafkaTopicName {
  return Object.values(KAFKA_TOPICS).includes(value as KafkaTopicName);
}
