import { Inject, Injectable, Optional } from "@nestjs/common";
import {
  KAFKA_TOPICS,
  type DeadLetterEvent,
  type DomainEvent
} from "@kafka-playground/contracts";
import {
  ApplicationMetrics,
  runInTraceSpan,
  SpanKind
} from "@kafka-playground/observability";
import { randomUUID } from "node:crypto";
import { KAFKA_MODULE_OPTIONS } from "./kafka.tokens";
import { KAFKA_HEADER_NAMES } from "./kafka-headers";
import { KafkaProducerService } from "./kafka-producer.service";
import {
  KafkaRetryPolicy,
  type KafkaRetryDecision
} from "./kafka-retry-policy";
import type {
  KafkaConsumerMessageContext,
  KafkaHeaders,
  KafkaModuleOptions
} from "./types";

/**
 * Публикует сообщение на следующий этап retry-цепочки или в DLQ.
 *
 * Dispatcher отвечает только за создание Kafka-сообщения. Решение о маршруте
 * принимает `KafkaRetryPolicy`, а задержку перед повторной обработкой реализует
 * `KafkaConsumerRunner`.
 */
@Injectable()
export class KafkaRetryDispatcher {
  constructor(
    private readonly policy: KafkaRetryPolicy,
    private readonly producer: KafkaProducerService,
    @Inject(KAFKA_MODULE_OPTIONS)
    private readonly options: KafkaModuleOptions,
    @Optional()
    private readonly metrics?: ApplicationMetrics
  ) {}

  /**
   * Маршрутизирует ошибку handler-а.
   *
   * Декодированное доменное событие повторно публикуется без изменения
   * `eventId`, `correlationId` и payload. Это позволяет downstream-сервисам
   * сохранить идемпотентность на всех retry-этапах.
   */
  async dispatch<TEvent extends DomainEvent>(params: {
    context: KafkaConsumerMessageContext<TEvent>;
    error: unknown;
  }): Promise<KafkaRetryDecision> {
    const decision = this.policy.decideFailure({
      currentTopic: params.context.topic,
      headers: params.context.headers,
      error: params.error
    });
    const headers = buildRetryHeaders(
      params.context.headers,
      decision
    );

    if (!decision.terminal) {
      await runInTraceSpan(
        "kafka retry dispatch",
        {
          kind: SpanKind.PRODUCER,
          attributes: {
            "messaging.system": "kafka",
            "messaging.destination.name": decision.destinationTopic,
            "messaging.operation.name": "retry",
            "retry.count": decision.retryCount,
            "event.id": params.context.event.eventId
          }
        },
        () =>
          this.producer.publish({
            topic: decision.destinationTopic,
            key: params.context.key ?? params.context.event.eventId,
            event: params.context.event,
            headers
          })
      );
      this.metrics?.recordKafkaRetry({
        originalTopic: decision.originalTopic,
        destinationTopic: decision.destinationTopic,
        stage: getRetryStage(decision.destinationTopic)
      });

      return decision;
    }

    await runInTraceSpan(
      "kafka dlq dispatch",
      {
        kind: SpanKind.PRODUCER,
        attributes: {
          "messaging.system": "kafka",
          "messaging.destination.name": KAFKA_TOPICS.deadLetterEvents,
          "messaging.operation.name": "dead_letter",
          "retry.count": decision.retryCount,
          "error.code": decision.errorCode,
          "event.id": params.context.event.eventId
        }
      },
      () =>
        this.publishDeadLetter({
          context: params.context,
          error: params.error,
          decision,
          headers
        })
    );

    return decision;
  }

  private async publishDeadLetter<TEvent extends DomainEvent>(params: {
    context: KafkaConsumerMessageContext<TEvent>;
    error: unknown;
    decision: KafkaRetryDecision;
    headers: KafkaHeaders;
  }): Promise<void> {
    const occurredAt = new Date().toISOString();
    const error = normalizeError(params.error);
    const deadLetterEvent: DeadLetterEvent = {
      eventId: randomUUID(),
      eventType: "DeadLetterEvent",
      eventVersion: 1,
      occurredAt,
      correlationId: params.context.event.correlationId,
      causationId: params.context.event.eventId,
      producer: this.options.serviceName,
      payload: {
        originalTopic: params.decision.originalTopic,
        originalPartition: params.context.partition,
        originalOffset: params.context.offset,
        errorMessage: error.message,
        errorStack: error.stack,
        rawEvent: JSON.stringify(params.context.event)
      }
    };

    await this.producer.publish({
      topic: KAFKA_TOPICS.deadLetterEvents,
      key: params.context.key ?? params.context.event.eventId,
      event: deadLetterEvent,
      headers: params.headers
    });
    this.metrics?.recordKafkaDlq(
      params.decision.originalTopic,
      params.decision.errorCode
    );
  }
}

/**
 * Дополняет существующие headers актуальным состоянием retry-цепочки.
 *
 * Бизнесовые и tracing headers сохраняются. Четыре retry header-а заменяются
 * рассчитанными значениями, поэтому устаревший счетчик не уйдет дальше.
 */
function buildRetryHeaders(
  currentHeaders: KafkaHeaders,
  decision: KafkaRetryDecision
): KafkaHeaders {
  return {
    ...currentHeaders,
    [KAFKA_HEADER_NAMES.retryCount]: String(decision.retryCount),
    [KAFKA_HEADER_NAMES.originalTopic]: decision.originalTopic,
    [KAFKA_HEADER_NAMES.firstFailedAt]: decision.firstFailedAt,
    [KAFKA_HEADER_NAMES.errorCode]: decision.errorCode
  };
}

/**
 * Приводит любое выброшенное значение к сериализуемому описанию ошибки.
 */
function normalizeError(error: unknown): {
  message: string;
  stack: string | null;
} {
  if (error instanceof Error) {
    return {
      message: error.message,
      stack: error.stack ?? null
    };
  }

  return {
    message: String(error),
    stack: null
  };
}

function getRetryStage(topic: string): string {
  const marker = ".retry-";
  const index = topic.lastIndexOf(marker);

  return index === -1 ? "unknown" : topic.slice(index + 1);
}
