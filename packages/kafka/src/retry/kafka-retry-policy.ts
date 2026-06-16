import { Injectable } from "@nestjs/common";
import { KAFKA_TOPICS } from "@kafka-playground/contracts";
import {
  ConfigurableKafkaRetryPolicy,
  type KafkaRetryRouteConfig
} from "./configurable-kafka-retry-policy";

/**
 * Стандартная retry-конфигурация текущего order-flow.
 *
 * Конфигурация вынесена в отдельную константу, чтобы приложения могли:
 *
 * - использовать готовый preset как есть;
 * - скопировать структуру и заменить topics для своего bounded context;
 * - перейти на `ConfigurableKafkaRetryPolicy` без зависимости от contracts
 *   playground-проекта.
 */
export const ORDER_FLOW_RETRY_CONFIG = {
  sourceTopics: [
    KAFKA_TOPICS.orderOrderEvents,
    KAFKA_TOPICS.riskRiskEvents,
    KAFKA_TOPICS.paymentPaymentEvents
  ],
  stages: [
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
  ],
  deadLetterTopic: KAFKA_TOPICS.deadLetterEvents
} as const satisfies KafkaRetryRouteConfig;

/**
 * Backward-compatible retry policy для текущего playground order-flow.
 *
 * Общий алгоритм находится в `ConfigurableKafkaRetryPolicy`; этот класс только
 * подставляет topics из `@kafka-playground/contracts` и регистрируется в NestJS
 * DI как существующий provider.
 */
@Injectable()
export class KafkaRetryPolicy extends ConfigurableKafkaRetryPolicy {
  constructor() {
    super(ORDER_FLOW_RETRY_CONFIG);
  }
}

export type {
  KafkaRetryDecision,
  KafkaRetryRouteConfig,
  KafkaRetryStage
} from "./configurable-kafka-retry-policy";
