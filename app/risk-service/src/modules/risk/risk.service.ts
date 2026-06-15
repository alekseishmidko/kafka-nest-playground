import { Injectable } from "@nestjs/common";
import {
  EVENT_TOPIC_MAP,
  type OrderCreatedEvent,
  type OrderRiskApprovedEvent,
  type OrderRiskRejectedEvent
} from "@kafka-playground/contracts";
import {
  createDeterministicEventId,
  KafkaProducerService
} from "@kafka-playground/kafka";
import { PinoLogger } from "@kafka-playground/observability";
import { RiskScorer } from "./risk.scorer";

@Injectable()
export class RiskService {
  constructor(
    private readonly scorer: RiskScorer,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(RiskService.name);
  }

  /**
   * Вычисляет и возвращает результат risk-проверки без внешних side effects.
   *
   * Результат сохраняется durable inbox до публикации. Поэтому повторная
   * обработка после сбоя использует прежний score и прежний `eventId`.
   */
  prepareRiskEvent(
    event: OrderCreatedEvent
  ): OrderRiskApprovedEvent | OrderRiskRejectedEvent {
    // Логируем бизнес-ключи до scoring: по ним удобно искать всю цепочку обработки заказа.
    this.logger.info(
      {
        orderId: event.payload.orderId,
        eventId: event.eventId,
        correlationId: event.correlationId,
        totalAmount: event.payload.totalAmount,
        itemCount: event.payload.itemCount
      },
      "Scoring order risk"
    );

    // Scorer синхронный и CPU-heavy. Пока он работает, event loop занят вычислением.
    const decision = this.scorer.score(event.payload);
    // Из одного входного события создаем строго одно исходящее risk-событие.
    const riskEvent = this.createRiskEvent(event, decision);

    return riskEvent;
  }

  /**
   * Публикует заранее подготовленное risk-событие.
   *
   * При crash recovery метод может быть вызван повторно с тем же `eventId`.
   * Downstream inbox обязан считать такую публикацию дублем.
   */
  async publishRiskEvent(
    riskEvent: OrderRiskApprovedEvent | OrderRiskRejectedEvent
  ): Promise<void> {
    await this.kafkaProducer.publish({
      topic: EVENT_TOPIC_MAP[riskEvent.eventType],
      // Key = orderId. Это сохраняет порядок событий одного заказа внутри Kafka partition.
      key: riskEvent.payload.orderId,
      event: riskEvent,
      // correlationId связывает всю бизнес-цепочку, causationId указывает на событие-причину.
      correlationId: riskEvent.correlationId,
      causationId: riskEvent.causationId ?? undefined
    });

    this.logger.info(
      {
        orderId: riskEvent.payload.orderId,
        eventId: riskEvent.eventId,
        eventType: riskEvent.eventType,
        riskScore: riskEvent.payload.riskScore,
        approved: riskEvent.eventType === "OrderRiskApproved",
        topic: EVENT_TOPIC_MAP[riskEvent.eventType],
        correlationId: riskEvent.correlationId
      },
      "Risk event published"
    );
  }

  private createRiskEvent(
    source: OrderCreatedEvent,
    decision: ReturnType<RiskScorer["score"]>
  ): OrderRiskApprovedEvent | OrderRiskRejectedEvent {
    // Общая metadata часть event envelope совпадает с контрактами в packages/contracts.
    const base = {
      eventId: createDeterministicEventId(
        "risk-service:risk-result",
        source.eventId
      ),
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      correlationId: source.correlationId,
      causationId: source.eventId,
      producer: "risk-service"
    };

    if (decision.approved) {
      // Approved событие говорит downstream-сервисам, что заказ может идти дальше по pipeline.
      return {
        ...base,
        eventType: "OrderRiskApproved",
        payload: {
          orderId: source.payload.orderId,
          amount: source.payload.totalAmount,
          currency: source.payload.currency,
          riskScore: decision.score,
          approvedBy: "risk-service"
        }
      };
    }

    // Rejected событие содержит machine-readable reason, чтобы order/payment pipeline мог принять решение.
    return {
      ...base,
      eventType: "OrderRiskRejected",
      payload: {
        orderId: source.payload.orderId,
        riskScore: decision.score,
        reason: decision.reason ?? "risk_score_threshold_exceeded",
        rejectedBy: "risk-service"
      }
    };
  }
}
