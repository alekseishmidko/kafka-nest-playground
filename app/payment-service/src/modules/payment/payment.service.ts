import { Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import {
  EVENT_TOPIC_MAP,
  type OrderRiskApprovedEvent,
  type PaymentAuthorizedEvent,
  type PaymentFailedEvent
} from "@kafka-playground/contracts";
import { KafkaProducerService } from "@kafka-playground/kafka";
import { randomUUID } from "node:crypto";
import { PinoLogger } from "nestjs-pino";
import { PaymentAuthorizer, type PaymentAuthorizationDecision } from "./payment.authorizer";

@Injectable()
export class PaymentService {
  private readonly provider: string;

  constructor(
    config: ConfigService,
    private readonly authorizer: PaymentAuthorizer,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly logger: PinoLogger
  ) {
    this.provider = config.get<string>("PAYMENT_PROVIDER") ?? "mock-payment-provider";
    this.logger.setContext(PaymentService.name);
  }

  async handleOrderRiskApproved(event: OrderRiskApprovedEvent): Promise<void> {
    this.logger.info(
      {
        orderId: event.payload.orderId,
        eventId: event.eventId,
        correlationId: event.correlationId,
        amount: event.payload.amount,
        currency: event.payload.currency,
        riskScore: event.payload.riskScore
      },
      "Authorizing payment"
    );

    const decision = this.hasPaymentAmount(event)
      ? this.authorizer.authorize(event.payload)
      : {
          authorized: false,
          paymentId: null,
          reason: "payment_amount_missing"
        };
    const paymentEvent = this.createPaymentEvent(event, decision);

    await this.kafkaProducer.publish({
      topic: EVENT_TOPIC_MAP[paymentEvent.eventType],
      key: event.payload.orderId,
      event: paymentEvent,
      correlationId: event.correlationId,
      causationId: event.eventId
    });

    this.logger.info(
      {
        orderId: event.payload.orderId,
        paymentId: paymentEvent.payload.paymentId,
        eventId: paymentEvent.eventId,
        eventType: paymentEvent.eventType,
        authorized: decision.authorized,
        topic: EVENT_TOPIC_MAP[paymentEvent.eventType],
        correlationId: paymentEvent.correlationId
      },
      "Payment event published"
    );
  }

  private createPaymentEvent(
    source: OrderRiskApprovedEvent,
    decision: PaymentAuthorizationDecision
  ): PaymentAuthorizedEvent | PaymentFailedEvent {
    const base = {
      eventId: randomUUID(),
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      correlationId: source.correlationId,
      causationId: source.eventId,
      producer: "payment-service"
    };

    if (decision.authorized) {
      return {
        ...base,
        eventType: "PaymentAuthorized",
        payload: {
          paymentId: decision.paymentId ?? randomUUID(),
          orderId: source.payload.orderId,
          amount: source.payload.amount,
          currency: source.payload.currency,
          provider: this.provider
        }
      };
    }

    return {
      ...base,
      eventType: "PaymentFailed",
      payload: {
        paymentId: decision.paymentId,
        orderId: source.payload.orderId,
        reason: decision.reason ?? "payment_authorization_failed",
        provider: this.provider
      }
    };
  }

  private hasPaymentAmount(event: OrderRiskApprovedEvent): boolean {
    return (
      Number.isFinite(event.payload.amount) &&
      typeof event.payload.currency === "string" &&
      event.payload.currency.length > 0
    );
  }
}
