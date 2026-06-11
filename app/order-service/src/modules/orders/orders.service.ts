import { Injectable } from "@nestjs/common";
import {
  type KafkaTopicName,
  type OrderCreatedEvent,
  type OrderRiskApprovedEvent,
  type OrderRiskRejectedEvent,
  type PaymentAuthorizedEvent,
  type PaymentFailedEvent
} from "@kafka-playground/contracts";
import { randomUUID } from "node:crypto";
import { PinoLogger } from "@kafka-playground/observability";
import type { CreateOrderDto } from "./dto/create-order.dto";
import { OutboxPublisherService } from "./outbox-publisher.service";
import { OrdersRepository } from "./orders.repository";
import type { OrderLifecycleEvent } from "./order-state-machine";
import { assertValidOrderId } from "./order-id";

export interface KafkaEventSource {
  topic: KafkaTopicName;
  offset: string;
}

@Injectable()
export class OrdersService {
  constructor(
    private readonly ordersRepository: OrdersRepository,
    private readonly outboxPublisher: OutboxPublisherService,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(OrdersService.name);
  }

  async createOrder(dto: CreateOrderDto) {
    this.logger.info(
      {
        userId: dto.userId,
        currency: dto.currency,
        itemLines: dto.items.length
      },
      "Creating pending order"
    );

    const totalAmount = dto.items.reduce(
      (sum, item) => sum + item.quantity * item.unitPrice,
      0
    );
    const itemCount = dto.items.reduce((sum, item) => sum + item.quantity, 0);

    const { order, event } = await this.ordersRepository.createPendingOrderWithOutbox({
      userId: dto.userId,
      currency: dto.currency,
      totalAmount,
      itemCount,
      items: dto.items,
      createEvent: (savedOrder): OrderCreatedEvent => ({
        eventId: randomUUID(),
        eventType: "OrderCreated",
        eventVersion: 1,
        occurredAt: new Date().toISOString(),
        correlationId: randomUUID(),
        causationId: null,
        producer: "order-service",
        payload: {
          orderId: savedOrder.id,
          userId: savedOrder.userId,
          currency: savedOrder.currency,
          totalAmount,
          itemCount
        }
      })
    });

    this.logger.info(
      {
        orderId: order.id,
        status: order.status,
        totalAmount,
        itemCount
      },
      "Pending order persisted"
    );

    this.logger.info(
      {
        orderId: order.id,
        eventId: event.eventId,
        eventType: event.eventType,
        correlationId: event.correlationId
      },
      "OrderCreated event queued in outbox"
    );

    // This is only a fast path. Durability comes from the persisted outbox row,
    // so the interval publisher can still recover the event if this call fails.
    void this.outboxPublisher.publishPending();

    return {
      id: order.id,
      status: order.status,
      userId: order.userId,
      currency: order.currency,
      totalAmount,
      itemCount,
      createdAt: order.createdAt.toISOString()
    };
  }

  async handleOrderRiskApproved(
    event: OrderRiskApprovedEvent,
    source: KafkaEventSource
  ): Promise<void> {
    await this.processLifecycleEvent(event, source);
  }

  async handleOrderRiskRejected(
    event: OrderRiskRejectedEvent,
    source: KafkaEventSource
  ): Promise<void> {
    await this.processLifecycleEvent(event, source, {
      reason: event.payload.reason,
      riskScore: event.payload.riskScore
    });
  }

  async handlePaymentAuthorized(
    event: PaymentAuthorizedEvent,
    source: KafkaEventSource
  ): Promise<void> {
    await this.processLifecycleEvent(event, source, {
      paymentId: event.payload.paymentId,
      provider: event.payload.provider
    });
  }

  async handlePaymentFailed(
    event: PaymentFailedEvent,
    source: KafkaEventSource
  ): Promise<void> {
    await this.processLifecycleEvent(event, source, {
      paymentId: event.payload.paymentId,
      provider: event.payload.provider,
      reason: event.payload.reason
    });
  }

  /**
   * Передаёт событие в транзакционный repository и централизованно логирует
   * все исходы: применение, дубль, неизвестный заказ и запрещённый переход.
   */
  private async processLifecycleEvent(
    event: OrderLifecycleEvent,
    source: KafkaEventSource,
    details: Record<string, unknown> = {}
  ): Promise<void> {
    const orderId = event.payload.orderId;

    assertValidOrderId(orderId);

    const result = await this.ordersRepository.processLifecycleEvent({
      orderId,
      event,
      sourceTopic: source.topic,
      sourceOffset: source.offset
    });

    if (result.outcome === "DUPLICATE_EVENT") {
      this.logger.info(
        {
          orderId,
          eventType: event.eventType,
          eventId: event.eventId,
          correlationId: event.correlationId,
          sourceTopic: source.topic,
          sourceOffset: source.offset,
          ...details
        },
        "Duplicate order status event skipped"
      );
      return;
    }

    if (result.outcome === "UNKNOWN_ORDER") {
      this.logger.warn(
        {
          orderId,
          eventType: event.eventType,
          eventId: event.eventId,
          correlationId: event.correlationId,
          sourceTopic: source.topic,
          sourceOffset: source.offset,
          ...details
        },
        "Order status event received for unknown order"
      );
      return;
    }

    if (result.outcome === "INVALID_TRANSITION") {
      this.logger.warn(
        {
          orderId,
          currentStatus: result.currentStatus,
          eventType: event.eventType,
          eventId: event.eventId,
          correlationId: event.correlationId,
          sourceTopic: source.topic,
          sourceOffset: source.offset,
          ...details
        },
        "Invalid order state transition skipped"
      );
      return;
    }

    this.logger.info(
      {
        orderId,
        previousStatus: result.previousStatus,
        status: result.status,
        eventType: event.eventType,
        eventId: event.eventId,
        correlationId: event.correlationId,
        sourceTopic: source.topic,
        sourceOffset: source.offset,
        ...details
      },
      "Order status updated from domain event"
    );

    if (result.finalEvent) {
      this.logger.info(
        {
          orderId,
          eventId: result.finalEvent.eventId,
          eventType: result.finalEvent.eventType,
          correlationId: result.finalEvent.correlationId
        },
        "Final order event queued in outbox"
      );

      // Вызов ускоряет отправку, но не является гарантией доставки: durable
      // источником остаётся outbox-запись, сохранённая в одной транзакции.
      void this.outboxPublisher.publishPending();
    }
  }
}
