import { Injectable } from "@nestjs/common";
import {
  type DomainEvent,
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
import { OrderStatus } from "./entities/order.entity";
import { OutboxPublisherService } from "./outbox-publisher.service";
import {
  OrderStatusUpdateResult,
  OrdersRepository
} from "./orders.repository";

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
    await this.updateOrderStatus(
      event.payload.orderId,
      OrderStatus.RiskApproved,
      event,
      source
    );
  }

  async handleOrderRiskRejected(
    event: OrderRiskRejectedEvent,
    source: KafkaEventSource
  ): Promise<void> {
    await this.updateOrderStatus(
      event.payload.orderId,
      OrderStatus.RiskRejected,
      event,
      source,
      {
        reason: event.payload.reason,
        riskScore: event.payload.riskScore
      }
    );
  }

  async handlePaymentAuthorized(
    event: PaymentAuthorizedEvent,
    source: KafkaEventSource
  ): Promise<void> {
    await this.updateOrderStatus(
      event.payload.orderId,
      OrderStatus.PaymentAuthorized,
      event,
      source,
      {
        paymentId: event.payload.paymentId,
        provider: event.payload.provider
      }
    );
  }

  async handlePaymentFailed(
    event: PaymentFailedEvent,
    source: KafkaEventSource
  ): Promise<void> {
    await this.updateOrderStatus(
      event.payload.orderId,
      OrderStatus.PaymentFailed,
      event,
      source,
      {
        paymentId: event.payload.paymentId,
        provider: event.payload.provider,
        reason: event.payload.reason
      }
    );
  }

  private async updateOrderStatus(
    orderId: string,
    status: OrderStatus,
    event: DomainEvent,
    source: KafkaEventSource,
    details: Record<string, unknown> = {}
  ): Promise<void> {
    const result = await this.ordersRepository.updateStatusFromEvent({
      orderId,
      status,
      event,
      sourceTopic: source.topic,
      sourceOffset: source.offset
    });

    if (result === OrderStatusUpdateResult.DuplicateEvent) {
      this.logger.info(
        {
          orderId,
          status,
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

    if (result === OrderStatusUpdateResult.UnknownOrder) {
      this.logger.warn(
        {
          orderId,
          status,
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

    this.logger.info(
      {
        orderId,
        status,
        eventType: event.eventType,
        eventId: event.eventId,
        correlationId: event.correlationId,
        sourceTopic: source.topic,
        sourceOffset: source.offset,
        ...details
      },
      "Order status updated from domain event"
    );
  }
}
