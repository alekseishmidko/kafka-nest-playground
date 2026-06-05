import { Injectable } from "@nestjs/common";
import {
  EVENT_TOPIC_MAP,
  type OrderCreatedEvent,
  type OrderRiskApprovedEvent,
  type OrderRiskRejectedEvent,
  type PaymentAuthorizedEvent,
  type PaymentFailedEvent
} from "@kafka-playground/contracts";
import { KafkaProducerService } from "@kafka-playground/kafka";
import { randomUUID } from "node:crypto";
import { PinoLogger } from "@kafka-playground/observability";
import type { CreateOrderDto } from "./dto/create-order.dto";
import { OrderStatus } from "./entities/order.entity";
import { OrdersRepository } from "./orders.repository";

@Injectable()
export class OrdersService {
  constructor(
    private readonly ordersRepository: OrdersRepository,
    private readonly kafkaProducer: KafkaProducerService,
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

    const order = await this.ordersRepository.createPendingOrder({
      userId: dto.userId,
      currency: dto.currency,
      totalAmount,
      itemCount,
      items: dto.items
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

    const event: OrderCreatedEvent = {
      eventId: randomUUID(),
      eventType: "OrderCreated",
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      correlationId: randomUUID(),
      causationId: null,
      producer: "order-service",
      payload: {
        orderId: order.id,
        userId: order.userId,
        currency: order.currency,
        totalAmount,
        itemCount
      }
    };

    // Publishes the domain event to Kafka and starts the async order pipeline.
    // Any service subscribed to order.order-events can react independently:
    // risk-service-go can run fraud scoring, analytics-service-go can update
    // projections, and notification-service can trigger user-facing messages.
    try {
      await this.kafkaProducer.publish({
        topic: EVENT_TOPIC_MAP.OrderCreated,
        key: order.id,
        event
      });

      this.logger.info(
        {
          orderId: order.id,
          eventId: event.eventId,
          eventType: event.eventType,
          topic: EVENT_TOPIC_MAP.OrderCreated,
          correlationId: event.correlationId
        },
        "OrderCreated event published"
      );
    } catch (error) {
      this.logger.warn(
        {
          orderId: order.id,
          eventId: event.eventId,
          eventType: event.eventType,
          topic: EVENT_TOPIC_MAP.OrderCreated,
          correlationId: event.correlationId,
          error
        },
        "OrderCreated event publish failed; order remains pending"
      );
    }

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

  async handleOrderRiskApproved(event: OrderRiskApprovedEvent): Promise<void> {
    await this.updateOrderStatus(
      event.payload.orderId,
      OrderStatus.RiskApproved,
      event.eventType,
      event.eventId,
      event.correlationId
    );
  }

  async handleOrderRiskRejected(event: OrderRiskRejectedEvent): Promise<void> {
    await this.updateOrderStatus(
      event.payload.orderId,
      OrderStatus.RiskRejected,
      event.eventType,
      event.eventId,
      event.correlationId,
      {
        reason: event.payload.reason,
        riskScore: event.payload.riskScore
      }
    );
  }

  async handlePaymentAuthorized(event: PaymentAuthorizedEvent): Promise<void> {
    await this.updateOrderStatus(
      event.payload.orderId,
      OrderStatus.PaymentAuthorized,
      event.eventType,
      event.eventId,
      event.correlationId,
      {
        paymentId: event.payload.paymentId,
        provider: event.payload.provider
      }
    );
  }

  async handlePaymentFailed(event: PaymentFailedEvent): Promise<void> {
    await this.updateOrderStatus(
      event.payload.orderId,
      OrderStatus.PaymentFailed,
      event.eventType,
      event.eventId,
      event.correlationId,
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
    eventType: string,
    eventId: string,
    correlationId: string,
    details: Record<string, unknown> = {}
  ): Promise<void> {
    const updated = await this.ordersRepository.updateStatus(orderId, status);

    if (!updated) {
      this.logger.warn(
        {
          orderId,
          status,
          eventType,
          eventId,
          correlationId,
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
        eventType,
        eventId,
        correlationId,
        ...details
      },
      "Order status updated from domain event"
    );
  }
}
