import { Injectable } from "@nestjs/common";
import {
  EVENT_TOPIC_MAP,
  type OrderCreatedEvent
} from "@kafka-playground/contracts";
import { KafkaProducerService } from "@kafka-playground/kafka";
import { randomUUID } from "node:crypto";
import { PinoLogger } from "nestjs-pino";
import type { CreateOrderDto } from "./dto/create-order.dto";
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
}
