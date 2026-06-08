import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import {
  EVENT_TOPIC_MAP,
  type OrderCreatedEvent,
  type DomainEvent,
  type KafkaTopicName
} from "@kafka-playground/contracts";
import { OutboxEventEntity, OutboxEventStatus } from "./entities/outbox-event.entity";
import { OrderEntity, OrderStatus, type OrderItemSnapshot } from "./entities/order.entity";

export interface CreatePendingOrderParams {
  userId: string;
  currency: string;
  totalAmount: number;
  itemCount: number;
  items: OrderItemSnapshot[];
}

/**
 * Параметры для создания заказа через transactional outbox.
 *
 * `createEvent` вызывается уже после сохранения заказа, потому что `orderId`
 * генерируется базой данных и нужен внутри `OrderCreated.payload`.
 */
export interface CreatePendingOrderWithOutboxParams extends CreatePendingOrderParams {
  createEvent(order: OrderEntity): OrderCreatedEvent;
}

/**
 * Параметры идемпотентного обновления заказа из входящего Kafka-события.
 */
export interface UpdateStatusFromEventParams {
  orderId: string;
  status: OrderStatus;
  event: DomainEvent;
  sourceTopic: KafkaTopicName;
  sourceOffset: string;
}

/**
 * Результат обработки входящего события статуса заказа.
 */
export enum OrderStatusUpdateResult {
  /** Статус заказа обновлён, событие обработано впервые. */
  Updated = "UPDATED",
  /** Такой `eventId` уже был обработан, бизнес-логику повторять нельзя. */
  DuplicateEvent = "DUPLICATE_EVENT",
  /** Событие валидное, но заказ с таким id не найден. */
  UnknownOrder = "UNKNOWN_ORDER"
}

@Injectable()
export class OrdersRepository {
  constructor(
    @InjectRepository(OrderEntity)
    private readonly repository: Repository<OrderEntity>
  ) {}

  async createPendingOrder(params: CreatePendingOrderParams): Promise<OrderEntity> {
    const order = this.repository.create({
      userId: params.userId,
      currency: params.currency,
      totalAmount: params.totalAmount.toFixed(2),
      itemCount: params.itemCount,
      status: OrderStatus.Pending,
      items: params.items
    });

    return this.repository.save(order);
  }

  /**
   * Создаёт заказ и outbox-запись `OrderCreated` в одной DB-транзакции.
   *
   * Это закрывает главный риск последовательности "сохранил заказ -> отправил
   * Kafka": если процесс упадёт после commit, outbox-запись останется в БД и
   * будет опубликована фоновым publisher-ом после рестарта.
   */
  async createPendingOrderWithOutbox(
    params: CreatePendingOrderWithOutboxParams
  ): Promise<{ order: OrderEntity; event: OrderCreatedEvent }> {
    return this.repository.manager.transaction(async (manager) => {
      const order = manager.create(OrderEntity, {
        userId: params.userId,
        currency: params.currency,
        totalAmount: params.totalAmount.toFixed(2),
        itemCount: params.itemCount,
        status: OrderStatus.Pending,
        items: params.items
      });
      const savedOrder = await manager.save(order);
      const event = params.createEvent(savedOrder);

      await manager.save(
        manager.create(OutboxEventEntity, {
          topic: EVENT_TOPIC_MAP.OrderCreated,
          messageKey: savedOrder.id,
          eventType: event.eventType,
          eventId: event.eventId,
          event,
          status: OutboxEventStatus.Pending
        })
      );

      return {
        order: savedOrder,
        event
      };
    });
  }

  async updateStatus(orderId: string, status: OrderStatus): Promise<boolean> {
    const result = await this.repository.update({ id: orderId }, { status });

    return (result.affected ?? 0) > 0;
  }

  /**
   * Идемпотентно применяет входящее Kafka-событие к заказу.
   *
   * Сначала вставляем `eventId` в `processed_kafka_events`. Если insert ничего
   * не вставил из-за unique conflict, значит событие уже применялось раньше и
   * повторное изменение статуса нужно пропустить.
   */
  async updateStatusFromEvent(
    params: UpdateStatusFromEventParams
  ): Promise<OrderStatusUpdateResult> {
    return this.repository.manager.transaction(async (manager) => {
      const inserted = await manager.query(
        `
          insert into processed_kafka_events (
            event_id,
            event_type,
            source_topic,
            source_offset
          )
          values ($1, $2, $3, $4)
          on conflict (event_id) do nothing
          returning id
        `,
        [
          params.event.eventId,
          params.event.eventType,
          params.sourceTopic,
          params.sourceOffset
        ]
      );

      if (inserted.length === 0) {
        return OrderStatusUpdateResult.DuplicateEvent;
      }

      const result = await manager.update(
        OrderEntity,
        { id: params.orderId },
        { status: params.status }
      );

      return (result.affected ?? 0) > 0
        ? OrderStatusUpdateResult.Updated
        : OrderStatusUpdateResult.UnknownOrder;
    });
  }
}
