import {
  BadRequestException,
  Injectable,
  NotFoundException
} from "@nestjs/common";
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
import { OutboxPublisherService } from "@kafka-playground/outbox";
import type { CreateOrderDto } from "./dto/create-order.dto";
import { OrdersRepository } from "./orders.repository";
import type { OrderLifecycleEvent } from "./order-state-machine";
import { assertValidOrderId } from "./order-id";
import type { OrderCancellationRequester } from "./order-cancellation";

export interface KafkaEventSource {
  topic: KafkaTopicName;
  offset: string;
}

/**
 * Команда отмены заказа.
 *
 * `requestedBy` нужен, чтобы отличать пользовательскую отмену от будущей
 * операторской/admin-отмены. Правила state machine одинаковые, но audit и
 * события должны сохранять источник команды.
 */
export interface CancelOrderCommand {
  id: string;
  reason: string;
  requestedBy?: OrderCancellationRequester;
}

/**
 * Техническая metadata команды создания заказа.
 *
 * Gateway передаёт сюда HTTP `Idempotency-Key` и hash нормализованного request
 * body. Domain payload заказа не загрязняется этой metadata: ключ нужен только
 * для безопасного повторного HTTP вызова.
 */
export interface CreateOrderCommand {
  idempotencyKey?: string;
  requestHash?: string;
}

/**
 * Application service order-service.
 *
 * Сервис держит orchestration-логику: считает derived поля заказа, выбирает
 * обычную или идемпотентную транзакцию repository, ускоряет outbox publisher и
 * централизованно логирует результат. Сами DB locks, inbox/outbox и state
 * transitions остаются ниже, в repository/state-machine, чтобы метод не
 * превращался в набор SQL-деталей.
 */
@Injectable()
export class OrdersService {
  constructor(
    private readonly ordersRepository: OrdersRepository,
    private readonly outboxPublisher: OutboxPublisherService,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(OrdersService.name);
  }

  /**
   * Создаёт новый PENDING order и ставит `OrderCreated` в transactional outbox.
   *
   * При наличии `Idempotency-Key` повтор с тем же key/hash возвращает сохранённый
   * response и не вызывает `publishPending`: второй outbox event не создаётся,
   * поэтому ускорять publisher нечего. Первый запрос сохраняет order, outbox и
   * idempotency response в одной PostgreSQL-транзакции.
   */
  async createOrder(dto: CreateOrderDto, command: CreateOrderCommand = {}) {
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

    const createParams: Parameters<
      OrdersRepository["createPendingOrderWithOutbox"]
    >[0] = {
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
    };
    const idempotency = readCreateOrderIdempotency(command);
    const result = idempotency
      ? await this.ordersRepository.createPendingOrderWithOutboxIdempotently({
          ...createParams,
          idempotency
        })
      : await this.createPendingOrderWithoutIdempotency(createParams);

    if (result.replayed) {
      this.logger.info(
        {
          idempotencyKey: idempotency?.key
        },
        "Returning stored create order response for Idempotency-Key"
      );

      return result.response;
    }

    const { order, event, response } = result;

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

    return response;
  }

  /**
   * Применяет положительное risk-событие к заказу.
   */
  async handleOrderRiskApproved(
    event: OrderRiskApprovedEvent,
    source: KafkaEventSource
  ): Promise<void> {
    await this.processLifecycleEvent(event, source);
  }

  /**
   * Применяет отрицательное risk-событие и сохраняет причину отказа в логах.
   */
  async handleOrderRiskRejected(
    event: OrderRiskRejectedEvent,
    source: KafkaEventSource
  ): Promise<void> {
    await this.processLifecycleEvent(event, source, {
      reason: event.payload.reason,
      riskScore: event.payload.riskScore
    });
  }

  /**
   * Применяет успешную авторизацию платежа и при финальном переходе создаёт
   * `OrderConfirmed` через repository.
   */
  async handlePaymentAuthorized(
    event: PaymentAuthorizedEvent,
    source: KafkaEventSource
  ): Promise<void> {
    await this.processLifecycleEvent(event, source, {
      paymentId: event.payload.paymentId,
      provider: event.payload.provider
    });
  }

  /**
   * Применяет отказ платежа и при финальном переходе создаёт `OrderCancelled`.
   */
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
   * Обрабатывает пользовательскую или операторскую отмену заказа.
   *
   * Repository всегда пишет `OrderCancellationRequested`, а затем либо
   * `OrderCancelled`, либо `OrderCancellationRejected`. Это делает историю
   * решений явной даже для запрещённых переходов, например отмены уже
   * подтверждённого заказа.
   */
  async cancelOrder(command: CancelOrderCommand) {
    assertValidOrderId(command.id);
    const reason = requireCancellationReason(command.reason);
    const requestedBy = command.requestedBy ?? "user";
    const result = await this.ordersRepository.cancelOrder({
      orderId: command.id,
      reason,
      requestedBy
    });

    if (result.outcome === "UNKNOWN_ORDER") {
      throw new NotFoundException(`Order ${command.id} was not found`);
    }

    void this.outboxPublisher.publishPending();

    if (result.outcome === "REJECTED") {
      this.logger.warn(
        {
          orderId: command.id,
          status: result.status,
          reason,
          requestedBy,
          rejectedReason: result.rejectedEvent.payload.rejectedReason,
          eventId: result.rejectedEvent.eventId
        },
        "Order cancellation rejected"
      );

      return {
        id: command.id,
        status: result.status,
        cancellationStatus: "REJECTED",
        reason,
        requestedBy,
        currentStatus: result.status
      };
    }

    this.logger.info(
      {
        orderId: command.id,
        previousStatus: result.previousStatus,
        status: result.status,
        reason,
        requestedBy,
        eventId: result.finalEvent.eventId
      },
      "Order cancellation accepted"
    );

    return {
      id: command.id,
      status: result.status,
      cancellationStatus: "ACCEPTED",
      reason,
      requestedBy,
      currentStatus: result.status
    };
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

  /**
   * Совместимый путь создания заказа без `Idempotency-Key`.
   *
   * Возвращаемая форма специально совпадает с результатом идемпотентного пути,
   * чтобы основной `createOrder` не держал две ветки логирования и публикации.
   */
  private async createPendingOrderWithoutIdempotency(
    params: Parameters<OrdersRepository["createPendingOrderWithOutbox"]>[0]
  ) {
    const { order, event } =
      await this.ordersRepository.createPendingOrderWithOutbox(params);

    return {
      replayed: false as const,
      order,
      event,
      response: {
        id: order.id,
        status: order.status,
        userId: order.userId,
        currency: order.currency,
        totalAmount: Number(order.totalAmount),
        itemCount: order.itemCount,
        createdAt: order.createdAt.toISOString()
      }
    };
  }
}

/**
 * Валидирует техническую idempotency metadata.
 *
 * Key и hash должны приходить парой. Один key без hash небезопасен: сервис не
 * сможет проверить, что повторный request имеет то же тело.
 */
function readCreateOrderIdempotency(command: CreateOrderCommand):
  | {
      key: string;
      requestHash: string;
    }
  | null {
  if (!command.idempotencyKey && !command.requestHash) {
    return null;
  }

  if (!command.idempotencyKey || !command.requestHash) {
    throw new BadRequestException(
      "Both Idempotency-Key and request hash metadata are required"
    );
  }

  return {
    key: command.idempotencyKey,
    requestHash: command.requestHash
  };
}

function requireCancellationReason(value: string): string {
  const reason = value.trim();

  if (reason.length < 5 || reason.length > 1000) {
    throw new BadRequestException(
      "Cancellation reason must contain between 5 and 1000 characters"
    );
  }

  return reason;
}
