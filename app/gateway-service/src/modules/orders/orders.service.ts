import {
  BadRequestException,
  Inject,
  Injectable,
  OnModuleInit
} from "@nestjs/common";
import { Metadata } from "@grpc/grpc-js";
import { ClientGrpc } from "@nestjs/microservices";
import { PinoLogger } from "@kafka-playground/observability";
import { createHash } from "node:crypto";
import { lastValueFrom, Observable } from "rxjs";
import { ORDERS_GRPC_CLIENT } from "../../grpc/grpc-clients.constants";
import type { CreateOrderDto } from "./dto/create-order.dto";
import type {
  CancelOrderDto,
  CancelOrderResponseDto
} from "./dto/cancel-order.dto";
import type { OrderResponseDto } from "./dto/order-response.dto";

interface OrdersGrpcService {
  createOrder(
    payload: CreateOrderDto,
    metadata?: Metadata
  ): Observable<OrderResponseDto>;
  cancelOrder(payload: {
    id: string;
    reason: string;
    requestedBy: "user";
  }): Observable<CancelOrderResponseDto>;
}

interface CreateOrderOptions {
  idempotencyKey?: string;
}

/**
 * Application service публичного gateway для order API.
 *
 * Gateway не владеет доменной транзакцией заказа: он валидирует HTTP DTO,
 * добавляет transport-level metadata и делегирует команду во внутренний
 * `order-service` по gRPC. Благодаря этому вся durable логика, включая
 * transactional outbox и idempotency storage, остаётся в одном сервисе рядом с
 * PostgreSQL-транзакцией заказа.
 */
@Injectable()
export class OrdersService implements OnModuleInit {
  private ordersGrpcService!: OrdersGrpcService;

  constructor(
    @Inject(ORDERS_GRPC_CLIENT) private readonly client: ClientGrpc,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(OrdersService.name);
  }

  /**
   * Получает typed gRPC proxy после инициализации Nest module.
   */
  onModuleInit() {
    this.ordersGrpcService =
      this.client.getService<OrdersGrpcService>("OrdersService");
  }

  /**
   * Передаёт создание заказа в order-service.
   *
   * Если клиент прислал `Idempotency-Key`, gateway считает hash уже
   * провалидированного DTO и передаёт key/hash в gRPC metadata. Hash нужен,
   * чтобы order-service мог отличить легальный повтор того же запроса от
   * опасной повторной попытки с тем же ключом, но другим телом.
   */
  async createOrder(
    dto: CreateOrderDto,
    options: CreateOrderOptions = {}
  ): Promise<OrderResponseDto> {
    const metadata = createIdempotencyMetadata(dto, options.idempotencyKey);

    this.logger.info(
      {
        userId: dto.userId,
        currency: dto.currency,
        itemCount: dto.items.reduce((sum, item) => sum + item.quantity, 0),
        hasIdempotencyKey: metadata !== undefined
      },
      "Forwarding create order request to order-service via gRPC"
    );

    const order = await lastValueFrom(
      this.ordersGrpcService.createOrder(dto, metadata)
    );

    this.logger.info(
      {
        orderId: order.id,
        status: order.status,
        userId: order.userId
      },
      "Order-service gRPC create order request completed"
    );

    return order;
  }

  /**
   * Передаёт пользовательскую отмену заказа во внутренний order-service.
   */
  async cancelOrder(
    id: string,
    dto: CancelOrderDto
  ): Promise<CancelOrderResponseDto> {
    this.logger.info(
      {
        orderId: id
      },
      "Forwarding cancel order request to order-service via gRPC"
    );

    const result = await lastValueFrom(
      this.ordersGrpcService.cancelOrder({
        id,
        reason: dto.reason,
        requestedBy: "user"
      })
    );

    this.logger.info(
      {
        orderId: result.id,
        status: result.status,
        cancellationStatus: result.cancellationStatus
      },
      "Order-service gRPC cancel order request completed"
    );

    return result;
  }
}

/**
 * Создаёт metadata для идемпотентного create order.
 *
 * Header остаётся HTTP-контрактом gateway, а `idempotency-request-hash` является
 * внутренней gRPC metadata. Клиенту не нужно знать, как именно hash считается.
 */
function createIdempotencyMetadata(
  dto: CreateOrderDto,
  idempotencyKey: string | undefined
): Metadata | undefined {
  if (idempotencyKey === undefined) {
    return undefined;
  }

  const normalizedKey = idempotencyKey.trim();

  if (normalizedKey.length < 1 || normalizedKey.length > 160) {
    throw new BadRequestException(
      "Idempotency-Key length must be between 1 and 160 characters"
    );
  }

  const metadata = new Metadata();
  metadata.set("idempotency-key", normalizedKey);
  metadata.set("idempotency-request-hash", hashRequestBody(dto));

  return metadata;
}

/**
 * Возвращает SHA-256 от стабильной JSON-формы DTO.
 *
 * Стабильная сериализация сортирует ключи объектов, поэтому одинаковое
 * логическое тело запроса даёт одинаковый hash независимо от порядка полей в
 * исходном JSON.
 */
function hashRequestBody(dto: CreateOrderDto): string {
  return createHash("sha256")
    .update(stableStringify(dto))
    .digest("hex");
}

/**
 * Минимальная стабильная JSON-сериализация для plain DTO.
 *
 * Здесь нет поддержки циклических структур, потому что DTO после class-validator
 * состоит только из строк, чисел, массивов и plain object-ов.
 */
function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  }

  if (value && typeof value === "object") {
    return `{${Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => `${JSON.stringify(key)}:${stableStringify(item)}`)
      .join(",")}}`;
  }

  return JSON.stringify(value);
}
