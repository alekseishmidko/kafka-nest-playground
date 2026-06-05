import { Inject, Injectable, OnModuleInit } from "@nestjs/common";
import { ClientGrpc } from "@nestjs/microservices";
import { PinoLogger } from "@kafka-playground/observability";
import { lastValueFrom, Observable } from "rxjs";
import { ORDERS_GRPC_CLIENT } from "../../grpc/grpc-clients.constants";
import type { CreateOrderDto } from "./dto/create-order.dto";
import type { OrderResponseDto } from "./dto/order-response.dto";

interface OrdersGrpcService {
  createOrder(payload: CreateOrderDto): Observable<OrderResponseDto>;
}

@Injectable()
export class OrdersService implements OnModuleInit {
  private ordersGrpcService!: OrdersGrpcService;

  constructor(
    @Inject(ORDERS_GRPC_CLIENT) private readonly client: ClientGrpc,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(OrdersService.name);
  }

  onModuleInit() {
    this.ordersGrpcService =
      this.client.getService<OrdersGrpcService>("OrdersService");
  }

  async createOrder(dto: CreateOrderDto): Promise<OrderResponseDto> {
    this.logger.info(
      {
        userId: dto.userId,
        currency: dto.currency,
        itemCount: dto.items.reduce((sum, item) => sum + item.quantity, 0)
      },
      "Forwarding create order request to order-service via gRPC"
    );

    const order = await lastValueFrom(this.ordersGrpcService.createOrder(dto));

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
}
