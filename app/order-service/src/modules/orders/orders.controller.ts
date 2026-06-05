import { Controller } from "@nestjs/common";
import { GrpcMethod } from "@nestjs/microservices";
import type { CreateOrderDto } from "./dto/create-order.dto";
import { OrdersService } from "./orders.service";

@Controller()
export class OrdersController {
  constructor(private readonly ordersService: OrdersService) {}

  @GrpcMethod("OrdersService", "CreateOrder")
  createOrder(dto: CreateOrderDto) {
    return this.ordersService.createOrder(dto);
  }
}
