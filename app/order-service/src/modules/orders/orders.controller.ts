import { Body, Controller, Post } from "@nestjs/common";
import { GrpcMethod } from "@nestjs/microservices";
import type { CreateOrderDto } from "./dto/create-order.dto";
import { OrdersService } from "./orders.service";

@Controller("orders")
export class OrdersController {
  constructor(private readonly ordersService: OrdersService) {}

  @Post()
  createOrderHttp(@Body() dto: CreateOrderDto) {
    return this.ordersService.createOrder(dto);
  }

  @GrpcMethod("OrdersService", "CreateOrder")
  createOrder(dto: CreateOrderDto) {
    return this.ordersService.createOrder(dto);
  }
}
