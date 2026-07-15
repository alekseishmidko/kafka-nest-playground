import { Controller } from "@nestjs/common";
import { GrpcMethod } from "@nestjs/microservices";
import type { Metadata } from "@grpc/grpc-js";
import type { CreateOrderDto } from "./dto/create-order.dto";
import { OrdersService } from "./orders.service";

@Controller()
export class OrdersController {
  constructor(private readonly ordersService: OrdersService) {}

  @GrpcMethod("OrdersService", "CreateOrder")
  createOrder(dto: CreateOrderDto, metadata?: Metadata) {
    return this.ordersService.createOrder(dto, {
      idempotencyKey: readMetadataValue(metadata, "idempotency-key"),
      requestHash: readMetadataValue(metadata, "idempotency-request-hash")
    });
  }

  @GrpcMethod("OrdersService", "CancelOrder")
  cancelOrder(command: {
    id: string;
    reason: string;
    requestedBy?: "user" | "operator";
  }) {
    return this.ordersService.cancelOrder(command);
  }
}

function readMetadataValue(
  metadata: Metadata | undefined,
  key: string
): string | undefined {
  const values = metadata?.get(key) ?? [];
  const value = values[0];

  return typeof value === "string" && value.trim() !== ""
    ? value
    : undefined;
}
