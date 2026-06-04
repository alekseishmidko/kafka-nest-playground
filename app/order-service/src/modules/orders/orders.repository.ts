import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { OrderEntity, OrderStatus, type OrderItemSnapshot } from "./entities/order.entity";

export interface CreatePendingOrderParams {
  userId: string;
  currency: string;
  totalAmount: number;
  itemCount: number;
  items: OrderItemSnapshot[];
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

  async updateStatus(orderId: string, status: OrderStatus): Promise<boolean> {
    const result = await this.repository.update({ id: orderId }, { status });

    return (result.affected ?? 0) > 0;
  }
}
