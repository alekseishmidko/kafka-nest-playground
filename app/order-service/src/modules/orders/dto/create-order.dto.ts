export interface CreateOrderItemDto {
  productId: string;
  quantity: number;
  unitPrice: number;
}

export interface CreateOrderDto {
  userId: string;
  currency: string;
  items: CreateOrderItemDto[];
}
