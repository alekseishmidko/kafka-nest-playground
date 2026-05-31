import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn
} from "typeorm";

export enum OrderStatus {
  Pending = "PENDING"
}

export interface OrderItemSnapshot {
  productId: string;
  quantity: number;
  unitPrice: number;
}

@Entity({ name: "orders" })
export class OrderEntity {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ type: "varchar", length: 120 })
  userId!: string;

  @Column({ type: "varchar", length: 3 })
  currency!: string;

  @Column({ type: "numeric", precision: 12, scale: 2 })
  totalAmount!: string;

  @Column({ type: "int" })
  itemCount!: number;

  @Column({
    type: "enum",
    enum: OrderStatus,
    default: OrderStatus.Pending
  })
  status!: OrderStatus;

  @Column({ type: "jsonb" })
  items!: OrderItemSnapshot[];

  @CreateDateColumn({ type: "timestamptz" })
  createdAt!: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt!: Date;
}
