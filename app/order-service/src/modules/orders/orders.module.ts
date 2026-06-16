import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import {
  OutboxEventEntity,
  OutboxPublisherService,
  PostgresOutboxStore
} from "@kafka-playground/outbox";
import { OrderEntity } from "./entities/order.entity";
import { ProcessedKafkaEventEntity } from "./entities/processed-kafka-event.entity";
import { OrdersEventsConsumer } from "./orders-events.consumer";
import { OrdersController } from "./orders.controller";
import { OrdersRepository } from "./orders.repository";
import { OrdersService } from "./orders.service";

@Module({
  imports: [
    TypeOrmModule.forFeature([
      OrderEntity,
      OutboxEventEntity,
      ProcessedKafkaEventEntity
    ])
  ],
  controllers: [OrdersController],
  providers: [
    OrdersEventsConsumer,
    OrdersRepository,
    OrdersService,
    OutboxPublisherService,
    PostgresOutboxStore
  ],
  exports: [OutboxPublisherService, PostgresOutboxStore]
})
export class OrdersModule {}
