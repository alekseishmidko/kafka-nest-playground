import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { OrderEntity } from "./entities/order.entity";
import { OutboxEventEntity } from "./entities/outbox-event.entity";
import { ProcessedKafkaEventEntity } from "./entities/processed-kafka-event.entity";
import { OutboxPublisherService } from "./outbox-publisher.service";
import { OutboxRepository } from "./outbox.repository";
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
    OutboxRepository
  ]
})
export class OrdersModule {}
