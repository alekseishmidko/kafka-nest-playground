import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { OutboxEventEntity } from "@kafka-playground/outbox";
import { OrdersModule } from "../orders/orders.module";
import { DlqConsumer } from "./dlq.consumer";
import { DlqController } from "./dlq.controller";
import { DlqRepository } from "./dlq.repository";
import { DlqService } from "./dlq.service";
import { DeadLetterEventEntity } from "./entities/dead-letter-event.entity";
import { DlqAuditLogEntity } from "./entities/dlq-audit-log.entity";
import {
  DlqApiKeyGuard,
  DlqRateLimitGuard
} from "./dlq-auth";
import { DlqRetentionService } from "./dlq-retention.service";

/**
 * Изолированный platform-модуль управления Dead Letter Queue.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([
      DeadLetterEventEntity,
      DlqAuditLogEntity,
      OutboxEventEntity
    ]),
    OrdersModule
  ],
  controllers: [DlqController],
  providers: [
    DlqApiKeyGuard,
    DlqRateLimitGuard,
    DlqConsumer,
    DlqRepository,
    DlqRetentionService,
    DlqService
  ],
  exports: [DlqRepository]
})
export class DlqModule {}
