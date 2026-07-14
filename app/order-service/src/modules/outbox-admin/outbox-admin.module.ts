import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { OutboxEventEntity } from "@kafka-playground/outbox";
import { OrdersModule } from "../orders/orders.module";
import { OutboxAdminController } from "./outbox-admin.controller";
import { OutboxAdminRepository } from "./outbox-admin.repository";
import { OutboxAdminService } from "./outbox-admin.service";

/**
 * Admin-модуль для просмотра и ручного восстановления transactional outbox.
 */
@Module({
  imports: [TypeOrmModule.forFeature([OutboxEventEntity]), OrdersModule],
  controllers: [OutboxAdminController],
  providers: [
    OutboxAdminRepository,
    OutboxAdminService
  ]
})
export class OutboxAdminModule {}
