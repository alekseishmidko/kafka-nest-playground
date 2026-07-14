import { MiddlewareConsumer, Module, NestModule } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { AdminSecurityModule } from "../admin-security";
import { AdminAuditController } from "./admin-audit.controller";
import { AdminAuditMiddleware } from "./admin-audit.middleware";
import { AdminAuditService } from "./admin-audit.service";
import { AdminAuditEventEntity } from "./entities/admin-audit-event.entity";

/**
 * Общий audit layer для всех order-service admin endpoints.
 */
@Module({
  imports: [
    AdminSecurityModule,
    TypeOrmModule.forFeature([AdminAuditEventEntity])
  ],
  controllers: [AdminAuditController],
  providers: [AdminAuditMiddleware, AdminAuditService],
  exports: [AdminAuditService]
})
export class AdminAuditModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(AdminAuditMiddleware).forRoutes("*");
  }
}
