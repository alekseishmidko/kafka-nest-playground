import { Module } from "@nestjs/common";
import { DlqModule } from "../dlq/dlq.module";
import { OrdersModule } from "../orders/orders.module";
import { OperationalMetricsCollector } from "./operational-metrics.collector";

/**
 * Собирает DB-backed operational metrics order-service.
 */
@Module({
  imports: [OrdersModule, DlqModule],
  providers: [OperationalMetricsCollector]
})
export class OperationalMetricsModule {}
