import { Module } from "@nestjs/common";
import { TechnicalRetentionService } from "./technical-retention.service";

/**
 * Плановая очистка технических таблиц order-service.
 */
@Module({
  providers: [TechnicalRetentionService]
})
export class TechnicalRetentionModule {}
