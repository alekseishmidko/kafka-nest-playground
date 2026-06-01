import { Module } from "@nestjs/common";
import { RiskConsumer } from "./risk.consumer";
import { RiskScorer } from "./risk.scorer";
import { RiskService } from "./risk.service";

// Модуль risk изолирует всю доменную логику risk-service:
// Kafka consumer, scoring и публикацию результата.
@Module({
  providers: [RiskConsumer, RiskScorer, RiskService]
})
export class RiskModule {}
