import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit
} from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { PinoLogger } from "@kafka-playground/observability";
import { DlqRepository } from "./dlq.repository";

/**
 * Удаляет старые завершённые DLQ-записи по retention policy.
 *
 * Записи `NEW` не удаляются независимо от возраста. Audit log удаляется через
 * FK `ON DELETE CASCADE` только вместе с уже завершённой DLQ-записью.
 */
@Injectable()
export class DlqRetentionService
  implements OnModuleInit, OnApplicationShutdown
{
  private readonly intervalMs = 24 * 60 * 60 * 1000;
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private readonly repository: DlqRepository,
    private readonly config: ConfigService,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(DlqRetentionService.name);
  }

  onModuleInit(): void {
    void this.cleanup();
    this.timer = setInterval(() => {
      void this.cleanup();
    }, this.intervalMs);
  }

  onApplicationShutdown(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async cleanup(): Promise<void> {
    try {
      const retentionDays = Number(
        this.config.get<string>("DLQ_RETENTION_DAYS", "90")
      );

      if (!Number.isFinite(retentionDays) || retentionDays < 1) {
        this.logger.warn(
          { retentionDays },
          "DLQ cleanup skipped because retention is invalid"
        );
        return;
      }

      const cutoff = new Date(
        Date.now() - retentionDays * 24 * 60 * 60 * 1000
      );
      const deleted = await this.repository.deleteResolvedBefore(cutoff);

      if (deleted > 0) {
        this.logger.info(
          { deleted, cutoff, retentionDays },
          "Resolved DLQ events deleted by retention policy"
        );
      }
    } catch (error) {
      this.logger.warn(
        {
          error:
            error instanceof Error ? error.message : String(error)
        },
        "DLQ retention cleanup failed"
      );
    }
  }
}
