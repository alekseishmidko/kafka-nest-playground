import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit
} from "@nestjs/common";
import {
  ApplicationMetrics,
  PinoLogger
} from "@kafka-playground/observability";
import {
  OutboxEventStatus,
  PostgresOutboxStore
} from "@kafka-playground/outbox";
import { DlqRepository } from "../dlq/dlq.repository";

/**
 * Синхронизирует Prometheus gauges с фактическим состоянием PostgreSQL.
 *
 * Gauge нельзя надёжно поддерживать только инкрементами: процесс может упасть
 * между изменением БД и обновлением метрики. Периодический SQL snapshot
 * самовосстанавливается после рестарта и остаётся источником правды.
 */
@Injectable()
export class OperationalMetricsCollector
  implements OnModuleInit, OnApplicationShutdown
{
  private readonly intervalMs = 15_000;
  private timer: NodeJS.Timeout | null = null;
  private collecting = false;

  constructor(
    private readonly outboxRepository: PostgresOutboxStore,
    private readonly dlqRepository: DlqRepository,
    private readonly metrics: ApplicationMetrics,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(OperationalMetricsCollector.name);
  }

  onModuleInit(): void {
    void this.collect();
    this.timer = setInterval(() => {
      void this.collect();
    }, this.intervalMs);
  }

  onApplicationShutdown(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Выполняет один согласованный для мониторинга snapshot.
   */
  async collect(): Promise<void> {
    if (this.collecting) {
      return;
    }

    this.collecting = true;

    try {
      const [outboxCounts, dlqNewCount] = await Promise.all([
        this.outboxRepository.countByStatuses(),
        this.dlqRepository.countNew()
      ]);

      for (const status of Object.values(OutboxEventStatus)) {
        this.metrics.setOutboxCount(status, outboxCounts[status]);
      }
      this.metrics.setDlqNewCount(dlqNewCount);
    } catch (error) {
      this.logger.warn(
        {
          error:
            error instanceof Error ? error.message : String(error)
        },
        "Operational metrics collection failed"
      );
    } finally {
      this.collecting = false;
    }
  }
}
