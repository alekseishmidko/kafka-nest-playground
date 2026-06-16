import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit
} from "@nestjs/common";
import { KafkaProducerService } from "@kafka-playground/kafka";
import {
  ApplicationMetrics,
  extractTraceContext,
  PinoLogger,
  runInTraceSpan,
  SpanKind
} from "@kafka-playground/observability";
import type {
  MessagePublisher,
  TransactionalMessageStore
} from "./outbox-message-store";
import { PostgresOutboxStore } from "./postgres-outbox.store";

/**
 * Фоновый publisher для transactional outbox.
 *
 * Алгоритм:
 *
 * 1. Периодически читает publishable-записи из `TransactionalMessageStore`.
 * 2. Публикует сохранённый event через `MessagePublisher`.
 * 3. После успеха переводит запись в `PUBLISHED`.
 * 4. После ошибки сохраняет `FAILED`, attempts и backoff.
 *
 * Если процесс упадёт после publish, но до `markPublished`, запись может быть
 * опубликована повторно. Это ожидаемый tradeoff outbox-паттерна: producer не
 * теряет события, а downstream consumers обязаны быть идемпотентными.
 */
@Injectable()
export class OutboxPublisherService
  implements OnModuleInit, OnApplicationShutdown
{
  /** Частота polling-а outbox-таблицы в local/dev реализации. */
  private readonly intervalMs = 1000;
  /** Максимум событий за один проход, чтобы publisher не монополизировал процесс. */
  private readonly batchSize = 25;
  private timer: NodeJS.Timeout | null = null;
  /** Защита от наложения двух publish-циклов внутри одного Node.js процесса. */
  private isPublishing = false;

  constructor(
    private readonly outboxStore: PostgresOutboxStore,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly logger: PinoLogger,
    private readonly metrics: ApplicationMetrics
  ) {
    this.logger.setContext(OutboxPublisherService.name);
  }

  /**
   * Запускает периодический polling и сразу делает первый проход.
   */
  onModuleInit(): void {
    this.timer = setInterval(() => {
      void this.publishPending();
    }, this.intervalMs);

    void this.publishPending();
  }

  /**
   * Останавливает polling timer при shutdown.
   */
  onApplicationShutdown(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Публикует доступные outbox-события.
   */
  async publishPending(): Promise<void> {
    await publishOutboxBatch({
      store: this.outboxStore,
      publisher: this.kafkaProducer,
      logger: this.logger,
      metrics: this.metrics,
      batchSize: this.batchSize,
      isPublishing: () => this.isPublishing,
      setPublishing: (value) => {
        this.isPublishing = value;
      }
    });
  }
}

/**
 * Выполняет один publish batch без привязки к NestJS lifecycle.
 *
 * Функция выделена отдельно, чтобы её можно было тестировать через fake store и
 * fake publisher без поднятия Nest application context.
 */
export async function publishOutboxBatch(params: {
  store: TransactionalMessageStore;
  publisher: MessagePublisher;
  logger: Pick<PinoLogger, "info" | "warn">;
  metrics?: Pick<ApplicationMetrics, "recordOutboxPublish">;
  batchSize: number;
  isPublishing(): boolean;
  setPublishing(value: boolean): void;
}): Promise<void> {
  if (params.isPublishing()) {
    return;
  }

  params.setPublishing(true);

  try {
    const events = await params.store.findPublishable(params.batchSize);

    for (const outboxEvent of events) {
      const parentContext = extractTraceContext(outboxEvent.traceContext);

      await runInTraceSpan(
        "outbox publish",
        {
          kind: SpanKind.PRODUCER,
          parentContext,
          attributes: {
            "messaging.system": "kafka",
            "messaging.destination.name": outboxEvent.topic,
            "messaging.operation.name": "publish",
            "outbox.event.id": outboxEvent.id,
            "event.id": outboxEvent.eventId,
            "event.type": outboxEvent.eventType
          }
        },
        async () => {
          try {
            await params.publisher.publish({
              topic: outboxEvent.topic,
              key: outboxEvent.messageKey,
              event: outboxEvent.event,
              correlationId: outboxEvent.event.correlationId,
              causationId: outboxEvent.event.causationId ?? undefined
            });

            await params.store.markPublished(outboxEvent.id);
            params.metrics?.recordOutboxPublish(outboxEvent.topic, "success");

            params.logger.info(
              {
                outboxEventId: outboxEvent.id,
                eventId: outboxEvent.eventId,
                eventType: outboxEvent.eventType,
                topic: outboxEvent.topic
              },
              "Outbox event published"
            );
          } catch (error) {
            const attempts = outboxEvent.attempts + 1;

            await params.store.markFailed(outboxEvent.id, attempts, error);
            params.metrics?.recordOutboxPublish(outboxEvent.topic, "failure");

            params.logger.warn(
              {
                outboxEventId: outboxEvent.id,
                eventId: outboxEvent.eventId,
                eventType: outboxEvent.eventType,
                topic: outboxEvent.topic,
                attempts,
                error
              },
              "Outbox event publish failed"
            );
            throw error;
          }
        }
      ).catch(() => {
        // Ошибка уже сохранена в outbox и записана в span. Следующая запись
        // пачки должна продолжить публикацию независимо от текущей.
      });
    }
  } finally {
    params.setPublishing(false);
  }
}
