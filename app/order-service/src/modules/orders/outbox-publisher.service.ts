import { Injectable, OnApplicationShutdown, OnModuleInit } from "@nestjs/common";
import { KafkaProducerService } from "@kafka-playground/kafka";
import { PinoLogger } from "@kafka-playground/observability";
import { OutboxRepository } from "./outbox.repository";

/**
 * Фоновый publisher для transactional outbox.
 *
 * Сервис периодически читает `outbox_events`, публикует события в Kafka и
 * помечает записи как `PUBLISHED`. Он не участвует в HTTP/gRPC ответе клиенту:
 * заказ считается созданным после DB commit, а Kafka-доставка догоняет его
 * асинхронно.
 *
 * Текущая реализация рассчитана на один инстанс `order-service`. Для нескольких
 * инстансов нужно добавить конкурентную аренду строк или выборку через
 * `FOR UPDATE SKIP LOCKED`, иначе два процесса могут одновременно взять одну
 * и ту же pending-запись.
 */
@Injectable()
export class OutboxPublisherService implements OnModuleInit, OnApplicationShutdown {
  /** Частота polling-а outbox-таблицы в local/dev реализации. */
  private readonly intervalMs = 1000;
  /** Максимум событий за один проход, чтобы publisher не монополизировал процесс. */
  private readonly batchSize = 25;
  private timer: NodeJS.Timeout | null = null;
  /** Простая защита от наложения двух publish-циклов внутри одного Node.js процесса. */
  private isPublishing = false;

  constructor(
    private readonly outboxRepository: OutboxRepository,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(OutboxPublisherService.name);
  }

  /**
   * Запускает периодический polling и сразу делает первый проход.
   *
   * Первый проход нужен, чтобы после рестарта сервиса быстро отправить события,
   * которые были сохранены в outbox до падения предыдущего процесса.
   */
  onModuleInit(): void {
    this.timer = setInterval(() => {
      void this.publishPending();
    }, this.intervalMs);

    void this.publishPending();
  }

  /**
   * Останавливает polling timer при shutdown, чтобы приложение корректно
   * завершалось без висящих interval handles.
   */
  onApplicationShutdown(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Публикует доступные outbox-события.
   *
   * Алгоритм:
   * 1. Берём ограниченную пачку publishable-записей.
   * 2. Для каждой записи вызываем Kafka producer.
   * 3. После успеха помечаем запись `PUBLISHED`.
   * 4. После ошибки сохраняем `FAILED`, увеличиваем attempts и ставим backoff.
   *
   * Важно: если publish в Kafka прошёл, а update `PUBLISHED` не успел
   * выполниться из-за падения процесса, событие будет опубликовано повторно.
   * Поэтому этот сервис даёт "не потерять событие", но не обещает exactly-once.
   */
  async publishPending(): Promise<void> {
    if (this.isPublishing) {
      return;
    }

    this.isPublishing = true;

    try {
      const events = await this.outboxRepository.findPublishable(this.batchSize);

      for (const outboxEvent of events) {
        try {
          await this.kafkaProducer.publish({
            topic: outboxEvent.topic,
            key: outboxEvent.messageKey,
            event: outboxEvent.event,
            correlationId: outboxEvent.event.correlationId,
            causationId: outboxEvent.event.causationId ?? undefined
          });

          await this.outboxRepository.markPublished(outboxEvent.id);

          this.logger.info(
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

          await this.outboxRepository.markFailed(outboxEvent.id, attempts, error);

          this.logger.warn(
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
        }
      }
    } finally {
      this.isPublishing = false;
    }
  }
}
