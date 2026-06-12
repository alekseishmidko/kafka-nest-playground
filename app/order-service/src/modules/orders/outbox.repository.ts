import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { LessThanOrEqual, Repository } from "typeorm";
import type { DomainEvent, KafkaTopicName } from "@kafka-playground/contracts";
import { OutboxEventEntity, OutboxEventStatus } from "./entities/outbox-event.entity";

/**
 * Данные, достаточные для постановки доменного события в outbox.
 */
export interface CreateOutboxEventParams {
  /** Kafka topic, определённый контрактами события. */
  topic: KafkaTopicName;
  /** Kafka key, обычно id агрегата, по которому нужен ordering. */
  messageKey: string;
  /** Полный domain envelope, который будет опубликован без пересборки. */
  event: DomainEvent;
}

/**
 * Репозиторий для outbox-таблицы.
 *
 * Здесь намеренно нет Kafka-зависимостей: этот слой только выбирает события,
 * меняет их статус и хранит retry metadata. Публикация остаётся в
 * `OutboxPublisherService`.
 */
@Injectable()
export class OutboxRepository {
  constructor(
    @InjectRepository(OutboxEventEntity)
    private readonly repository: Repository<OutboxEventEntity>
  ) {}

  /**
   * Создаёт entity в статусе `PENDING`, но не сохраняет её.
   *
   * Метод оставлен для сценариев, где вызывающий код сам управляет транзакцией
   * и хочет сохранить outbox-запись вместе с другими entity через один manager.
   */
  createPending(params: CreateOutboxEventParams): OutboxEventEntity {
    return this.repository.create({
      topic: params.topic,
      messageKey: params.messageKey,
      eventType: params.event.eventType,
      eventId: params.event.eventId,
      event: params.event,
      status: OutboxEventStatus.Pending
    });
  }

  /**
   * Возвращает события, которые можно публиковать прямо сейчас.
   *
   * В выборку попадают:
   * - новые `PENDING` события;
   * - `FAILED` события, у которых уже прошёл `nextAttemptAt`.
   *
   * Порядок по `createdAt` делает отправку предсказуемой для одного процесса.
   * Для нескольких publisher-инстансов позже понадобится row locking
   * (`FOR UPDATE SKIP LOCKED`) или отдельный lease-механизм.
   */
  async findPublishable(limit: number): Promise<OutboxEventEntity[]> {
    const now = new Date();

    return this.repository.find({
      where: [
        { status: OutboxEventStatus.Pending },
        {
          status: OutboxEventStatus.Failed,
          nextAttemptAt: LessThanOrEqual(now)
        }
      ],
      order: {
        createdAt: "ASC"
      },
      take: limit
    });
  }

  /**
   * Помечает outbox-запись как опубликованную.
   *
   * Эта операция выполняется только после успешного Kafka publish. Если процесс
   * упадёт после publish, но до этого update, событие может быть отправлено
   * повторно. Это ожидаемый tradeoff outbox-паттерна, который закрывается
   * идемпотентностью consumer-ов.
   */
  async markPublished(id: string): Promise<void> {
    await this.repository.update(
      { id },
      {
        status: OutboxEventStatus.Published,
        publishedAt: new Date(),
        lastError: null
      }
    );
  }

  /**
   * Сохраняет ошибку публикации и планирует следующую попытку.
   *
   * Backoff ограничен сверху, чтобы временная недоступность Kafka не создавала
   * tight loop и не забивала логи одинаковыми ошибками каждую миллисекунду.
   */
  async markFailed(id: string, attempts: number, error: unknown): Promise<void> {
    await this.repository.update(
      { id },
      {
        status: OutboxEventStatus.Failed,
        attempts,
        nextAttemptAt: nextAttemptAt(attempts),
        lastError: normalizeError(error)
      }
    );
  }

  /**
   * Возвращает количество строк по рабочим статусам для Prometheus gauge.
   */
  async countByStatuses(): Promise<Record<OutboxEventStatus, number>> {
    const rows: Array<{ status: OutboxEventStatus; count: string }> =
      await this.repository
        .createQueryBuilder("outbox")
        .select("outbox.status", "status")
        .addSelect("count(*)", "count")
        .groupBy("outbox.status")
        .getRawMany();
    const result = {
      [OutboxEventStatus.Pending]: 0,
      [OutboxEventStatus.Published]: 0,
      [OutboxEventStatus.Failed]: 0
    };

    for (const row of rows) {
      result[row.status] = Number(row.count);
    }

    return result;
  }
}

/**
 * Рассчитывает время следующего retry по экспоненциальному backoff.
 */
function nextAttemptAt(attempts: number): Date {
  const retryDelayMs = Math.min(30000, 1000 * 2 ** Math.max(0, attempts - 1));

  return new Date(Date.now() + retryDelayMs);
}

/**
 * Приводит неизвестную ошибку к строке, пригодной для хранения в БД.
 */
function normalizeError(error: unknown): string {
  if (error instanceof Error) {
    return error.stack ?? error.message;
  }

  return String(error);
}
