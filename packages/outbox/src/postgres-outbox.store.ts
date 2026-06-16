import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { LessThanOrEqual, Repository } from "typeorm";
import {
  OutboxEventEntity,
  OutboxEventStatus
} from "./outbox-event.entity";
import type {
  CreateOutboxEventParams,
  TransactionalMessageStore
} from "./outbox-message-store";

/**
 * PostgreSQL/TypeORM реализация transactional outbox store.
 *
 * Store не публикует сообщения и не знает о Kafka. Его ответственность:
 * создавать `PENDING` entity, выбирать publishable-записи, сохранять результат
 * попытки публикации и отдавать snapshot для метрик.
 */
@Injectable()
export class PostgresOutboxStore
  implements TransactionalMessageStore<OutboxEventEntity>
{
  constructor(
    @InjectRepository(OutboxEventEntity)
    private readonly repository: Repository<OutboxEventEntity>
  ) {}

  /**
   * Создаёт entity в статусе `PENDING`, но не сохраняет её.
   *
   * Вызывающий код сохраняет entity через свой transaction manager вместе с
   * бизнес-сущностями. Это ключевая гарантия transactional outbox: бизнес-факт
   * и намерение отправить событие попадают в БД атомарно.
   */
  createPending(params: CreateOutboxEventParams): OutboxEventEntity {
    return this.repository.create({
      topic: params.topic,
      messageKey: params.messageKey,
      eventType: params.event.eventType,
      eventId: params.event.eventId,
      event: params.event,
      traceContext: params.traceContext ?? null,
      status: OutboxEventStatus.Pending
    });
  }

  /**
   * Возвращает события, которые можно публиковать прямо сейчас.
   *
   * В выборку попадают новые `PENDING` события и `FAILED` события, у которых
   * уже прошёл `nextAttemptAt`. Для нескольких publisher-инстансов в будущем
   * стоит заменить этот метод на lease или `FOR UPDATE SKIP LOCKED`.
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
   * Помечает запись как опубликованную.
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
 * Backward-compatible alias для сервисов, где старое имя уже отражено в DI.
 */
export { PostgresOutboxStore as OutboxRepository };

/**
 * Рассчитывает время следующего retry по экспоненциальному backoff.
 */
function nextAttemptAt(attempts: number): Date {
  const retryDelayMs = Math.min(30_000, 1000 * 2 ** Math.max(0, attempts - 1));

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
