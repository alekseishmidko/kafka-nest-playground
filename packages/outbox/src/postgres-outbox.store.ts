import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import {
  OutboxEventEntity,
  OutboxEventStatus
} from "./outbox-event.entity";
import { createOutboxEventEntity } from "./create-outbox-event-entity";
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
    return this.repository.create(createOutboxEventEntity(params));
  }

  /**
   * Атомарно забирает события, которые можно публиковать прямо сейчас.
   *
   * В выборку попадают новые `PENDING` события и `FAILED` события, у которых
   * уже прошёл `nextAttemptAt`. `FOR UPDATE SKIP LOCKED` нужен для нескольких
   * publisher-реплик: первая транзакция блокирует и помечает строки lease-ом,
   * остальные реплики пропускают эти строки и не публикуют тот же event.
   */
  async findPublishable(
    limit: number,
    options: {
      ownerId: string;
      leaseMs: number;
    } = {
      ownerId: "outbox-publisher",
      leaseMs: 30_000
    }
  ): Promise<OutboxEventEntity[]> {
    const rows = await this.repository.manager.query(
      `
        with candidates as (
          select id
          from outbox_events
          where (
              status = 'PENDING'
              or (
                status = 'FAILED'
                and (next_attempt_at is null or next_attempt_at <= now())
              )
            )
            and (locked_until is null or locked_until <= now())
          order by created_at asc, id asc
          limit $1
          for update skip locked
        )
        update outbox_events outbox
        set
          locked_by = $2,
          locked_until = now() + ($3::text || ' milliseconds')::interval,
          updated_at = now()
        from candidates
        where outbox.id = candidates.id
        returning
          outbox.id,
          outbox.topic,
          outbox.message_key as "messageKey",
          outbox.event_type as "eventType",
          outbox.event_id as "eventId",
          outbox.event,
          outbox.trace_context as "traceContext",
          outbox.status,
          outbox.attempts,
          outbox.next_attempt_at as "nextAttemptAt",
          outbox.locked_by as "lockedBy",
          outbox.locked_until as "lockedUntil",
          outbox.published_at as "publishedAt",
          outbox.last_error as "lastError",
          outbox.created_at as "createdAt",
          outbox.updated_at as "updatedAt"
      `,
      [limit, options.ownerId, options.leaseMs]
    );

    return rows as OutboxEventEntity[];
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
        lastError: null,
        lockedBy: null,
        lockedUntil: null
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
        lastError: normalizeError(error),
        lockedBy: null,
        lockedUntil: null
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
