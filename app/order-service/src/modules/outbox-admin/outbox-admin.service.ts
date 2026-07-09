import {
  ConflictException,
  Injectable,
  NotFoundException
} from "@nestjs/common";
import {
  OutboxEventEntity,
  OutboxEventStatus,
  OutboxPublisherService
} from "@kafka-playground/outbox";
import { DataSource, type EntityManager } from "typeorm";
import { OutboxAdminRepository } from "./outbox-admin.repository";

export interface OutboxEventPage {
  items: OutboxEventEntity[];
  total: number;
  limit: number;
  offset: number;
}

export interface RetryFailedOutboxResult {
  retried: number;
  limit: number;
}

/**
 * Use cases административного управления transactional outbox.
 *
 * Сервис не публикует события напрямую: он только переводит записи в состояние,
 * которое уже существующий `OutboxPublisherService` умеет забрать через
 * `findPublishable`. Так admin API не дублирует delivery logic и не нарушает
 * lease-защиту между publisher-репликами.
 */
@Injectable()
export class OutboxAdminService {
  constructor(
    private readonly repository: OutboxAdminRepository,
    private readonly dataSource: DataSource,
    private readonly outboxPublisher: OutboxPublisherService
  ) {}

  async findPage(params: {
    status?: OutboxEventStatus;
    limit: number;
    offset: number;
  }): Promise<OutboxEventPage> {
    const page = await this.repository.findPage(params);

    return {
      ...page,
      limit: params.limit,
      offset: params.offset
    };
  }

  async findOne(id: string): Promise<OutboxEventEntity> {
    const entity = await this.repository.findById(id);

    if (!entity) {
      throw new NotFoundException(`Outbox event ${id} was not found`);
    }

    return entity;
  }

  /**
   * Снимает backoff с одной FAILED-записи и запускает publisher fast path.
   */
  async retryOne(id: string): Promise<OutboxEventEntity> {
    const entity = await this.dataSource.transaction((manager) =>
      lockOutboxEvent(manager, id, (outbox) => {
        assertRetryable(outbox);
        outbox.nextAttemptAt = null;
        outbox.lockedBy = null;
        outbox.lockedUntil = null;

        return manager.save(outbox);
      })
    );

    void this.outboxPublisher.publishPending();

    return entity;
  }

  /**
   * Снимает backoff с пачки FAILED-записей.
   */
  async retryFailed(limit: number): Promise<RetryFailedOutboxResult> {
    const retried = await this.repository.makeFailedReadyForRetry(limit);

    if (retried > 0) {
      void this.outboxPublisher.publishPending();
    }

    return { retried, limit };
  }

  /**
   * Исключает stuck outbox-запись из публикации после ручного разбора.
   */
  async ignore(
    id: string,
    params: {
      operatorId: string;
      reason: string;
    }
  ): Promise<OutboxEventEntity> {
    const reason = requireReason(params.reason);

    return this.dataSource.transaction((manager) =>
      lockOutboxEvent(manager, id, (outbox) => {
        assertIgnorable(outbox);
        outbox.status = OutboxEventStatus.Ignored;
        outbox.nextAttemptAt = null;
        outbox.lockedBy = null;
        outbox.lockedUntil = null;
        outbox.lastError = `Ignored by ${params.operatorId}: ${reason}`;

        return manager.save(outbox);
      })
    );
  }
}

async function lockOutboxEvent<T>(
  manager: EntityManager,
  id: string,
  action: (outbox: OutboxEventEntity) => Promise<T>
): Promise<T> {
  const entity = await manager.findOne(OutboxEventEntity, {
    where: { id },
    lock: { mode: "pessimistic_write" }
  });

  if (!entity) {
    throw new NotFoundException(`Outbox event ${id} was not found`);
  }

  return action(entity);
}

function assertRetryable(outbox: OutboxEventEntity): void {
  if (outbox.status !== OutboxEventStatus.Failed) {
    throw new ConflictException(
      `Only FAILED outbox events can be retried, got ${outbox.status}`
    );
  }
}

function assertIgnorable(outbox: OutboxEventEntity): void {
  if (
    outbox.status !== OutboxEventStatus.Failed &&
    outbox.status !== OutboxEventStatus.Pending
  ) {
    throw new ConflictException(
      `Only PENDING or FAILED outbox events can be ignored, got ${outbox.status}`
    );
  }
}

function requireReason(value: string): string {
  const reason = value.trim();

  if (reason.length < 5 || reason.length > 1000) {
    throw new ConflictException(
      "reason must contain between 5 and 1000 characters"
    );
  }

  return reason;
}
