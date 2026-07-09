import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import {
  OutboxEventEntity,
  OutboxEventStatus
} from "@kafka-playground/outbox";
import {
  type FindOptionsWhere,
  Repository
} from "typeorm";

export interface FindOutboxEventsParams {
  status?: OutboxEventStatus;
  limit: number;
  offset: number;
}

/**
 * Read/write adapter для административного просмотра outbox.
 *
 * Репозиторий оставляет бизнес-решения сервису: здесь только простые запросы и
 * bulk update, чтобы controller не зависел от TypeORM API напрямую.
 */
@Injectable()
export class OutboxAdminRepository {
  constructor(
    @InjectRepository(OutboxEventEntity)
    private readonly repository: Repository<OutboxEventEntity>
  ) {}

  async findPage(
    params: FindOutboxEventsParams
  ): Promise<{ items: OutboxEventEntity[]; total: number }> {
    const where: FindOptionsWhere<OutboxEventEntity> = params.status
      ? { status: params.status }
      : {};
    const [items, total] = await this.repository.findAndCount({
      where,
      order: {
        createdAt: "DESC",
        id: "DESC"
      },
      take: params.limit,
      skip: params.offset
    });

    return { items, total };
  }

  async findById(id: string): Promise<OutboxEventEntity | null> {
    return this.repository.findOneBy({ id });
  }

  /**
   * Делает все FAILED-записи publishable прямо сейчас.
   */
  async makeFailedReadyForRetry(limit: number): Promise<number> {
    const result = await this.repository.manager.query(
      `
        with candidates as (
          select id
          from outbox_events
          where status = 'FAILED'
          order by updated_at asc, id asc
          limit $1
          for update skip locked
        )
        update outbox_events outbox
        set
          next_attempt_at = null,
          locked_by = null,
          locked_until = null,
          updated_at = now()
        from candidates
        where outbox.id = candidates.id
        returning outbox.id
      `,
      [limit]
    );

    return Array.isArray(result) ? result.length : 0;
  }
}
