import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import {
  type DeadLetterEvent,
  type KafkaTopicName
} from "@kafka-playground/contracts";
import {
  type FindOptionsWhere,
  Repository
} from "typeorm";
import {
  DeadLetterEventEntity,
  DeadLetterEventStatus
} from "./entities/dead-letter-event.entity";

export interface SaveDeadLetterEventParams {
  event: DeadLetterEvent;
  messageKey: string | null;
  errorCode: string;
  retryCount: number;
  firstFailedAt: Date | null;
  originalEvent: DeadLetterEventEntity["originalEvent"];
}

export interface FindDeadLetterEventsParams {
  status?: DeadLetterEventStatus;
  limit: number;
  offset: number;
}

/**
 * Инкапсулирует запросы административного DLQ-хранилища.
 */
@Injectable()
export class DlqRepository {
  constructor(
    @InjectRepository(DeadLetterEventEntity)
    private readonly repository: Repository<DeadLetterEventEntity>
  ) {}

  /**
   * Идемпотентно сохраняет Kafka `DeadLetterEvent`.
   *
   * `orIgnore()` опирается на unique constraint `dead_letter_event_id`.
   * Повторная доставка после rebalance не создаст вторую административную
   * запись и не сбросит уже принятое оператором решение.
   */
  async saveIfAbsent(
    params: SaveDeadLetterEventParams
  ): Promise<DeadLetterEventEntity> {
    await this.repository
      .createQueryBuilder()
      .insert()
      .into(DeadLetterEventEntity)
      .values({
        deadLetterEventId: params.event.eventId,
        originalEventId: params.originalEvent?.eventId ?? null,
        originalTopic:
          params.event.payload.originalTopic as KafkaTopicName,
        originalPartition: params.event.payload.originalPartition,
        originalOffset: params.event.payload.originalOffset,
        messageKey: params.messageKey,
        errorCode: params.errorCode,
        errorMessage: params.event.payload.errorMessage,
        errorStack: params.event.payload.errorStack,
        retryCount: params.retryCount,
        firstFailedAt: params.firstFailedAt,
        originalEvent: params.originalEvent,
        status: DeadLetterEventStatus.New
      })
      .orIgnore()
      .execute();

    return this.repository.findOneByOrFail({
      deadLetterEventId: params.event.eventId
    });
  }

  async findPage(
    params: FindDeadLetterEventsParams
  ): Promise<{ items: DeadLetterEventEntity[]; total: number }> {
    const where: FindOptionsWhere<DeadLetterEventEntity> = params.status
      ? { status: params.status }
      : {};
    const [items, total] = await this.repository.findAndCount({
      where,
      order: {
        createdAt: "DESC"
      },
      take: params.limit,
      skip: params.offset
    });

    return { items, total };
  }

  async findById(id: string): Promise<DeadLetterEventEntity | null> {
    return this.repository.findOneBy({ id });
  }
}
