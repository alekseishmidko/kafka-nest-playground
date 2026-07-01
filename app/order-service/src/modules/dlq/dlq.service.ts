import {
  ConflictException,
  Injectable,
  NotFoundException,
  UnprocessableEntityException
} from "@nestjs/common";
import { DataSource } from "typeorm";
import type { DeadLetterEvent } from "@kafka-playground/contracts";
import {
  KAFKA_HEADER_NAMES,
  readHeader,
  type KafkaConsumerMessageContext
} from "@kafka-playground/kafka";
import {
  captureActiveTraceContext,
  PinoLogger,
  runInTraceSpan,
  SpanKind
} from "@kafka-playground/observability";
import {
  createOutboxEventEntity,
  OutboxPublisherService
} from "@kafka-playground/outbox";
import {
  createReprocessedEvent,
  type CorrectedPayload
} from "./dlq-reprocess.factory";
import { parseOriginalEvent } from "./dlq-event.parser";
import { DlqRepository } from "./dlq.repository";
import {
  DeadLetterEventEntity,
  DeadLetterEventStatus
} from "./entities/dead-letter-event.entity";
import {
  DlqAuditAction,
  DlqAuditLogEntity
} from "./entities/dlq-audit-log.entity";

export interface DeadLetterEventPage {
  items: DeadLetterEventEntity[];
  total: number;
  limit: number;
  offset: number;
}

/**
 * Реализует use cases административного управления DLQ.
 *
 * Сервис не содержит HTTP- или Kafka-специфичной маршрутизации. Consumer
 * передаёт сюда полученное событие, controller вызывает чтение и команды
 * оператора, а транзакционные гарантии сосредоточены в одном месте.
 */
@Injectable()
export class DlqService {
  constructor(
    private readonly repository: DlqRepository,
    private readonly dataSource: DataSource,
    private readonly outboxPublisher: OutboxPublisherService,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(DlqService.name);
  }

  /**
   * Сохраняет `DeadLetterEvent`, полученный из Kafka.
   */
  async capture(
    context: KafkaConsumerMessageContext<DeadLetterEvent>
  ): Promise<DeadLetterEventEntity> {
    const originalEvent = parseOriginalEvent(
      context.event.payload.rawEvent
    );
    const entity = await this.repository.saveIfAbsent({
      event: context.event,
      messageKey: context.key,
      errorCode:
        readHeader(context.headers, KAFKA_HEADER_NAMES.errorCode) ??
        "UNKNOWN_ERROR",
      retryCount: readNonNegativeIntegerHeader(
        context.headers,
        KAFKA_HEADER_NAMES.retryCount
      ),
      firstFailedAt: readDateHeader(
        context.headers,
        KAFKA_HEADER_NAMES.firstFailedAt
      ),
      originalEvent
    });

    this.logger.warn(
      {
        dlqId: entity.id,
        deadLetterEventId: entity.deadLetterEventId,
        originalEventId: entity.originalEventId,
        originalTopic: entity.originalTopic,
        errorCode: entity.errorCode,
        retryCount: entity.retryCount
      },
      "Dead letter event persisted"
    );

    return entity;
  }

  async findPage(params: {
    status?: DeadLetterEventStatus;
    limit: number;
    offset: number;
  }): Promise<DeadLetterEventPage> {
    const page = await this.repository.findPage(params);

    return {
      ...page,
      limit: params.limit,
      offset: params.offset
    };
  }

  async findOne(id: string): Promise<DeadLetterEventEntity> {
    const entity = await this.repository.findById(id);

    if (!entity) {
      throw new NotFoundException(`DLQ event ${id} was not found`);
    }

    return entity;
  }

  /**
   * Ставит исправленную копию исходного события в transactional outbox.
   *
   * Row lock предотвращает две одновременные reprocess-команды. Изменение
   * DLQ-статуса и создание outbox-записи выполняются в одной транзакции:
   * невозможно получить `REPROCESSED` без гарантированной будущей публикации.
   */
  async reprocess(
    id: string,
    correctedPayload: CorrectedPayload,
    params: {
      expectedVersion: number;
      operatorId: string;
      comment: string;
    }
  ): Promise<DeadLetterEventEntity> {
    const comment = requireComment(params.comment);
    const result = await runInTraceSpan(
      "dlq reprocess",
      {
        kind: SpanKind.INTERNAL,
        attributes: {
          "dlq.event.id": id,
          "dlq.operator.id": params.operatorId,
          "dlq.operation": "reprocess"
        }
      },
      () => this.dataSource.transaction(async (manager) => {
      const entity = await manager.findOne(DeadLetterEventEntity, {
        where: { id },
        lock: { mode: "pessimistic_write" }
      });

      if (!entity) {
        throw new NotFoundException(`DLQ event ${id} was not found`);
      }

      assertNewStatus(entity);
      assertExpectedVersion(entity, params.expectedVersion);

      if (!entity.originalEvent) {
        throw new UnprocessableEntityException(
          "DLQ event does not contain a valid original event envelope"
        );
      }

      const reprocessed = createReprocessedEvent(
        entity.originalEvent,
        correctedPayload,
        entity.originalTopic
      );

      await manager.save(
        createOutboxEventEntity({
          topic: reprocessed.topic,
          messageKey: entity.messageKey ?? reprocessed.messageKey,
          event: reprocessed.event,
          traceContext: captureActiveTraceContext()
        })
      );

      entity.status = DeadLetterEventStatus.Reprocessed;
      entity.reprocessedEventId = reprocessed.event.eventId;
      entity.reprocessedAt = new Date();
      entity.resolvedBy = params.operatorId;
      entity.resolutionComment = comment;
      const savedEntity = await manager.save(entity);

      await manager.save(
        manager.create(DlqAuditLogEntity, {
          deadLetterEventId: entity.id,
          action: DlqAuditAction.Reprocess,
          operatorId: params.operatorId,
          previousStatus: DeadLetterEventStatus.New,
          newStatus: DeadLetterEventStatus.Reprocessed,
          comment,
          reprocessedEventId: reprocessed.event.eventId
        })
      );

      return {
        entity: savedEntity,
        reprocessedEvent: reprocessed.event
      };
      })
    );

    // Это только fast path. Если Kafka недоступна, outbox publisher сохранит
    // FAILED и повторит отправку по своему backoff.
    void this.outboxPublisher.publishPending();

    this.logger.info(
      {
        dlqId: result.entity.id,
        deadLetterEventId: result.entity.deadLetterEventId,
        originalEventId: result.entity.originalEventId,
        reprocessedEventId: result.entity.reprocessedEventId,
        eventType: result.reprocessedEvent.eventType,
        topic: result.entity.originalTopic,
        operatorId: params.operatorId
      },
      "Dead letter event accepted for reprocessing"
    );

    return result.entity;
  }

  /**
   * Помечает событие как осознанно проигнорированное.
   */
  async ignore(
    id: string,
    reason: string,
    params: {
      expectedVersion: number;
      operatorId: string;
    }
  ): Promise<DeadLetterEventEntity> {
    const normalizedReason = requireComment(reason);

    return runInTraceSpan(
      "dlq ignore",
      {
        kind: SpanKind.INTERNAL,
        attributes: {
          "dlq.event.id": id,
          "dlq.operator.id": params.operatorId,
          "dlq.operation": "ignore"
        }
      },
      () => this.dataSource.transaction(async (manager) => {
      const entity = await manager.findOne(DeadLetterEventEntity, {
        where: { id },
        lock: { mode: "pessimistic_write" }
      });

      if (!entity) {
        throw new NotFoundException(`DLQ event ${id} was not found`);
      }

      assertNewStatus(entity);
      assertExpectedVersion(entity, params.expectedVersion);

      entity.status = DeadLetterEventStatus.Ignored;
      entity.ignoredAt = new Date();
      entity.ignoreReason = normalizedReason;
      entity.resolvedBy = params.operatorId;
      entity.resolutionComment = normalizedReason;

      const saved = await manager.save(entity);
      await manager.save(
        manager.create(DlqAuditLogEntity, {
          deadLetterEventId: entity.id,
          action: DlqAuditAction.Ignore,
          operatorId: params.operatorId,
          previousStatus: DeadLetterEventStatus.New,
          newStatus: DeadLetterEventStatus.Ignored,
          comment: normalizedReason,
          reprocessedEventId: null
        })
      );

      this.logger.info(
        {
          dlqId: saved.id,
          deadLetterEventId: saved.deadLetterEventId,
          reason: normalizedReason,
          operatorId: params.operatorId
        },
        "Dead letter event ignored"
      );

      return saved;
      })
    );
  }
}

function assertExpectedVersion(
  entity: DeadLetterEventEntity,
  expectedVersion: number
): void {
  if (
    !Number.isInteger(expectedVersion) ||
    expectedVersion < 1
  ) {
    throw new UnprocessableEntityException(
      "version must be a positive integer"
    );
  }

  if (entity.version !== expectedVersion) {
    throw new ConflictException(
      `DLQ event ${entity.id} version changed from ${expectedVersion} to ${entity.version}`
    );
  }
}

function requireComment(value: string): string {
  const comment = value.trim();

  if (comment.length < 5 || comment.length > 1000) {
    throw new UnprocessableEntityException(
      "Operator comment must contain between 5 and 1000 characters"
    );
  }

  return comment;
}

function assertNewStatus(entity: DeadLetterEventEntity): void {
  if (entity.status !== DeadLetterEventStatus.New) {
    throw new ConflictException(
      `DLQ event ${entity.id} is already ${entity.status}`
    );
  }
}

function readNonNegativeIntegerHeader(
  headers: KafkaConsumerMessageContext["headers"],
  name: string
): number {
  const value = Number(readHeader(headers, name) ?? "0");

  return Number.isInteger(value) && value >= 0 ? value : 0;
}

function readDateHeader(
  headers: KafkaConsumerMessageContext["headers"],
  name: string
): Date | null {
  const value = readHeader(headers, name);

  if (!value) {
    return null;
  }

  const date = new Date(value);

  return Number.isNaN(date.getTime()) ? null : date;
}
