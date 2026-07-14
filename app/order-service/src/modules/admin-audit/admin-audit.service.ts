import { Injectable, NotFoundException } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { PinoLogger } from "@kafka-playground/observability";
import { FindOptionsWhere, Repository } from "typeorm";
import {
  AdminAuditDecision,
  AdminAuditEventEntity
} from "./entities/admin-audit-event.entity";

export interface RecordAdminAuditEventParams {
  actor: string | null;
  role: string | null;
  method: string;
  path: string;
  action: string;
  entityType: string | null;
  entityId: string | null;
  decision: AdminAuditDecision;
  statusCode: number;
  requestId: string;
  correlationId: string | null;
  ip: string | null;
  userAgent: string | null;
  durationMs: number;
  metadata?: Record<string, unknown> | null;
}

export interface FindAdminAuditEventsParams {
  actor?: string;
  role?: string;
  method?: string;
  path?: string;
  action?: string;
  entityType?: string;
  entityId?: string;
  decision?: AdminAuditDecision;
  limit: number;
  offset: number;
}

export interface AdminAuditEventsPage {
  items: AdminAuditEventEntity[];
  total: number;
  limit: number;
  offset: number;
}

/**
 * Пишет append-only audit trail для административных HTTP-запросов.
 *
 * Сервис не участвует в бизнес-транзакциях: audit фиксирует факт HTTP-доступа к
 * admin API и итоговый статус ответа. Domain-specific журналы, например
 * `dlq_audit_log`, могут оставаться рядом с use case-ами для детального
 * описания доменного решения.
 */
@Injectable()
export class AdminAuditService {
  constructor(
    @InjectRepository(AdminAuditEventEntity)
    private readonly repository: Repository<AdminAuditEventEntity>,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(AdminAuditService.name);
  }

  async record(params: RecordAdminAuditEventParams): Promise<void> {
    try {
      await this.repository.insert({
        actor: params.actor,
        role: params.role,
        method: truncate(params.method, 16),
        path: params.path,
        action: truncate(params.action, 120),
        entityType: truncateNullable(params.entityType, 120),
        entityId: truncateNullable(params.entityId, 160),
        decision: params.decision,
        statusCode: params.statusCode,
        requestId: truncate(params.requestId, 160),
        correlationId: truncateNullable(params.correlationId, 160),
        ip: normalizeIp(params.ip),
        userAgent: params.userAgent,
        durationMs: Math.max(0, Math.round(params.durationMs)),
        metadata: (params.metadata ?? null) as never
      });
    } catch (error) {
      this.logger.warn(
        {
          error: error instanceof Error ? error.message : String(error),
          method: params.method,
          path: params.path,
          requestId: params.requestId
        },
        "Admin audit event was not persisted"
      );
    }
  }

  /**
   * Возвращает страницу audit events для расследований admin-действий.
   *
   * Фильтры намеренно работают как exact match: такой API проще объяснить,
   * логировать и использовать в incident review. Если позже понадобится поиск
   * по диапазону дат или partial match по path, лучше добавить отдельные query
   * параметры и индексы под конкретные сценарии, а не усложнять текущий контракт.
   */
  async findPage(
    params: FindAdminAuditEventsParams
  ): Promise<AdminAuditEventsPage> {
    const where = createFindWhere(params);
    const [items, total] = await this.repository.findAndCount({
      where,
      order: {
        createdAt: "DESC"
      },
      take: params.limit,
      skip: params.offset
    });

    return {
      items,
      total,
      limit: params.limit,
      offset: params.offset
    };
  }

  /**
   * Возвращает одну audit-запись по UUID.
   */
  async findOne(id: string): Promise<AdminAuditEventEntity> {
    const event = await this.repository.findOneBy({ id });

    if (!event) {
      throw new NotFoundException(`Admin audit event ${id} was not found`);
    }

    return event;
  }
}

function createFindWhere(
  params: FindAdminAuditEventsParams
): FindOptionsWhere<AdminAuditEventEntity> {
  const where: FindOptionsWhere<AdminAuditEventEntity> = {};

  if (params.actor !== undefined) {
    where.actor = params.actor;
  }

  if (params.role !== undefined) {
    where.role = params.role;
  }

  if (params.method !== undefined) {
    where.method = params.method;
  }

  if (params.path !== undefined) {
    where.path = params.path;
  }

  if (params.action !== undefined) {
    where.action = params.action;
  }

  if (params.entityType !== undefined) {
    where.entityType = params.entityType;
  }

  if (params.entityId !== undefined) {
    where.entityId = params.entityId;
  }

  if (params.decision !== undefined) {
    where.decision = params.decision;
  }

  return where;
}

function truncate(value: string, maxLength: number): string {
  return value.length > maxLength ? value.slice(0, maxLength) : value;
}

function truncateNullable(
  value: string | null,
  maxLength: number
): string | null {
  return value === null ? null : truncate(value, maxLength);
}

function normalizeIp(value: string | null): string | null {
  if (!value) {
    return null;
  }

  const firstIp = value.split(",")[0]?.trim();
  return firstIp === "" ? null : firstIp;
}
