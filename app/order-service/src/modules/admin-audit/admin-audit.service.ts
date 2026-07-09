import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { PinoLogger } from "@kafka-playground/observability";
import { Repository } from "typeorm";
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
