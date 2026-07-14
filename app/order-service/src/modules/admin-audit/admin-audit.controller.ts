import {
  BadRequestException,
  Controller,
  DefaultValuePipe,
  Get,
  Param,
  ParseIntPipe,
  ParseUUIDPipe,
  Query,
  UseGuards
} from "@nestjs/common";
import {
  AdminApiKeyGuard,
  AdminPermission,
  AdminPermissions,
  AdminRateLimitGuard,
  AdminRole,
  AdminRoles
} from "../admin-security";
import { AdminAuditService } from "./admin-audit.service";
import { AdminAuditDecision } from "./entities/admin-audit-event.entity";

/**
 * Read-only Admin API для просмотра общего audit trail.
 *
 * Endpoint нужен для расследований production-инцидентов: можно увидеть кто,
 * когда и с каким результатом обращался к `/admin/*` API. Сам просмотр audit
 * также проходит через общий admin security layer и будет записан middleware в
 * `admin_audit_events`, чтобы чтение чувствительных operational-логов тоже было
 * отслеживаемым.
 */
@Controller("admin/audit-events")
@UseGuards(AdminApiKeyGuard, AdminRateLimitGuard)
export class AdminAuditController {
  constructor(private readonly adminAudit: AdminAuditService) {}

  /**
   * Возвращает страницу audit events от новых к старым.
   */
  @Get()
  @AdminRoles(AdminRole.Viewer, AdminRole.Operator)
  @AdminPermissions(AdminPermission.Read)
  findMany(
    @Query("actor") actor: string | undefined,
    @Query("role") role: string | undefined,
    @Query("method") method: string | undefined,
    @Query("path") path: string | undefined,
    @Query("action") action: string | undefined,
    @Query("entityType") entityType: string | undefined,
    @Query("entityId") entityId: string | undefined,
    @Query("decision") decision: string | undefined,
    @Query("limit", new DefaultValuePipe(50), ParseIntPipe) limit: number,
    @Query("offset", new DefaultValuePipe(0), ParseIntPipe) offset: number
  ) {
    assertPage(limit, offset);

    return this.adminAudit.findPage({
      actor: normalizeOptional(actor),
      role: normalizeOptional(role),
      method: normalizeMethod(method),
      path: normalizeOptional(path),
      action: normalizeOptional(action),
      entityType: normalizeOptional(entityType),
      entityId: normalizeOptional(entityId),
      decision: parseDecision(decision),
      limit,
      offset
    });
  }

  @Get(":id")
  @AdminRoles(AdminRole.Viewer, AdminRole.Operator)
  @AdminPermissions(AdminPermission.Read)
  findOne(@Param("id", new ParseUUIDPipe()) id: string) {
    return this.adminAudit.findOne(id);
  }
}

function assertPage(limit: number, offset: number): void {
  if (limit < 1 || limit > 200) {
    throw new BadRequestException("limit must be between 1 and 200");
  }

  if (offset < 0) {
    throw new BadRequestException(
      "offset must be greater than or equal to 0"
    );
  }
}

function normalizeOptional(value: string | undefined): string | undefined {
  return value === undefined || value.trim() === "" ? undefined : value;
}

function normalizeMethod(value: string | undefined): string | undefined {
  const normalized = normalizeOptional(value);
  return normalized === undefined ? undefined : normalized.toUpperCase();
}

function parseDecision(
  value: string | undefined
): AdminAuditDecision | undefined {
  const normalized = normalizeOptional(value);

  if (normalized === undefined) {
    return undefined;
  }

  if (
    !Object.values(AdminAuditDecision).includes(
      normalized as AdminAuditDecision
    )
  ) {
    throw new BadRequestException(
      `decision must be one of: ${Object.values(AdminAuditDecision).join(", ")}`
    );
  }

  return normalized as AdminAuditDecision;
}
