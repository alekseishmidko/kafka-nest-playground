import {
  BadRequestException,
  Body,
  Controller,
  DefaultValuePipe,
  Get,
  Param,
  ParseIntPipe,
  ParseUUIDPipe,
  Post,
  Query,
  UseGuards
} from "@nestjs/common";
import { OutboxEventStatus } from "@kafka-playground/outbox";
import {
  AdminApiKeyGuard,
  AdminPermission,
  AdminPermissions,
  AdminRateLimitGuard,
  AdminRole,
  AdminRoles,
  CurrentAdminPrincipal,
  type AdminPrincipal
} from "../admin-security";
import { OutboxAdminService } from "./outbox-admin.service";

interface IgnoreOutboxEventBody {
  reason?: string;
}

/**
 * Внутренний Admin API для расследования и ручного управления outbox.
 *
 * Endpoint-ы используют уже существующие admin auth/rate-limit guards, а общий
 * `AdminAuditMiddleware` пишет audit trail для каждого `/admin/outbox/*`
 * запроса: кто смотрел stuck events, кто запускал retry и кто исключил запись.
 */
@Controller("admin/outbox")
@UseGuards(AdminApiKeyGuard, AdminRateLimitGuard)
export class OutboxAdminController {
  constructor(private readonly outboxAdmin: OutboxAdminService) {}

  /**
   * Возвращает страницу outbox-событий для диагностики stuck/backlog проблем.
   */
  @Get()
  @AdminRoles(AdminRole.Viewer, AdminRole.Operator)
  @AdminPermissions(AdminPermission.Read)
  findMany(
    @Query("status") status: string | undefined,
    @Query("limit", new DefaultValuePipe(50), ParseIntPipe) limit: number,
    @Query("offset", new DefaultValuePipe(0), ParseIntPipe) offset: number
  ) {
    assertPage(limit, offset);

    return this.outboxAdmin.findPage({
      status: parseOutboxStatus(status),
      limit,
      offset
    });
  }

  @Get(":id")
  @AdminRoles(AdminRole.Viewer, AdminRole.Operator)
  @AdminPermissions(AdminPermission.Read)
  findOne(@Param("id", new ParseUUIDPipe()) id: string) {
    return this.outboxAdmin.findOne(id);
  }

  /**
   * Снимает retry backoff с одной FAILED-записи.
   */
  @Post(":id/retry")
  @AdminRoles(AdminRole.Operator)
  @AdminPermissions(AdminPermission.Dangerous)
  retryOne(@Param("id", new ParseUUIDPipe()) id: string) {
    return this.outboxAdmin.retryOne(id);
  }

  /**
   * Снимает retry backoff с пачки FAILED-записей.
   */
  @Post("retry-failed")
  @AdminRoles(AdminRole.Operator)
  @AdminPermissions(AdminPermission.Dangerous)
  retryFailed(
    @Query("limit", new DefaultValuePipe(100), ParseIntPipe) limit: number
  ) {
    if (limit < 1 || limit > 500) {
      throw new BadRequestException(
        "limit must be between 1 and 500"
      );
    }

    return this.outboxAdmin.retryFailed(limit);
  }

  /**
   * Исключает запись из публикации после ручного расследования.
   */
  @Post(":id/ignore")
  @AdminRoles(AdminRole.Operator)
  @AdminPermissions(AdminPermission.Write)
  ignore(
    @Param("id", new ParseUUIDPipe()) id: string,
    @Body() body: IgnoreOutboxEventBody,
    @CurrentAdminPrincipal() principal: AdminPrincipal
  ) {
    return this.outboxAdmin.ignore(id, {
      operatorId: principal.operatorId,
      reason: body.reason ?? ""
    });
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

function parseOutboxStatus(
  value: string | undefined
): OutboxEventStatus | undefined {
  if (value === undefined || value === "") {
    return undefined;
  }

  if (
    !Object.values(OutboxEventStatus).includes(
      value as OutboxEventStatus
    )
  ) {
    throw new BadRequestException(
      `status must be one of: ${Object.values(OutboxEventStatus).join(", ")}`
    );
  }

  return value as OutboxEventStatus;
}
