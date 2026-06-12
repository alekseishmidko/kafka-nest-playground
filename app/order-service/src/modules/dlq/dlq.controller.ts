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
import type { CorrectedPayload } from "./dlq-reprocess.factory";
import { DlqService } from "./dlq.service";
import { DeadLetterEventStatus } from "./entities/dead-letter-event.entity";
import {
  CurrentDlqPrincipal,
  DlqAdminRole,
  DlqApiKeyGuard,
  type DlqAdminPrincipal,
  DlqRateLimitGuard,
  DlqRoles
} from "./dlq-auth";

interface ReprocessDeadLetterEventBody {
  payload?: CorrectedPayload;
  version?: number;
  comment?: string;
}

interface IgnoreDeadLetterEventBody {
  reason?: string;
  version?: number;
}

/**
 * Внутренний административный HTTP API для DLQ.
 *
 * Все endpoint-ы защищены API key, RBAC и process-local rate limit. Сетевую
 * изоляцию необходимо сохранить как дополнительный уровень защиты, поскольку
 * API позволяет повторно запускать доменные события.
 */
@Controller("admin/dlq")
@UseGuards(DlqApiKeyGuard, DlqRateLimitGuard)
export class DlqController {
  constructor(private readonly dlqService: DlqService) {}

  /**
   * Возвращает страницу DLQ-записей, начиная с самых новых.
   */
  @Get()
  @DlqRoles(DlqAdminRole.Viewer, DlqAdminRole.Operator)
  findMany(
    @Query("status") status: string | undefined,
    @Query("limit", new DefaultValuePipe(50), ParseIntPipe) limit: number,
    @Query("offset", new DefaultValuePipe(0), ParseIntPipe) offset: number
  ) {
    if (limit < 1 || limit > 200) {
      throw new BadRequestException(
        "limit must be between 1 and 200"
      );
    }

    if (offset < 0) {
      throw new BadRequestException(
        "offset must be greater than or equal to 0"
      );
    }

    return this.dlqService.findPage({
      status: parseStatus(status),
      limit,
      offset
    });
  }

  @Get(":id")
  @DlqRoles(DlqAdminRole.Viewer, DlqAdminRole.Operator)
  findOne(
    @Param("id", new ParseUUIDPipe()) id: string
  ) {
    return this.dlqService.findOne(id);
  }

  /**
   * Проверяет исправленный payload и ставит новую копию event-а в outbox.
   */
  @Post(":id/reprocess")
  @DlqRoles(DlqAdminRole.Operator)
  reprocess(
    @Param("id", new ParseUUIDPipe()) id: string,
    @Body() body: ReprocessDeadLetterEventBody,
    @CurrentDlqPrincipal() principal: DlqAdminPrincipal
  ) {
    return this.dlqService.reprocess(
      id,
      body.payload ?? {},
      {
        expectedVersion: body.version ?? 0,
        operatorId: principal.operatorId,
        comment: body.comment ?? ""
      }
    );
  }

  @Post(":id/ignore")
  @DlqRoles(DlqAdminRole.Operator)
  ignore(
    @Param("id", new ParseUUIDPipe()) id: string,
    @Body() body: IgnoreDeadLetterEventBody,
    @CurrentDlqPrincipal() principal: DlqAdminPrincipal
  ) {
    return this.dlqService.ignore(id, body.reason ?? "", {
      expectedVersion: body.version ?? 0,
      operatorId: principal.operatorId
    });
  }
}

function parseStatus(
  value: string | undefined
): DeadLetterEventStatus | undefined {
  if (value === undefined || value === "") {
    return undefined;
  }

  if (
    !Object.values(DeadLetterEventStatus).includes(
      value as DeadLetterEventStatus
    )
  ) {
    throw new BadRequestException(
      `status must be one of: ${Object.values(DeadLetterEventStatus).join(", ")}`
    );
  }

  return value as DeadLetterEventStatus;
}
