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
  Query
} from "@nestjs/common";
import type { CorrectedPayload } from "./dlq-reprocess.factory";
import { DlqService } from "./dlq.service";
import { DeadLetterEventStatus } from "./entities/dead-letter-event.entity";

interface ReprocessDeadLetterEventBody {
  payload?: CorrectedPayload;
}

interface IgnoreDeadLetterEventBody {
  reason?: string;
}

/**
 * Внутренний административный HTTP API для DLQ.
 *
 * Endpoint-ы не предназначены для публичного клиентского трафика. В production
 * их необходимо закрывать authentication/authorization и сетевой политикой.
 */
@Controller("admin/dlq")
export class DlqController {
  constructor(private readonly dlqService: DlqService) {}

  /**
   * Возвращает страницу DLQ-записей, начиная с самых новых.
   */
  @Get()
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
  findOne(
    @Param("id", new ParseUUIDPipe()) id: string
  ) {
    return this.dlqService.findOne(id);
  }

  /**
   * Проверяет исправленный payload и ставит новую копию event-а в outbox.
   */
  @Post(":id/reprocess")
  reprocess(
    @Param("id", new ParseUUIDPipe()) id: string,
    @Body() body: ReprocessDeadLetterEventBody
  ) {
    return this.dlqService.reprocess(id, body.payload ?? {});
  }

  @Post(":id/ignore")
  ignore(
    @Param("id", new ParseUUIDPipe()) id: string,
    @Body() body: IgnoreDeadLetterEventBody
  ) {
    return this.dlqService.ignore(id, body.reason ?? "");
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
