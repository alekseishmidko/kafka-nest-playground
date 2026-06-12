import {
  CanActivate,
  createParamDecorator,
  ExecutionContext,
  ForbiddenException,
  HttpException,
  HttpStatus,
  Injectable,
  SetMetadata,
  UnauthorizedException
} from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { Reflector } from "@nestjs/core";
import { createHash, timingSafeEqual } from "node:crypto";

const DLQ_ROLES_METADATA = "dlq_roles";
const API_KEY_HEADER = "x-admin-api-key";

export enum DlqAdminRole {
  Viewer = "DLQ_VIEWER",
  Operator = "DLQ_OPERATOR"
}

export interface DlqAdminPrincipal {
  operatorId: string;
  role: DlqAdminRole;
  apiKeyFingerprint: string;
}

/**
 * Минимальная форма HTTP request, необходимая guards.
 *
 * Локальный тип не связывает доменный модуль с Express: Nest-приложение
 * сможет перейти на другой HTTP adapter без изменения логики авторизации.
 */
interface AuthenticatedRequest {
  headers: Record<string, string | string[] | undefined>;
  dlqPrincipal?: DlqAdminPrincipal;
}

/**
 * Ограничивает endpoint одной или несколькими DLQ-ролями.
 */
export const DlqRoles = (...roles: DlqAdminRole[]) =>
  SetMetadata(DLQ_ROLES_METADATA, roles);

/**
 * Извлекает проверенного principal-а из HTTP request.
 */
export const CurrentDlqPrincipal = createParamDecorator(
  (_data: unknown, context: ExecutionContext): DlqAdminPrincipal => {
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();

    if (!request.dlqPrincipal) {
      throw new UnauthorizedException("DLQ principal is missing");
    }

    return request.dlqPrincipal;
  }
);

/**
 * Проверяет API key и назначает минимально необходимую роль.
 *
 * Сравнение выполняется через `timingSafeEqual`, чтобы не раскрывать ключ
 * измерением времени посимвольного сравнения.
 */
@Injectable()
export class DlqApiKeyGuard implements CanActivate {
  constructor(
    private readonly config: ConfigService,
    private readonly reflector: Reflector
  ) {}

  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    const rawHeader = request.headers[API_KEY_HEADER];
    const apiKey = Array.isArray(rawHeader) ? rawHeader[0] : rawHeader;

    if (!apiKey) {
      throw new UnauthorizedException(
        `${API_KEY_HEADER} header is required`
      );
    }

    const principal = this.resolvePrincipal(apiKey);
    const allowedRoles =
      this.reflector.getAllAndOverride<DlqAdminRole[]>(
        DLQ_ROLES_METADATA,
        [context.getHandler(), context.getClass()]
      ) ?? [DlqAdminRole.Operator];

    if (!allowedRoles.includes(principal.role)) {
      throw new ForbiddenException(
        `Role ${principal.role} cannot perform this operation`
      );
    }

    request.dlqPrincipal = principal;
    return true;
  }

  private resolvePrincipal(apiKey: string): DlqAdminPrincipal {
    const operatorKey = this.config.getOrThrow<string>(
      "DLQ_ADMIN_OPERATOR_API_KEY"
    );
    const viewerKey = this.config.getOrThrow<string>(
      "DLQ_ADMIN_VIEWER_API_KEY"
    );

    if (safeEqual(apiKey, operatorKey)) {
      return {
        operatorId: "dlq-operator",
        role: DlqAdminRole.Operator,
        apiKeyFingerprint: fingerprint(apiKey)
      };
    }

    if (safeEqual(apiKey, viewerKey)) {
      return {
        operatorId: "dlq-viewer",
        role: DlqAdminRole.Viewer,
        apiKeyFingerprint: fingerprint(apiKey)
      };
    }

    throw new UnauthorizedException("Invalid DLQ Admin API key");
  }
}

interface RateBucket {
  windowStartedAt: number;
  requests: number;
}

/**
 * Простой process-local fixed-window rate limiter Admin API.
 *
 * Для нескольких replicas состояние следует перенести в Redis. В текущем
 * single-instance проекте limiter ограничивает ошибочные скрипты и ручные
 * циклы без добавления внешней зависимости.
 */
@Injectable()
export class DlqRateLimitGuard implements CanActivate {
  private readonly buckets = new Map<string, RateBucket>();
  private readonly windowMs = 60_000;
  private readonly maxRequests = 60;

  canActivate(context: ExecutionContext): boolean {
    const request = context
      .switchToHttp()
      .getRequest<AuthenticatedRequest>();
    const principal = request.dlqPrincipal;

    if (!principal) {
      throw new UnauthorizedException("DLQ principal is missing");
    }

    const now = Date.now();
    const bucket = this.buckets.get(principal.apiKeyFingerprint);

    if (!bucket || now - bucket.windowStartedAt >= this.windowMs) {
      this.buckets.set(principal.apiKeyFingerprint, {
        windowStartedAt: now,
        requests: 1
      });
      return true;
    }

    bucket.requests += 1;

    if (bucket.requests > this.maxRequests) {
      throw new HttpException(
        "DLQ Admin API rate limit exceeded",
        HttpStatus.TOO_MANY_REQUESTS
      );
    }

    return true;
  }
}

function safeEqual(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);

  return (
    leftBuffer.length === rightBuffer.length &&
    timingSafeEqual(leftBuffer, rightBuffer)
  );
}

function fingerprint(apiKey: string): string {
  return createHash("sha256").update(apiKey).digest("hex").slice(0, 16);
}
