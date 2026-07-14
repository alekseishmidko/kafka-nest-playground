import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
  UnauthorizedException
} from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { Reflector } from "@nestjs/core";
import { createHash, timingSafeEqual } from "node:crypto";
import {
  ADMIN_API_KEY_HEADER,
  ADMIN_PERMISSIONS_METADATA,
  ADMIN_ROLES_METADATA,
  AdminPermission,
  AdminRole,
  type AdminAuthenticatedRequest,
  type AdminPrincipal
} from "./admin-security.types";

/**
 * Проверяет admin API key и назначает principal.
 *
 * Сейчас guard сохраняет совместимость с историческими переменными
 * `DLQ_ADMIN_OPERATOR_API_KEY` и `DLQ_ADMIN_VIEWER_API_KEY`. Они уже
 * используются локальной инфраструктурой и e2e. Позже их можно переименовать в
 * `ADMIN_OPERATOR_API_KEY`/`ADMIN_VIEWER_API_KEY` без изменения controllers.
 */
@Injectable()
export class AdminApiKeyGuard implements CanActivate {
  constructor(
    private readonly config: ConfigService,
    private readonly reflector: Reflector
  ) {}

  canActivate(context: ExecutionContext): boolean {
    const request = context
      .switchToHttp()
      .getRequest<AdminAuthenticatedRequest>();
    const rawHeader = request.headers[ADMIN_API_KEY_HEADER];
    const apiKey = Array.isArray(rawHeader) ? rawHeader[0] : rawHeader;

    if (!apiKey) {
      throw new UnauthorizedException(
        `${ADMIN_API_KEY_HEADER} header is required`
      );
    }

    const principal = this.resolvePrincipal(apiKey);
    const allowedRoles =
      this.reflector.getAllAndOverride<AdminRole[]>(
        ADMIN_ROLES_METADATA,
        [context.getHandler(), context.getClass()]
      ) ?? [AdminRole.Operator];

    if (!allowedRoles.includes(principal.role)) {
      throw new ForbiddenException(
        `Role ${principal.role} cannot perform this operation`
      );
    }

    const requiredPermissions =
      this.reflector.getAllAndOverride<AdminPermission[]>(
        ADMIN_PERMISSIONS_METADATA,
        [context.getHandler(), context.getClass()]
      ) ?? [];

    assertPermissions(principal, requiredPermissions);

    request.adminPrincipal = principal;
    request.dlqPrincipal = principal;
    return true;
  }

  private resolvePrincipal(apiKey: string): AdminPrincipal {
    const operatorKey = this.config.getOrThrow<string>(
      "DLQ_ADMIN_OPERATOR_API_KEY"
    );
    const viewerKey = this.config.getOrThrow<string>(
      "DLQ_ADMIN_VIEWER_API_KEY"
    );

    if (safeEqual(apiKey, operatorKey)) {
      return {
        operatorId: "dlq-operator",
        role: AdminRole.Operator,
        permissions: [
          AdminPermission.Read,
          AdminPermission.Write,
          AdminPermission.Dangerous
        ],
        apiKeyFingerprint: fingerprint(apiKey)
      };
    }

    if (safeEqual(apiKey, viewerKey)) {
      return {
        operatorId: "dlq-viewer",
        role: AdminRole.Viewer,
        permissions: [AdminPermission.Read],
        apiKeyFingerprint: fingerprint(apiKey)
      };
    }

    throw new UnauthorizedException("Invalid Admin API key");
  }
}

function assertPermissions(
  principal: AdminPrincipal,
  requiredPermissions: AdminPermission[]
): void {
  const missing = requiredPermissions.filter(
    (permission) => !principal.permissions.includes(permission)
  );

  if (missing.length > 0) {
    throw new ForbiddenException(
      `Principal ${principal.operatorId} lacks admin permissions: ${missing.join(", ")}`
    );
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
