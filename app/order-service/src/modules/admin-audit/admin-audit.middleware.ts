import { Injectable, type NestMiddleware } from "@nestjs/common";
import { randomUUID } from "node:crypto";
import {
  AdminAuditDecision
} from "./entities/admin-audit-event.entity";
import { AdminAuditService } from "./admin-audit.service";

interface AdminPrincipal {
  operatorId?: string;
  role?: string;
}

interface AdminAuditRequest {
  method?: string;
  originalUrl?: string;
  url?: string;
  headers?: Record<string, string | string[] | undefined>;
  ip?: string;
  socket?: {
    remoteAddress?: string;
  };
  dlqPrincipal?: AdminPrincipal;
  adminPrincipal?: AdminPrincipal;
}

interface AdminAuditResponse {
  statusCode?: number;
  once(event: "finish", listener: () => void): void;
}

/**
 * Middleware уровня `/admin/*`, который пишет общий audit trail после ответа.
 *
 * В отличие от interceptor-а, middleware запускается до guards. Поэтому оно
 * видит не только успешные admin actions, но и отклонённые `401/403/429`
 * запросы, что важно для расследования попыток доступа.
 */
@Injectable()
export class AdminAuditMiddleware implements NestMiddleware {
  constructor(private readonly audit: AdminAuditService) {}

  use(
    request: AdminAuditRequest,
    response: AdminAuditResponse,
    next: () => void
  ): void {
    const startedAt = Date.now();
    const requestId = readHeader(request, "x-request-id") ?? randomUUID();
    const method = (request.method ?? "UNKNOWN").toUpperCase();
    const parsedPath = parsePath(request.originalUrl ?? request.url ?? "/");

    if (!isAdminPath(parsedPath.pathname)) {
      next();
      return;
    }

    response.once("finish", () => {
      const statusCode = response.statusCode ?? 0;
      const principal = request.adminPrincipal ?? request.dlqPrincipal ?? null;
      const target = inferAuditTarget(method, parsedPath.pathname);

      void this.audit.record({
        actor: principal?.operatorId ?? null,
        role: principal?.role ?? null,
        method,
        path: parsedPath.pathname,
        action: target.action,
        entityType: target.entityType,
        entityId: target.entityId,
        decision: decisionFromStatus(statusCode),
        statusCode,
        requestId,
        correlationId: readCorrelationId(request),
        ip: readClientIp(request),
        userAgent: readHeader(request, "user-agent"),
        durationMs: Date.now() - startedAt,
        metadata: {
          query: parsedPath.query,
          adminArea: target.adminArea
        }
      });
    });

    next();
  }
}

function isAdminPath(pathname: string): boolean {
  return pathname === "/admin" || pathname.startsWith("/admin/");
}

function parsePath(rawUrl: string): { pathname: string; query: string | null } {
  const url = new URL(rawUrl, "http://order-service.local");
  const query = url.searchParams.toString();

  return {
    pathname: url.pathname,
    query: query === "" ? null : query
  };
}

function inferAuditTarget(
  method: string,
  pathname: string
): {
  adminArea: string | null;
  action: string;
  entityType: string | null;
  entityId: string | null;
} {
  const parts = pathname.split("/").filter(Boolean);
  const adminArea = parts[1] ?? null;
  const entityId = parts[2] ?? null;
  const command = parts[3] ?? null;

  if (adminArea === "dlq") {
    return {
      adminArea,
      action: command ? `dlq.${command}` : method === "GET" ? "dlq.read" : "dlq",
      entityType: entityId ? "dead_letter_event" : "dead_letter_queue",
      entityId
    };
  }

  if (adminArea === "outbox") {
    return {
      adminArea,
      action: command ? `outbox.${command}` : method === "GET" ? "outbox.read" : "outbox",
      entityType: entityId ? "outbox_event" : "outbox",
      entityId
    };
  }

  return {
    adminArea,
    action: [adminArea, command ?? method.toLowerCase()]
      .filter(Boolean)
      .join("."),
    entityType: adminArea,
    entityId
  };
}

function decisionFromStatus(statusCode: number): AdminAuditDecision {
  if (statusCode >= 200 && statusCode < 400) {
    return AdminAuditDecision.Allowed;
  }

  if ([401, 403, 429].includes(statusCode)) {
    return AdminAuditDecision.Denied;
  }

  return AdminAuditDecision.Failed;
}

function readCorrelationId(request: AdminAuditRequest): string | null {
  const explicit = readHeader(request, "x-correlation-id");

  if (explicit) {
    return explicit;
  }

  const traceparent = readHeader(request, "traceparent");
  const traceId = traceparent?.split("-")[1];

  return traceId && traceId.length > 0 ? traceId : null;
}

function readClientIp(request: AdminAuditRequest): string | null {
  return (
    readHeader(request, "x-forwarded-for") ??
    readHeader(request, "x-real-ip") ??
    request.ip ??
    request.socket?.remoteAddress ??
    null
  );
}

function readHeader(
  request: AdminAuditRequest,
  name: string
): string | null {
  const value = request.headers?.[name.toLowerCase()];
  const firstValue = Array.isArray(value) ? value[0] : value;

  return firstValue === undefined || firstValue === "" ? null : firstValue;
}
