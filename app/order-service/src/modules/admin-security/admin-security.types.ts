export const ADMIN_ROLES_METADATA = "admin_roles";
export const ADMIN_PERMISSIONS_METADATA = "admin_permissions";
export const ADMIN_API_KEY_HEADER = "x-admin-api-key";

export enum AdminRole {
  Viewer = "ADMIN_VIEWER",
  Operator = "ADMIN_OPERATOR"
}

export enum AdminPermission {
  Read = "admin:read",
  Write = "admin:write",
  Dangerous = "admin:dangerous"
}

export interface AdminPrincipal {
  operatorId: string;
  role: AdminRole;
  permissions: AdminPermission[];
  apiKeyFingerprint: string;
}

/**
 * Минимальная форма HTTP request, необходимая admin security guards.
 *
 * Тип намеренно не зависит от Express/Fastify. Nest request object всё равно
 * может хранить runtime-поля `adminPrincipal` и `dlqPrincipal`, но этот модуль
 * не должен импортировать конкретный HTTP adapter.
 */
export interface AdminAuthenticatedRequest {
  headers: Record<string, string | string[] | undefined>;
  adminPrincipal?: AdminPrincipal;
  /**
   * Backward-compatible alias для старого DLQ-кода и audit middleware.
   *
   * Новые admin endpoints должны читать `adminPrincipal`.
   */
  dlqPrincipal?: AdminPrincipal;
}
