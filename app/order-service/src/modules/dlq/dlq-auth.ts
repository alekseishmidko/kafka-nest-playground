/**
 * @deprecated Используйте `../admin-security`.
 *
 * Файл оставлен как compatibility layer для старых imports. Новые admin
 * endpoints не должны зависеть от DLQ-модуля ради auth/RBAC/rate-limit.
 */
export {
  AdminApiKeyGuard as DlqApiKeyGuard,
  AdminRateLimitGuard as DlqRateLimitGuard,
  AdminRole as DlqAdminRole,
  AdminRoles as DlqRoles,
  CurrentAdminPrincipal as CurrentDlqPrincipal
} from "../admin-security";
export type {
  AdminPrincipal as DlqAdminPrincipal
} from "../admin-security";
