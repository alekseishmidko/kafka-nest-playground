import { Global, Module } from "@nestjs/common";
import { AdminApiKeyGuard } from "./admin-api-key.guard";
import { AdminRateLimitGuard } from "./admin-rate-limit.guard";

/**
 * Общий security-модуль для всех `/admin/*` endpoints.
 *
 * Модуль отделяет auth/RBAC/rate-limit от DLQ. Новые admin controllers должны
 * импортировать guards/decorators отсюда, а не из `modules/dlq`.
 */
@Global()
@Module({
  providers: [AdminApiKeyGuard, AdminRateLimitGuard],
  exports: [AdminApiKeyGuard, AdminRateLimitGuard]
})
export class AdminSecurityModule {}
