import { Global, Module } from "@nestjs/common";
import { AdminApiKeyGuard } from "./admin-api-key.guard";
import { AdminRateLimitGuard } from "./admin-rate-limit.guard";
import {
  ADMIN_RATE_LIMIT_STORE,
  createAdminRateLimitStoreFromEnv
} from "./admin-rate-limit.store";

/**
 * Общий security-модуль для всех `/admin/*` endpoints.
 *
 * Модуль отделяет auth/RBAC/rate-limit от DLQ. Новые admin controllers должны
 * импортировать guards/decorators отсюда, а не из `modules/dlq`.
 */
@Global()
@Module({
  providers: [
    AdminApiKeyGuard,
    AdminRateLimitGuard,
    {
      provide: ADMIN_RATE_LIMIT_STORE,
      useFactory: createAdminRateLimitStoreFromEnv
    }
  ],
  exports: [AdminApiKeyGuard, AdminRateLimitGuard]
})
export class AdminSecurityModule {}
