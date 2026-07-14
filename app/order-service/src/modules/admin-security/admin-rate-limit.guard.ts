import {
  CanActivate,
  ExecutionContext,
  HttpException,
  HttpStatus,
  Inject,
  Injectable,
  Optional,
  UnauthorizedException
} from "@nestjs/common";
import type { AdminAuthenticatedRequest } from "./admin-security.types";
import {
  ADMIN_RATE_LIMIT_STORE,
  createAdminRateLimitStoreFromEnv,
  type AdminRateLimitStore
} from "./admin-rate-limit.store";

/**
 * Fixed-window rate limiter для Admin API.
 *
 * Guard намеренно не знает, где хранится счётчик. В local/dev можно оставить
 * in-memory storage, а в production с несколькими replicas включить Redis
 * backend через `ADMIN_RATE_LIMIT_BACKEND=redis`. Redis делает лимит общим для
 * всех replicas, потому что все процессы инкрементируют один и тот же ключ.
 */
@Injectable()
export class AdminRateLimitGuard implements CanActivate {
  private readonly windowMs = readPositiveInt(
    "ADMIN_RATE_LIMIT_WINDOW_MS",
    60_000
  );
  private readonly maxRequests = readPositiveInt(
    "ADMIN_RATE_LIMIT_MAX_REQUESTS",
    60
  );

  constructor(
    @Optional()
    @Inject(ADMIN_RATE_LIMIT_STORE)
    private readonly store: AdminRateLimitStore = createAdminRateLimitStoreFromEnv()
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context
      .switchToHttp()
      .getRequest<AdminAuthenticatedRequest>();
    const principal = request.adminPrincipal;

    if (!principal) {
      throw new UnauthorizedException("Admin principal is missing");
    }

    const key = `admin:${principal.apiKeyFingerprint}`;
    const rate = await this.increment(key);

    if (rate.requests > this.maxRequests) {
      throw new HttpException(
        "Admin API rate limit exceeded",
        HttpStatus.TOO_MANY_REQUESTS
      );
    }

    return true;
  }

  private async increment(key: string) {
    try {
      return await this.store.increment(key, this.windowMs);
    } catch (error) {
      throw new HttpException(
        "Admin API rate limit storage is unavailable",
        HttpStatus.TOO_MANY_REQUESTS,
        {
          cause: error
        }
      );
    }
  }
}

function readPositiveInt(name: string, defaultValue: number): number {
  const rawValue = process.env[name];

  if (!rawValue) {
    return defaultValue;
  }

  const value = Number(rawValue);

  return Number.isInteger(value) && value > 0 ? value : defaultValue;
}
