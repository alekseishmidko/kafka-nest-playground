import {
  CanActivate,
  ExecutionContext,
  HttpException,
  HttpStatus,
  Injectable,
  UnauthorizedException
} from "@nestjs/common";
import type { AdminAuthenticatedRequest } from "./admin-security.types";

interface RateBucket {
  windowStartedAt: number;
  requests: number;
}

/**
 * Process-local fixed-window rate limiter для Admin API.
 *
 * В single-instance local/dev режиме этого достаточно, чтобы ограничить
 * ошибочные скрипты и ручные циклы. Для нескольких replicas storage нужно
 * вынести в Redis или API gateway, иначе каждая replica будет считать лимит
 * отдельно.
 */
@Injectable()
export class AdminRateLimitGuard implements CanActivate {
  private readonly buckets = new Map<string, RateBucket>();
  private readonly windowMs = 60_000;
  private readonly maxRequests = 60;

  canActivate(context: ExecutionContext): boolean {
    const request = context
      .switchToHttp()
      .getRequest<AdminAuthenticatedRequest>();
    const principal = request.adminPrincipal;

    if (!principal) {
      throw new UnauthorizedException("Admin principal is missing");
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
        "Admin API rate limit exceeded",
        HttpStatus.TOO_MANY_REQUESTS
      );
    }

    return true;
  }
}
