import {
  createParamDecorator,
  ExecutionContext,
  UnauthorizedException
} from "@nestjs/common";
import type {
  AdminAuthenticatedRequest,
  AdminPrincipal
} from "./admin-security.types";

/**
 * Извлекает проверенного admin principal-а из HTTP request.
 */
export const CurrentAdminPrincipal = createParamDecorator(
  (_data: unknown, context: ExecutionContext): AdminPrincipal => {
    const request = context
      .switchToHttp()
      .getRequest<AdminAuthenticatedRequest>();

    if (!request.adminPrincipal) {
      throw new UnauthorizedException("Admin principal is missing");
    }

    return request.adminPrincipal;
  }
);
