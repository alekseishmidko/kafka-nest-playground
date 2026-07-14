const test = require("node:test");
const assert = require("node:assert/strict");
const {
  AdminPermission,
  AdminRole,
  AdminApiKeyGuard,
  AdminRateLimitGuard
} = require("../dist/modules/admin-security/index.js");

const VIEWER_KEY = "viewer-secret-for-unit-test";
const OPERATOR_KEY = "operator-secret-for-unit-test";

/**
 * Создаёт минимальный Nest ExecutionContext для изолированного тестирования
 * guard-ов без HTTP adapter и поднятия всего приложения.
 */
function createContext(apiKey) {
  const request = {
    headers: apiKey ? { "x-admin-api-key": apiKey } : {}
  };

  return {
    request,
    context: {
      switchToHttp() {
        return {
          getRequest() {
            return request;
          }
        };
      },
      getHandler() {
        return function handler() {};
      },
      getClass() {
        return class Controller {};
      }
    }
  };
}

function createAuthGuard(allowedRoles, requiredPermissions = []) {
  const config = {
    getOrThrow(name) {
      if (name === "DLQ_ADMIN_OPERATOR_API_KEY") {
        return OPERATOR_KEY;
      }

      if (name === "DLQ_ADMIN_VIEWER_API_KEY") {
        return VIEWER_KEY;
      }

      throw new Error(`Unknown config key: ${name}`);
    }
  };
  const reflector = {
    getAllAndOverride(_key) {
      if (_key === "admin_permissions") {
        return requiredPermissions;
      }

      return allowedRoles;
    }
  };

  return new AdminApiKeyGuard(config, reflector);
}

test("отклоняет запрос без Admin API key", () => {
  const guard = createAuthGuard([AdminRole.Viewer]);
  const { context } = createContext();

  assert.throws(
    () => guard.canActivate(context),
    /x-admin-api-key header is required/
  );
});

test("не разрешает viewer выполнять операторскую команду", () => {
  const guard = createAuthGuard([AdminRole.Operator]);
  const { context } = createContext(VIEWER_KEY);

  assert.throws(
    () => guard.canActivate(context),
    /ADMIN_VIEWER cannot perform this operation/
  );
});

test("разрешает viewer читать admin endpoints", () => {
  const guard = createAuthGuard(
    [AdminRole.Viewer, AdminRole.Operator],
    [AdminPermission.Read]
  );
  const { context, request } = createContext(VIEWER_KEY);

  assert.equal(guard.canActivate(context), true);
  assert.deepEqual(request.adminPrincipal.permissions, [
    AdminPermission.Read
  ]);
});

test("не разрешает viewer выполнять dangerous admin action", () => {
  const guard = createAuthGuard(
    [AdminRole.Viewer, AdminRole.Operator],
    [AdminPermission.Dangerous]
  );
  const { context } = createContext(VIEWER_KEY);

  assert.throws(
    () => guard.canActivate(context),
    /admin:dangerous/
  );
});

test("аутентифицирует operator и не сохраняет исходный ключ", () => {
  const guard = createAuthGuard([AdminRole.Operator]);
  const { context, request } = createContext(OPERATOR_KEY);

  assert.equal(guard.canActivate(context), true);
  assert.equal(request.adminPrincipal.role, AdminRole.Operator);
  assert.equal(request.adminPrincipal.operatorId, "dlq-operator");
  assert.deepEqual(request.adminPrincipal.permissions, [
    AdminPermission.Read,
    AdminPermission.Write,
    AdminPermission.Dangerous
  ]);
  assert.equal(request.dlqPrincipal, request.adminPrincipal);
  assert.notEqual(
    request.adminPrincipal.apiKeyFingerprint,
    OPERATOR_KEY
  );
});

test("ограничивает один ключ шестьюдесятью запросами в минуту", () => {
  const authGuard = createAuthGuard([AdminRole.Operator]);
  const rateLimitGuard = new AdminRateLimitGuard();
  const { context } = createContext(OPERATOR_KEY);

  authGuard.canActivate(context);

  for (let requestNumber = 1; requestNumber <= 60; requestNumber += 1) {
    assert.equal(rateLimitGuard.canActivate(context), true);
  }

  assert.throws(
    () => rateLimitGuard.canActivate(context),
    /rate limit exceeded/
  );
});
