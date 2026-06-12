const test = require("node:test");
const assert = require("node:assert/strict");
const {
  DlqAdminRole,
  DlqApiKeyGuard,
  DlqRateLimitGuard
} = require("../dist/modules/dlq/dlq-auth.js");

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

function createAuthGuard(allowedRoles) {
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
    getAllAndOverride() {
      return allowedRoles;
    }
  };

  return new DlqApiKeyGuard(config, reflector);
}

test("отклоняет запрос без Admin API key", () => {
  const guard = createAuthGuard([DlqAdminRole.Viewer]);
  const { context } = createContext();

  assert.throws(
    () => guard.canActivate(context),
    /x-admin-api-key header is required/
  );
});

test("не разрешает viewer выполнять операторскую команду", () => {
  const guard = createAuthGuard([DlqAdminRole.Operator]);
  const { context } = createContext(VIEWER_KEY);

  assert.throws(
    () => guard.canActivate(context),
    /DLQ_VIEWER cannot perform this operation/
  );
});

test("аутентифицирует operator и не сохраняет исходный ключ", () => {
  const guard = createAuthGuard([DlqAdminRole.Operator]);
  const { context, request } = createContext(OPERATOR_KEY);

  assert.equal(guard.canActivate(context), true);
  assert.equal(request.dlqPrincipal.role, DlqAdminRole.Operator);
  assert.equal(request.dlqPrincipal.operatorId, "dlq-operator");
  assert.notEqual(
    request.dlqPrincipal.apiKeyFingerprint,
    OPERATOR_KEY
  );
});

test("ограничивает один ключ шестьюдесятью запросами в минуту", () => {
  const authGuard = createAuthGuard([DlqAdminRole.Operator]);
  const rateLimitGuard = new DlqRateLimitGuard();
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
