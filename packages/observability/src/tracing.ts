import {
  context,
  createTraceState,
  propagation,
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  trace,
  type Attributes,
  type Context
} from "@opentelemetry/api";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { NodeSDK } from "@opentelemetry/sdk-node";

const TRACER_NAME = "@kafka-playground/observability";

export const TRACE_HEADER_NAMES = {
  traceParent: "traceparent",
  traceState: "tracestate",
  traceId: "x-trace-id",
  spanId: "x-span-id"
} as const;

export type TraceCarrier = Record<string, string>;

export interface TraceSpanOptions {
  kind?: SpanKind;
  attributes?: Attributes;
  parentContext?: Context;
}

let tracingSdk: NodeSDK | null = null;

/**
 * Инициализирует OpenTelemetry до загрузки NestJS, gRPC, HTTP и PostgreSQL.
 *
 * Функция идемпотентна: повторный вызов из теста или bootstrap-файла не
 * регистрирует второй provider. Экспорт выполняется по OTLP/HTTP в Collector.
 */
export function initializeTracing(options: {
  serviceName: string;
  environment?: string;
  enabled?: boolean;
  otlpEndpoint?: string;
}): void {
  if (tracingSdk || options.enabled === false) {
    return;
  }

  const endpoint =
    options.otlpEndpoint ??
    process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT ??
    "http://localhost:4318/v1/traces";

  tracingSdk = new NodeSDK({
    serviceName: options.serviceName,
    traceExporter: new OTLPTraceExporter({ url: endpoint }),
    instrumentations: [
      getNodeAutoInstrumentations({
        // Файловые spans создают много шума и не описывают бизнес-путь заказа.
        "@opentelemetry/instrumentation-fs": {
          enabled: false
        },
        // Kafka tracing реализован в общем packages/kafka: там доступны
        // domain eventId, retry stage и DLQ semantics. Автоинструментация
        // создала бы дублирующие producer/consumer spans.
        "@opentelemetry/instrumentation-kafkajs": {
          enabled: false
        },
        "@opentelemetry/instrumentation-http": {
          ignoreIncomingRequestHook: (request) =>
            isTechnicalEndpoint(request.url)
        }
      })
    ]
  });
  tracingSdk.start();
}

/**
 * Определяет технические HTTP endpoints, которые не описывают пользовательский
 * бизнес-сценарий и не должны создавать traces или access-логи.
 */
export function isTechnicalEndpoint(
  url: string | undefined
): boolean {
  const pathname = (url ?? "").split("?", 1)[0];

  return (
    pathname === "/metrics" ||
    pathname === "/health" ||
    pathname.startsWith("/health/")
  );
}

/**
 * Отправляет накопленные spans перед остановкой процесса.
 */
export async function shutdownTracing(): Promise<void> {
  const sdk = tracingSdk;

  if (!sdk) {
    return;
  }

  tracingSdk = null;
  await sdk.shutdown();
}

/**
 * Выполняет асинхронную операцию внутри span и единообразно фиксирует ошибку.
 */
export async function runInTraceSpan<T>(
  name: string,
  options: TraceSpanOptions,
  operation: () => Promise<T>
): Promise<T> {
  const tracer = trace.getTracer(TRACER_NAME);
  const parentContext = options.parentContext ?? context.active();

  return tracer.startActiveSpan(
    name,
    {
      kind: options.kind ?? SpanKind.INTERNAL,
      attributes: options.attributes
    },
    parentContext,
    async (span) => {
      try {
        return await operation();
      } catch (error) {
        span.recordException(normalizeError(error));
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: error instanceof Error ? error.message : String(error)
        });
        throw error;
      } finally {
        span.end();
      }
    }
  );
}

/**
 * Отмечает текущий span ошибочным, когда ошибка будет обработана и не выйдет
 * наружу, например после успешной отправки сообщения в retry topic.
 */
export function markActiveSpanAsFailed(error: unknown): void {
  const span = trace.getActiveSpan();

  if (!span) {
    return;
  }

  span.recordException(normalizeError(error));
  span.setStatus({
    code: SpanStatusCode.ERROR,
    message: error instanceof Error ? error.message : String(error)
  });
}

/**
 * Сериализует активный W3C trace context для долговременного хранения.
 *
 * Outbox publisher запускается после завершения исходного HTTP/gRPC запроса,
 * поэтому родительский context необходимо сохранить вместе с событием.
 */
export function captureActiveTraceContext(): TraceCarrier | null {
  const carrier: TraceCarrier = {};

  injectTraceContext(carrier);

  return carrier[TRACE_HEADER_NAMES.traceParent] ? carrier : null;
}

/**
 * Добавляет W3C headers и удобные диагностические `x-trace-id/x-span-id`.
 */
export function injectTraceContext(
  carrier: TraceCarrier,
  sourceContext: Context = context.active()
): TraceCarrier {
  propagation.inject(sourceContext, carrier);
  const spanContext = trace.getSpanContext(sourceContext);

  if (spanContext?.traceId) {
    carrier[TRACE_HEADER_NAMES.traceParent] =
      `00-${spanContext.traceId}-${spanContext.spanId}-${spanContext.traceFlags.toString(16).padStart(2, "0")}`;
    if (spanContext.traceState) {
      carrier[TRACE_HEADER_NAMES.traceState] =
        spanContext.traceState.serialize();
    }
    carrier[TRACE_HEADER_NAMES.traceId] = spanContext.traceId;
    carrier[TRACE_HEADER_NAMES.spanId] = spanContext.spanId;
  }

  return carrier;
}

/**
 * Восстанавливает remote parent из Kafka headers или persisted outbox context.
 */
export function extractTraceContext(
  carrier: Record<string, string | Buffer | undefined> | null | undefined
): Context {
  if (!carrier) {
    return ROOT_CONTEXT;
  }

  const normalized = Object.fromEntries(
    Object.entries(carrier).flatMap(([name, value]) => {
      if (typeof value === "string") {
        return [[name, value]];
      }

      if (Buffer.isBuffer(value)) {
        return [[name, value.toString("utf8")]];
      }

      return [];
    })
  );

  const extracted = propagation.extract(ROOT_CONTEXT, normalized);

  if (trace.getSpanContext(extracted)) {
    return extracted;
  }

  return parseTraceParent(normalized);
}

/**
 * Возвращает идентификаторы активного span для структурированных логов.
 */
export function getActiveTraceLogFields(): {
  traceId?: string;
  spanId?: string;
} {
  const spanContext = trace.getActiveSpan()?.spanContext();

  if (!spanContext) {
    return {};
  }

  return {
    traceId: spanContext.traceId,
    spanId: spanContext.spanId
  };
}

function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/**
 * Минимальный строгий parser W3C `traceparent` для режима без SDK propagator.
 */
function parseTraceParent(carrier: TraceCarrier): Context {
  const value = carrier[TRACE_HEADER_NAMES.traceParent];
  const match =
    /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/i.exec(
      value ?? ""
    );

  if (!match || /^0+$/.test(match[1]) || /^0+$/.test(match[2])) {
    return ROOT_CONTEXT;
  }

  return trace.setSpanContext(ROOT_CONTEXT, {
    traceId: match[1].toLowerCase(),
    spanId: match[2].toLowerCase(),
    traceFlags: Number.parseInt(match[3], 16),
    isRemote: true,
    traceState: parseTraceState(
      carrier[TRACE_HEADER_NAMES.traceState]
    )
  });
}

function parseTraceState(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  try {
    return createTraceState(value);
  } catch {
    // Повреждённый tracestate не должен ломать обработку бизнес-сообщения.
    return undefined;
  }
}

export { SpanKind };
