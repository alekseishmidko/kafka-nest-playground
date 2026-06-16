import type { KafkaDomainEvent, KafkaHeaderInput, KafkaHeaders } from "./types";

/**
 * Канонические имена технических Kafka headers.
 *
 * Константа исключает расхождения в написании заголовков между producer,
 * consumer и retry-механизмом.
 */
export const KAFKA_HEADER_NAMES = {
  correlationId: "x-correlation-id",
  causationId: "x-causation-id",
  eventId: "x-event-id",
  eventType: "x-event-type",
  eventVersion: "x-event-version",
  retryCount: "x-retry-count",
  originalTopic: "x-original-topic",
  firstFailedAt: "x-first-failed-at",
  errorCode: "x-error-code",
  traceParent: "traceparent",
  traceState: "tracestate",
  traceId: "x-trace-id",
  spanId: "x-span-id"
} as const;

/**
 * Формирует полный набор headers для доменного события.
 *
 * Переданные `extraHeaders` сохраняются, а обязательные поля envelope имеют
 * приоритет. Благодаря этому retry-сообщение сохраняет служебные заголовки, но
 * не может случайно подменить `eventId`, `eventType` или `correlationId`.
 */
export function buildKafkaHeaders(
  event: KafkaDomainEvent,
  extraHeaders: KafkaHeaderInput = {}
): KafkaHeaders {
  return compactHeaders({
    ...extraHeaders,
    [KAFKA_HEADER_NAMES.correlationId]: event.correlationId,
    [KAFKA_HEADER_NAMES.causationId]: event.causationId ?? undefined,
    [KAFKA_HEADER_NAMES.eventId]: event.eventId,
    [KAFKA_HEADER_NAMES.eventType]: event.eventType,
    [KAFKA_HEADER_NAMES.eventVersion]: String(event.eventVersion)
  });
}

/**
 * Читает строковое значение Kafka header независимо от представления KafkaJS.
 *
 * KafkaJS может вернуть header как `string` или `Buffer`, поэтому преобразование
 * централизовано и не дублируется в consumer-ах.
 */
export function readHeader(headers: KafkaHeaders | undefined, name: string): string | undefined {
  const value = headers?.[name];

  if (typeof value === "string") {
    return value;
  }

  if (Buffer.isBuffer(value)) {
    return value.toString("utf8");
  }

  return undefined;
}

/**
 * Удаляет headers со значением `undefined` перед передачей сообщения KafkaJS.
 */
export function compactHeaders(headers: KafkaHeaderInput): KafkaHeaders {
  return Object.fromEntries(
    Object.entries(headers).filter((entry): entry is [string, string | Buffer] => {
      const value = entry[1];
      return value !== undefined;
    })
  );
}
