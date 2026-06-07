import type { DomainEvent } from "@kafka-playground/contracts";
import type { KafkaHeaderInput, KafkaHeaders } from "./types";

export const KAFKA_HEADER_NAMES = {
  correlationId: "x-correlation-id",
  causationId: "x-causation-id",
  eventId: "x-event-id",
  eventType: "x-event-type",
  eventVersion: "x-event-version"
} as const;

export function buildKafkaHeaders(
  event: DomainEvent,
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

export function compactHeaders(headers: KafkaHeaderInput): KafkaHeaders {
  return Object.fromEntries(
    Object.entries(headers).filter((entry): entry is [string, string | Buffer] => {
      const value = entry[1];
      return value !== undefined;
    })
  );
}
