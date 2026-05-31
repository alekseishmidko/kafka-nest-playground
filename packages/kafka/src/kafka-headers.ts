import type { DomainEvent } from "@kafka-playground/contracts";
import type { KafkaHeaders } from "./types";

export const KAFKA_HEADER_NAMES = {
  correlationId: "x-correlation-id",
  causationId: "x-causation-id",
  eventId: "x-event-id",
  eventType: "x-event-type",
  eventVersion: "x-event-version"
} as const;

export function buildKafkaHeaders(
  event: DomainEvent,
  extraHeaders: KafkaHeaders = {}
): KafkaHeaders {
  return {
    ...extraHeaders,
    [KAFKA_HEADER_NAMES.correlationId]: event.correlationId,
    [KAFKA_HEADER_NAMES.causationId]: event.causationId ?? undefined,
    [KAFKA_HEADER_NAMES.eventId]: event.eventId,
    [KAFKA_HEADER_NAMES.eventType]: event.eventType,
    [KAFKA_HEADER_NAMES.eventVersion]: String(event.eventVersion)
  };
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
