import type { DomainEvent } from "@kafka-playground/contracts";

/**
 * Разбирает исходное событие из строкового поля `DeadLetterEvent.payload.rawEvent`.
 *
 * Consumer не отклоняет весь DLQ event, если JSON повреждён: технические данные
 * ошибки всё равно полезно сохранить. Однако reprocess такой записи будет
 * запрещён, пока система не получит корректный исходный envelope.
 */
export function parseOriginalEvent(rawEvent: string | null): DomainEvent | null {
  if (!rawEvent) {
    return null;
  }

  try {
    const value: unknown = JSON.parse(rawEvent);

    return isDomainEventEnvelope(value) ? value : null;
  } catch {
    return null;
  }
}

/**
 * Выполняет минимальную структурную проверку общего event envelope.
 *
 * Полная event-specific валидация выполняется перед reprocess, когда оператор
 * уже передал исправленный payload.
 */
function isDomainEventEnvelope(value: unknown): value is DomainEvent {
  if (!isRecord(value) || !isRecord(value.payload)) {
    return false;
  }

  return (
    typeof value.eventId === "string" &&
    typeof value.eventType === "string" &&
    typeof value.eventVersion === "number" &&
    typeof value.occurredAt === "string" &&
    typeof value.correlationId === "string" &&
    typeof value.producer === "string"
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
