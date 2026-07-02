import type {
  KafkaDomainEvent,
  KafkaTopicName
} from "@kafka-playground/kafka";
import type { TraceCarrier } from "@kafka-playground/observability";
import {
  OutboxEventEntity,
  OutboxEventStatus
} from "./outbox-event.entity";

/**
 * Параметры создания persisted outbox entity.
 */
export interface CreateOutboxEventEntityParams<
  TEvent extends KafkaDomainEvent = KafkaDomainEvent
> {
  /** Topic, куда outbox publisher должен доставить событие. */
  topic: KafkaTopicName;
  /** Message key для сохранения ordering внутри partition. */
  messageKey: string;
  /** Полный domain event envelope, уже зафиксированный доменной логикой. */
  event: TEvent;
  /** Технический trace context исходной операции. */
  traceContext?: TraceCarrier | null;
}

/**
 * Создаёт `OutboxEventEntity` из минимального доменного API.
 *
 * Фабрика намеренно скрывает технические поля outbox-таблицы от прикладного
 * кода: `eventType`, `eventId` и `PENDING` выводятся из event envelope. Так
 * сервисы не дублируют wiring outbox-схемы и не могут случайно поставить
 * несогласованные значения, например `event.eventType = OrderCreated`, а
 * `eventType = PaymentFailed`.
 *
 * Функция не сохраняет entity в БД. Вызывающий код обязан выполнить
 * `manager.save(...)` внутри той же транзакции, где меняются бизнес-данные.
 */
export function createOutboxEventEntity<
  TEvent extends KafkaDomainEvent = KafkaDomainEvent
>(
  params: CreateOutboxEventEntityParams<TEvent>
): OutboxEventEntity<TEvent> {
  assertNonEmptyString(params.topic, "topic");
  assertNonEmptyString(params.messageKey, "messageKey");
  assertValidEventEnvelope(params.event);

  const entity = new OutboxEventEntity<TEvent>();

  entity.topic = params.topic;
  entity.messageKey = params.messageKey;
  entity.eventType = params.event.eventType;
  entity.eventId = params.event.eventId;
  entity.event = params.event;
  entity.traceContext = params.traceContext ?? null;
  entity.status = OutboxEventStatus.Pending;
  entity.attempts = 0;
  entity.nextAttemptAt = null;
  entity.lockedBy = null;
  entity.lockedUntil = null;
  entity.publishedAt = null;
  entity.lastError = null;

  return entity;
}

/**
 * Проверяет минимальный event envelope, необходимый outbox publisher-у.
 */
function assertValidEventEnvelope(event: KafkaDomainEvent): void {
  if (!event || typeof event !== "object") {
    throw new Error("Outbox event must be an object");
  }

  assertNonEmptyString(event.eventId, "event.eventId");
  assertNonEmptyString(event.eventType, "event.eventType");
  assertNonEmptyString(event.occurredAt, "event.occurredAt");
  assertNonEmptyString(event.correlationId, "event.correlationId");
  assertNonEmptyString(event.producer, "event.producer");

  if (!Number.isInteger(event.eventVersion) || event.eventVersion < 1) {
    throw new Error("event.eventVersion must be a positive integer");
  }

  if (!("payload" in event)) {
    throw new Error("event.payload is required");
  }
}

function assertNonEmptyString(value: unknown, fieldName: string): asserts value is string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`${fieldName} must be a non-empty string`);
  }
}
