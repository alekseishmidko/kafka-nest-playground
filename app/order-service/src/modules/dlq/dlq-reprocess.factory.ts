import {
  EVENT_TOPIC_MAP,
  type DomainEvent,
  type DomainEventType,
  type KafkaTopicName
} from "@kafka-playground/contracts";
import { UnprocessableEntityException } from "@nestjs/common";
import { randomUUID } from "node:crypto";

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Поля payload, которые оператор хочет заменить перед повторной публикацией.
 */
export type CorrectedPayload = Record<string, unknown>;

export interface ReprocessedEventResult {
  event: DomainEvent;
  topic: KafkaTopicName;
  messageKey: string;
}

/**
 * Создаёт новую, валидированную копию исходного события.
 *
 * Старый event не изменяется. Новый `eventId` нужен, чтобы downstream
 * идемпотентность не сочла reprocess дублем уже обработанной попытки.
 * `causationId` указывает на исходный event и сохраняет аудит происхождения.
 */
export function createReprocessedEvent(
  originalEvent: DomainEvent,
  correctedPayload: CorrectedPayload,
  expectedTopic: KafkaTopicName,
  now = new Date()
): ReprocessedEventResult {
  assertOnlyAllowedPayloadFields(
    originalEvent.eventType,
    correctedPayload
  );
  const payload = {
    ...asRecord(originalEvent.payload, "original payload"),
    ...correctedPayload
  };
  const event = {
    ...originalEvent,
    eventId: randomUUID(),
    occurredAt: now.toISOString(),
    causationId: originalEvent.eventId,
    producer: "order-service-dlq-reprocessor",
    payload
  } as unknown as DomainEvent;

  validateDomainEvent(event);

  const contractTopic = EVENT_TOPIC_MAP[event.eventType as DomainEventType];

  if (!contractTopic || contractTopic !== expectedTopic) {
    throw new UnprocessableEntityException(
      `Event ${event.eventType} must be published to ${contractTopic ?? "an unknown topic"}, not ${expectedTopic}`
    );
  }

  return {
    event,
    topic: expectedTopic,
    messageKey: getMessageKey(event)
  };
}

const ALLOWED_CORRECTED_PAYLOAD_FIELDS: Record<
  DomainEventType,
  readonly string[]
> = {
  OrderCreated: [
    "orderId",
    "userId",
    "currency",
    "totalAmount",
    "itemCount"
  ],
  OrderCancellationRequested: [
    "orderId",
    "userId",
    "reason",
    "requestedBy",
    "requestedAt"
  ],
  OrderCancellationRejected: [
    "orderId",
    "userId",
    "reason",
    "requestedBy",
    "currentStatus",
    "rejectedReason",
    "rejectedAt"
  ],
  OrderConfirmed: [
    "orderId",
    "userId",
    "currency",
    "totalAmount",
    "paymentId",
    "confirmedAt"
  ],
  OrderCancelled: [
    "orderId",
    "userId",
    "reason",
    "cancelledBy",
    "cancelledAt"
  ],
  OrderRiskApproved: [
    "orderId",
    "amount",
    "currency",
    "riskScore",
    "approvedBy"
  ],
  OrderRiskRejected: [
    "orderId",
    "riskScore",
    "reason",
    "rejectedBy"
  ],
  PaymentAuthorized: [
    "paymentId",
    "orderId",
    "amount",
    "currency",
    "provider"
  ],
  PaymentFailed: [
    "paymentId",
    "orderId",
    "reason",
    "provider"
  ],
  NotificationCommand: [
    "notificationId",
    "recipient",
    "channel",
    "template",
    "dataJson"
  ],
  DeadLetterEvent: []
};

/**
 * Запрещает произвольное расширение payload через Admin API.
 */
function assertOnlyAllowedPayloadFields(
  eventType: DomainEventType,
  correctedPayload: CorrectedPayload
): void {
  const allowedFields = ALLOWED_CORRECTED_PAYLOAD_FIELDS[eventType] ?? [];
  const unknownFields = Object.keys(correctedPayload).filter(
    (field) => !allowedFields.includes(field)
  );

  if (unknownFields.length > 0) {
    throw new UnprocessableEntityException(
      `Payload fields are not editable for ${eventType}: ${unknownFields.join(", ")}`
    );
  }
}

/**
 * Проверяет поля, необходимые контракту и текущим consumers.
 *
 * Проверка намеренно выполняется до записи в outbox. Иначе некорректное событие
 * было бы принято Admin API и снова попало бы в DLQ после публикации.
 */
export function validateDomainEvent(event: DomainEvent): void {
  const payload = asRecord(event.payload, "payload");

  requireString(event.correlationId, "correlationId");
  requireString(event.eventType, "eventType");

  switch (event.eventType) {
    case "OrderCreated":
      requireUuid(payload.orderId, "payload.orderId");
      requireString(payload.userId, "payload.userId");
      requireString(payload.currency, "payload.currency");
      requireNumber(payload.totalAmount, "payload.totalAmount");
      requireNumber(payload.itemCount, "payload.itemCount");
      return;
    case "OrderCancellationRequested":
      requireUuid(payload.orderId, "payload.orderId");
      requireString(payload.userId, "payload.userId");
      requireString(payload.reason, "payload.reason");
      requireOneOf(payload.requestedBy, "payload.requestedBy", [
        "user",
        "operator"
      ]);
      requireString(payload.requestedAt, "payload.requestedAt");
      return;
    case "OrderCancellationRejected":
      requireUuid(payload.orderId, "payload.orderId");
      requireString(payload.userId, "payload.userId");
      requireString(payload.reason, "payload.reason");
      requireOneOf(payload.requestedBy, "payload.requestedBy", [
        "user",
        "operator"
      ]);
      requireString(payload.currentStatus, "payload.currentStatus");
      requireString(payload.rejectedReason, "payload.rejectedReason");
      requireString(payload.rejectedAt, "payload.rejectedAt");
      return;
    case "OrderConfirmed":
      requireUuid(payload.orderId, "payload.orderId");
      requireString(payload.userId, "payload.userId");
      requireString(payload.currency, "payload.currency");
      requireNumber(payload.totalAmount, "payload.totalAmount");
      requireString(payload.paymentId, "payload.paymentId");
      requireString(payload.confirmedAt, "payload.confirmedAt");
      return;
    case "OrderCancelled":
      requireUuid(payload.orderId, "payload.orderId");
      requireString(payload.userId, "payload.userId");
      requireString(payload.reason, "payload.reason");
      requireOneOf(payload.cancelledBy, "payload.cancelledBy", [
        "risk",
        "payment",
        "user",
        "operator"
      ]);
      requireString(payload.cancelledAt, "payload.cancelledAt");
      return;
    case "OrderRiskApproved":
      requireUuid(payload.orderId, "payload.orderId");
      requireNumber(payload.amount, "payload.amount");
      requireString(payload.currency, "payload.currency");
      requireNumber(payload.riskScore, "payload.riskScore");
      requireString(payload.approvedBy, "payload.approvedBy");
      return;
    case "OrderRiskRejected":
      requireUuid(payload.orderId, "payload.orderId");
      requireNumber(payload.riskScore, "payload.riskScore");
      requireString(payload.reason, "payload.reason");
      requireString(payload.rejectedBy, "payload.rejectedBy");
      return;
    case "PaymentAuthorized":
      requireString(payload.paymentId, "payload.paymentId");
      requireUuid(payload.orderId, "payload.orderId");
      requireNumber(payload.amount, "payload.amount");
      requireString(payload.currency, "payload.currency");
      requireString(payload.provider, "payload.provider");
      return;
    case "PaymentFailed":
      if (payload.paymentId !== null) {
        requireString(payload.paymentId, "payload.paymentId");
      }
      requireUuid(payload.orderId, "payload.orderId");
      requireString(payload.reason, "payload.reason");
      requireString(payload.provider, "payload.provider");
      return;
    case "NotificationCommand":
      requireString(payload.notificationId, "payload.notificationId");
      requireString(payload.recipient, "payload.recipient");
      requireOneOf(payload.channel, "payload.channel", [
        "email",
        "push",
        "webhook"
      ]);
      requireString(payload.template, "payload.template");
      requireString(payload.dataJson, "payload.dataJson");
      return;
    case "DeadLetterEvent":
      throw new UnprocessableEntityException(
        "DeadLetterEvent cannot be reprocessed as a business event"
      );
  }
}

function getMessageKey(event: DomainEvent): string {
  const payload = asRecord(event.payload, "payload");
  const candidate =
    payload.orderId ??
    payload.notificationId ??
    event.eventId;

  return requireString(candidate, "Kafka message key");
}

function asRecord(
  value: unknown,
  fieldName: string
): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new UnprocessableEntityException(
      `${fieldName} must be an object`
    );
  }

  return value as Record<string, unknown>;
}

function requireString(value: unknown, fieldName: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new UnprocessableEntityException(
      `${fieldName} must be a non-empty string`
    );
  }

  return value;
}

function requireUuid(value: unknown, fieldName: string): string {
  const stringValue = requireString(value, fieldName);

  if (!UUID_PATTERN.test(stringValue)) {
    throw new UnprocessableEntityException(
      `${fieldName} must be a UUID`
    );
  }

  return stringValue;
}

function requireNumber(value: unknown, fieldName: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new UnprocessableEntityException(
      `${fieldName} must be a finite number`
    );
  }

  return value;
}

function requireOneOf(
  value: unknown,
  fieldName: string,
  allowedValues: readonly string[]
): string {
  const stringValue = requireString(value, fieldName);

  if (!allowedValues.includes(stringValue)) {
    throw new UnprocessableEntityException(
      `${fieldName} must be one of: ${allowedValues.join(", ")}`
    );
  }

  return stringValue;
}
