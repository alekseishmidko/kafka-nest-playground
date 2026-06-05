import { KAFKA_TOPICS, type KafkaTopicName } from "./topics";

export interface EventEnvelope<TPayload, TEventType extends string = string> {
  eventId: string;
  eventType: TEventType;
  eventVersion: number;
  occurredAt: string;
  correlationId: string;
  causationId: string | null;
  producer: string;
  payload: TPayload;
}

export interface OrderCreatedPayload {
  orderId: string;
  userId: string;
  currency: string;
  totalAmount: number;
  itemCount: number;
}

export type OrderCreatedEvent = EventEnvelope<OrderCreatedPayload, "OrderCreated">;

export interface OrderRiskApprovedPayload {
  orderId: string;
  amount: number;
  currency: string;
  riskScore: number;
  approvedBy: string;
}

export type OrderRiskApprovedEvent = EventEnvelope<
  OrderRiskApprovedPayload,
  "OrderRiskApproved"
>;

export interface OrderRiskRejectedPayload {
  orderId: string;
  riskScore: number;
  reason: string;
  rejectedBy: string;
}

export type OrderRiskRejectedEvent = EventEnvelope<
  OrderRiskRejectedPayload,
  "OrderRiskRejected"
>;

export interface PaymentAuthorizedPayload {
  paymentId: string;
  orderId: string;
  amount: number;
  currency: string;
  provider: string;
}

export type PaymentAuthorizedEvent = EventEnvelope<
  PaymentAuthorizedPayload,
  "PaymentAuthorized"
>;

export interface PaymentFailedPayload {
  paymentId: string | null;
  orderId: string;
  reason: string;
  provider: string;
}

export type PaymentFailedEvent = EventEnvelope<PaymentFailedPayload, "PaymentFailed">;

export interface NotificationCommandPayload {
  notificationId: string;
  recipient: string;
  channel: "email" | "push" | "webhook";
  template: string;
  dataJson: string;
}

export type NotificationCommandEvent = EventEnvelope<
  NotificationCommandPayload,
  "NotificationCommand"
>;

export interface DeadLetterPayload {
  originalTopic: KafkaTopicName;
  originalPartition: number;
  originalOffset: string;
  errorMessage: string;
  errorStack: string | null;
  rawEvent: string | null;
}

export type DeadLetterEvent = EventEnvelope<DeadLetterPayload, "DeadLetterEvent">;

export type DomainEvent =
  | OrderCreatedEvent
  | OrderRiskApprovedEvent
  | OrderRiskRejectedEvent
  | PaymentAuthorizedEvent
  | PaymentFailedEvent
  | NotificationCommandEvent
  | DeadLetterEvent;

export const EVENT_TOPIC_MAP = {
  OrderCreated: KAFKA_TOPICS.orderOrderEvents,
  OrderRiskApproved: KAFKA_TOPICS.riskRiskEvents,
  OrderRiskRejected: KAFKA_TOPICS.riskRiskEvents,
  PaymentAuthorized: KAFKA_TOPICS.paymentPaymentEvents,
  PaymentFailed: KAFKA_TOPICS.paymentPaymentEvents,
  NotificationCommand: KAFKA_TOPICS.notificationNotificationCommands,
  DeadLetterEvent: KAFKA_TOPICS.deadLetterEvents
} as const;

export type DomainEventType = keyof typeof EVENT_TOPIC_MAP;

export const EVENT_SCHEMA_SUBJECTS = {
  OrderCreated: `${EVENT_TOPIC_MAP.OrderCreated}-OrderCreated-value`,
  OrderRiskApproved: `${EVENT_TOPIC_MAP.OrderRiskApproved}-OrderRiskApproved-value`,
  OrderRiskRejected: `${EVENT_TOPIC_MAP.OrderRiskRejected}-OrderRiskRejected-value`,
  PaymentAuthorized: `${EVENT_TOPIC_MAP.PaymentAuthorized}-PaymentAuthorized-value`,
  PaymentFailed: `${EVENT_TOPIC_MAP.PaymentFailed}-PaymentFailed-value`,
  NotificationCommand: `${EVENT_TOPIC_MAP.NotificationCommand}-NotificationCommand-value`,
  DeadLetterEvent: `${EVENT_TOPIC_MAP.DeadLetterEvent}-DeadLetterEvent-value`
} as const satisfies Record<DomainEventType, string>;
