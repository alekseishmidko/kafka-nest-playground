export const KAFKA_TOPICS = {
  identityUserEvents: "identity.user-events",
  catalogProductEvents: "catalog.product-events",
  catalogInventoryEvents: "catalog.inventory-events",
  orderOrderEvents: "order.order-events",
  paymentPaymentEvents: "payment.payment-events",
  riskRiskEvents: "risk.risk-events",
  pricingPriceEvents: "pricing.price-events",
  notificationNotificationCommands: "notification.notification-commands",
  analyticsDomainEvents: "analytics.domain-events",
  deadLetterEvents: "dead-letter.events"
} as const;

export type KafkaTopicName = (typeof KAFKA_TOPICS)[keyof typeof KAFKA_TOPICS];

export const KAFKA_TOPIC_NAMES = Object.values(KAFKA_TOPICS);

export const TOPIC_OWNERS = {
  [KAFKA_TOPICS.identityUserEvents]: "identity-service",
  [KAFKA_TOPICS.catalogProductEvents]: "catalog-service",
  [KAFKA_TOPICS.catalogInventoryEvents]: "catalog-service",
  [KAFKA_TOPICS.orderOrderEvents]: "order-service",
  [KAFKA_TOPICS.paymentPaymentEvents]: "payment-service",
  [KAFKA_TOPICS.riskRiskEvents]: "risk-service-go",
  [KAFKA_TOPICS.pricingPriceEvents]: "pricing-service-go",
  [KAFKA_TOPICS.notificationNotificationCommands]: "notification-service",
  [KAFKA_TOPICS.analyticsDomainEvents]: "analytics-service-go",
  [KAFKA_TOPICS.deadLetterEvents]: "platform"
} as const satisfies Record<KafkaTopicName, string>;

export const TOPIC_KEY_STRATEGY = {
  [KAFKA_TOPICS.identityUserEvents]: "userId",
  [KAFKA_TOPICS.catalogProductEvents]: "productId",
  [KAFKA_TOPICS.catalogInventoryEvents]: "productId",
  [KAFKA_TOPICS.orderOrderEvents]: "orderId",
  [KAFKA_TOPICS.paymentPaymentEvents]: "orderId",
  [KAFKA_TOPICS.riskRiskEvents]: "orderId",
  [KAFKA_TOPICS.pricingPriceEvents]: "productId",
  [KAFKA_TOPICS.notificationNotificationCommands]: "notificationId",
  [KAFKA_TOPICS.analyticsDomainEvents]: "aggregateId",
  [KAFKA_TOPICS.deadLetterEvents]: "eventId"
} as const satisfies Record<KafkaTopicName, string>;
