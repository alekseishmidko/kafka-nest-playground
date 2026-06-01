package contracts

const (
	// Эти строки должны совпадать с packages/contracts в TypeScript-части монорепы.
	TopicOrderEvents = "order.order-events"
	TopicRiskEvents  = "risk.risk-events"

	EventOrderCreated      = "OrderCreated"
	EventOrderRiskApproved = "OrderRiskApproved"
	EventOrderRiskRejected = "OrderRiskRejected"

	SubjectOrderCreated      = TopicOrderEvents + "-OrderCreated-value"
	SubjectOrderRiskApproved = TopicRiskEvents + "-OrderRiskApproved-value"
	SubjectOrderRiskRejected = TopicRiskEvents + "-OrderRiskRejected-value"
)

type EventEnvelope[T any] struct {
	// T any - generic payload: общий envelope один, payload у каждого события свой.
	EventID       string  `json:"eventId"`
	EventType     string  `json:"eventType"`
	EventVersion  int     `json:"eventVersion"`
	OccurredAt    string  `json:"occurredAt"`
	CorrelationID string  `json:"correlationId"`
	CausationID   *string `json:"causationId"`
	Producer      string  `json:"producer"`
	Payload       T       `json:"payload"`
}

type OrderCreatedPayload struct {
	// json-теги документируют имена полей из общего event-контракта.
	OrderID     string  `json:"orderId"`
	UserID      string  `json:"userId"`
	Currency    string  `json:"currency"`
	TotalAmount float64 `json:"totalAmount"`
	ItemCount   int     `json:"itemCount"`
}

type OrderCreatedEvent = EventEnvelope[OrderCreatedPayload]

type OrderRiskApprovedPayload struct {
	OrderID    string  `json:"orderId"`
	RiskScore  float64 `json:"riskScore"`
	ApprovedBy string  `json:"approvedBy"`
}

type OrderRiskRejectedPayload struct {
	OrderID    string  `json:"orderId"`
	RiskScore  float64 `json:"riskScore"`
	Reason     string  `json:"reason"`
	RejectedBy string  `json:"rejectedBy"`
}
