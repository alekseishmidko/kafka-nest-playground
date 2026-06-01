package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/kafka-playground/risk-service-go/internal/config"
	"github.com/kafka-playground/risk-service-go/internal/contracts"
	"github.com/kafka-playground/risk-service-go/internal/risk"
	"github.com/kafka-playground/risk-service-go/internal/schemaregistry"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Service struct {
	// Service держит долгоживущие зависимости Kafka consumer/producer pipeline.
	cfg      config.Config
	logger   *slog.Logger
	registry *schemaregistry.Client
	scorer   risk.Scorer
	client   *kgo.Client
}

func NewService(cfg config.Config, logger *slog.Logger, registry *schemaregistry.Client, scorer risk.Scorer) (*Service, error) {
	// franz-go один клиент использует и для чтения, и для записи в Kafka.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.ClientID(cfg.KafkaClientID),
		kgo.ConsumerGroup(cfg.KafkaConsumerGroup),
		kgo.ConsumeTopics(contracts.TopicOrderEvents),
		// Offset коммитим вручную только после успешной публикации risk-события.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}

	return &Service{
		cfg:      cfg,
		logger:   logger,
		registry: registry,
		scorer:   scorer,
		client:   client,
	}, nil
}

func (s *Service) Run(ctx context.Context) error {
	// Run - бесконечный consumer loop. Он завершится, когда отменят context.
	s.logger.Info("risk consumer started",
		"topic", contracts.TopicOrderEvents,
		"groupId", s.cfg.KafkaConsumerGroup,
	)

	for {
		// PollFetches блокируется до новых сообщений или отмены context.
		fetches := s.client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return err
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			if err := s.handleRecord(ctx, record); err != nil {
				s.logger.Error("failed to process record",
					"topic", record.Topic,
					"partition", record.Partition,
					"offset", record.Offset,
					"error", err,
				)
				continue
			}
			// Если дошли сюда, значит событие обработано и результат опубликован.
			if err := s.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit record offset: %w", err)
			}
		}
	}
}

func (s *Service) Close() {
	s.client.Close()
}

func (s *Service) handleRecord(ctx context.Context, record *kgo.Record) error {
	// В Kafka лежит бинарный Avro payload с schema id в первых байтах.
	native, err := s.registry.Decode(ctx, record.Value)
	if err != nil {
		return err
	}

	event, err := orderCreatedFromNative(native)
	if err != nil {
		return err
	}
	if event.EventType != contracts.EventOrderCreated {
		s.logger.Debug("skipping non OrderCreated event", "eventType", event.EventType)
		return nil
	}

	s.logger.Info("order created event consumed",
		"orderId", event.Payload.OrderID,
		"eventId", event.EventID,
		"correlationId", event.CorrelationID,
		"totalAmount", event.Payload.TotalAmount,
		"itemCount", event.Payload.ItemCount,
	)

	// Scoring специально CPU-heavy: имитирует дорогие antifraud/risk правила.
	decision, err := s.scorer.Score(ctx, event.Payload)
	if err != nil {
		return err
	}

	// Schema Registry subject зависит от типа исходящего события.
	outgoing := s.riskEventNative(event, decision)
	subject := contracts.SubjectOrderRiskApproved
	eventType := contracts.EventOrderRiskApproved
	if !decision.Approved {
		subject = contracts.SubjectOrderRiskRejected
		eventType = contracts.EventOrderRiskRejected
	}

	value, err := s.registry.Encode(ctx, subject, outgoing)
	if err != nil {
		return err
	}

	// Сохраняем Kafka key от заказа, чтобы события одного заказа попадали в одну partition.
	result := &kgo.Record{
		Topic: contracts.TopicRiskEvents,
		Key:   record.Key,
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "x-correlation-id", Value: []byte(event.CorrelationID)},
			{Key: "x-causation-id", Value: []byte(event.EventID)},
			{Key: "x-event-id", Value: []byte(outgoing["eventId"].(string))},
			{Key: "x-event-type", Value: []byte(eventType)},
			{Key: "x-event-version", Value: []byte("1")},
		},
	}

	if err := s.produceSync(ctx, result); err != nil {
		return err
	}

	s.logger.Info("risk event published",
		"orderId", event.Payload.OrderID,
		"eventType", eventType,
		"riskScore", decision.Score,
		"topic", contracts.TopicRiskEvents,
	)
	return nil
}

func (s *Service) produceSync(ctx context.Context, record *kgo.Record) error {
	// ProduceSync проще для учебного сервиса: ждем ack от Kafka перед commit offset.
	results := s.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce risk event: %w", err)
	}
	return nil
}

func (s *Service) riskEventNative(source contracts.OrderCreatedEvent, decision risk.Decision) map[string]interface{} {
	// goavro принимает map[string]interface{} как "native" представление Avro record.
	eventID := uuid.NewString()
	base := map[string]interface{}{
		"eventId":       eventID,
		"eventVersion":  int32(1),
		"occurredAt":    time.Now().UTC().Format(time.RFC3339Nano),
		"correlationId": source.CorrelationID,
		// Avro union ["null", "string"] кодируется через map с именем выбранного типа.
		"causationId": map[string]interface{}{"string": source.EventID},
		"producer":    "risk-service-go",
	}

	if decision.Approved {
		base["eventType"] = contracts.EventOrderRiskApproved
		base["payload"] = map[string]interface{}{
			"orderId":    source.Payload.OrderID,
			"riskScore":  decision.Score,
			"approvedBy": "risk-service-go",
		}
		return base
	}

	base["eventType"] = contracts.EventOrderRiskRejected
	base["payload"] = map[string]interface{}{
		"orderId":    source.Payload.OrderID,
		"riskScore":  decision.Score,
		"reason":     decision.Reason,
		"rejectedBy": "risk-service-go",
	}
	return base
}

func orderCreatedFromNative(native map[string]interface{}) (contracts.OrderCreatedEvent, error) {
	// Преобразуем динамический Avro record в типизированную Go-структуру.
	payload, ok := native["payload"].(map[string]interface{})
	if !ok {
		return contracts.OrderCreatedEvent{}, fmt.Errorf("OrderCreated.payload is missing or invalid")
	}

	itemCount, err := intFromNative(payload["itemCount"])
	if err != nil {
		return contracts.OrderCreatedEvent{}, fmt.Errorf("payload.itemCount: %w", err)
	}

	return contracts.OrderCreatedEvent{
		EventID:       stringFromNative(native["eventId"]),
		EventType:     stringFromNative(native["eventType"]),
		EventVersion:  intFromNativeDefault(native["eventVersion"]),
		OccurredAt:    stringFromNative(native["occurredAt"]),
		CorrelationID: stringFromNative(native["correlationId"]),
		Producer:      stringFromNative(native["producer"]),
		Payload: contracts.OrderCreatedPayload{
			OrderID:     stringFromNative(payload["orderId"]),
			UserID:      stringFromNative(payload["userId"]),
			Currency:    stringFromNative(payload["currency"]),
			TotalAmount: floatFromNative(payload["totalAmount"]),
			ItemCount:   itemCount,
		},
	}, nil
}

func stringFromNative(value interface{}) string {
	// interface{} означает "значение любого типа"; после Avro decode типы проверяются вручную.
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func floatFromNative(value interface{}) float64 {
	// Avro double обычно приходит как float64, но helper терпим к другим числовым типам.
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int32:
		return float64(typed)
	case int64:
		return float64(typed)
	default:
		return 0
	}
}

func intFromNativeDefault(value interface{}) int {
	parsed, _ := intFromNative(value)
	return parsed
}

func intFromNative(value interface{}) (int, error) {
	// Avro int чаще всего декодируется как int32.
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int32:
		return int(typed), nil
	case int64:
		return int(typed), nil
	default:
		return 0, fmt.Errorf("expected int-compatible value, got %T", value)
	}
}
