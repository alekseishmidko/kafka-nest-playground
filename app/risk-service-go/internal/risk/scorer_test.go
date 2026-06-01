package risk

import (
	"context"
	"testing"

	"github.com/kafka-playground/risk-service-go/internal/contracts"
)

func TestScorerRejectsHighRiskOrder(t *testing.T) {
	// Низкий threshold делает тест предсказуемым: дорогой заказ должен быть отклонен.
	scorer := NewScorer(10, 0.50)

	decision, err := scorer.Score(context.Background(), contracts.OrderCreatedPayload{
		OrderID:     "order-1",
		UserID:      "user-1",
		Currency:    "USD",
		TotalAmount: 3000,
		ItemCount:   30,
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Approved {
		t.Fatalf("expected high risk order to be rejected, score=%f", decision.Score)
	}
	if decision.Reason == "" {
		t.Fatal("expected rejection reason")
	}
}

func TestScorerApprovesLowRiskOrder(t *testing.T) {
	// Высокий threshold проверяет положительную ветку без долгого CPU scoring.
	scorer := NewScorer(10, 0.95)

	decision, err := scorer.Score(context.Background(), contracts.OrderCreatedPayload{
		OrderID:     "order-2",
		UserID:      "user-2",
		Currency:    "USD",
		TotalAmount: 10,
		ItemCount:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Approved {
		t.Fatalf("expected low risk order to be approved, score=%f reason=%s", decision.Score, decision.Reason)
	}
}
