package risk

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/kafka-playground/risk-service-go/internal/contracts"
)

type Scorer struct {
	// iterations управляет искусственной CPU-нагрузкой, threshold - границей отказа.
	iterations int
	threshold  float64
}

type Decision struct {
	// Decision - доменное решение risk-сервиса после scoring.
	Approved bool
	Score    float64
	Reason   string
}

func NewScorer(iterations int, threshold float64) Scorer {
	// Нормализуем настройки, чтобы некорректный env не создавал бессмысленный scorer.
	if iterations < 1 {
		iterations = 1
	}
	if threshold <= 0 || threshold >= 1 {
		threshold = 0.72
	}
	return Scorer{iterations: iterations, threshold: threshold}
}

func (s Scorer) Score(ctx context.Context, payload contracts.OrderCreatedPayload) (Decision, error) {
	// Seed строится из данных заказа, поэтому результат детерминирован для одного заказа.
	seed := []byte(fmt.Sprintf("%s:%s:%s:%0.2f:%d", payload.OrderID, payload.UserID, payload.Currency, payload.TotalAmount, payload.ItemCount))
	hash := sha256.Sum256(seed)

	for i := 0; i < s.iterations; i++ {
		if i%8192 == 0 {
			// Даже тяжелая CPU-задача должна уметь останавливаться при shutdown.
			select {
			case <-ctx.Done():
				return Decision{}, ctx.Err()
			default:
			}
		}
		roundInput := make([]byte, len(hash)+8)
		copy(roundInput, hash[:])
		binary.BigEndian.PutUint64(roundInput[len(hash):], uint64(i))
		hash = sha256.Sum256(roundInput)
	}

	// Итоговый score смешивает детерминированный hash и простые признаки заказа.
	randomFactor := float64(binary.BigEndian.Uint64(hash[:8])%10000) / 10000
	amountFactor := math.Min(payload.TotalAmount/2000, 1)
	itemFactor := math.Min(float64(payload.ItemCount)/20, 1)
	score := 0.50*amountFactor + 0.25*itemFactor + 0.25*randomFactor
	score = math.Round(score*10000) / 10000

	// Чем выше score, тем рискованнее заказ.
	if score >= s.threshold {
		return Decision{
			Approved: false,
			Score:    score,
			Reason:   "risk_score_threshold_exceeded",
		}, nil
	}

	return Decision{
		Approved: true,
		Score:    score,
	}, nil
}
