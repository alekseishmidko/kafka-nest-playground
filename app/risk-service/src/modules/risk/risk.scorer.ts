import { Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import type { OrderCreatedPayload } from "@kafka-playground/contracts";
import { createHash } from "node:crypto";

export interface RiskDecision {
  // approved=false означает, что заказ не прошел risk check.
  approved: boolean;
  // score в диапазоне 0..1: чем выше значение, тем рискованнее заказ.
  score: number;
  // reason заполняется только для отказа.
  reason?: string;
}

@Injectable()
export class RiskScorer {
  private readonly iterations: number;
  private readonly threshold: number;

  constructor(config: ConfigService) {
    // Количество итераций управляет искусственной CPU-нагрузкой.
    // В prod оно выше, чтобы сервис был похож на тяжелый scoring workload.
    this.iterations = normalizeIterations(
      Number(config.get<string>("RISK_SCORE_ITERATIONS") ?? 300000)
    );
    // threshold - граница отказа. score >= threshold => OrderRiskRejected.
    this.threshold = normalizeThreshold(
      Number(config.get<string>("RISK_SCORE_THRESHOLD") ?? 0.72)
    );
  }

  score(payload: OrderCreatedPayload): RiskDecision {
    // Seed строится из полей заказа. Для одного и того же заказа результат будет детерминированным.
    const seed = `${payload.orderId}:${payload.userId}:${payload.currency}:${payload.totalAmount.toFixed(2)}:${payload.itemCount}`;
    let hash = createHash("sha256").update(seed).digest();

    // Имитируем дорогой scoring: много раз пересчитываем hash.
    // В реальном сервисе здесь могли бы быть ML-модель, graph lookup или сложные fraud rules.
    for (let index = 0; index < this.iterations; index += 1) {
      const round = Buffer.allocUnsafe(hash.length + 8);
      hash.copy(round, 0);
      round.writeBigUInt64BE(BigInt(index), hash.length);
      hash = createHash("sha256").update(round).digest();
    }

    // randomFactor не случайный: он получается из hash и остается стабильным для одного заказа.
    const randomFactor = Number(hash.readBigUInt64BE(0) % 10000n) / 10000;
    // Чем больше сумма и количество товаров, тем выше вклад в риск.
    const amountFactor = Math.min(payload.totalAmount / 2000, 1);
    const itemFactor = Math.min(payload.itemCount / 20, 1);
    // Весовая формула простая и прозрачная для учебного примера.
    const score =
      Math.round((0.5 * amountFactor + 0.25 * itemFactor + 0.25 * randomFactor) * 10000) /
      10000;

    // Порог включительный: score ровно на threshold тоже считается отказом.
    if (score >= this.threshold) {
      return {
        approved: false,
        score,
        reason: "risk_score_threshold_exceeded"
      };
    }

    return {
      approved: true,
      score
    };
  }
}

function normalizeIterations(value: number): number {
  // Защищаем сервис от NaN, отрицательных и дробных значений в env.
  if (!Number.isFinite(value) || value < 1) {
    return 300000;
  }

  return Math.trunc(value);
}

function normalizeThreshold(value: number): number {
  // threshold должен быть внутри диапазона 0..1, иначе возвращаем безопасный default.
  if (!Number.isFinite(value) || value <= 0 || value >= 1) {
    return 0.72;
  }

  return value;
}
