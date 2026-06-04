import { Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import type { OrderRiskApprovedPayload } from "@kafka-playground/contracts";
import { createHash } from "node:crypto";

export interface PaymentAuthorizationDecision {
  authorized: boolean;
  paymentId: string | null;
  reason?: string;
}

@Injectable()
export class PaymentAuthorizer {
  private readonly failureThreshold: number;

  constructor(config: ConfigService) {
    this.failureThreshold = normalizeThreshold(
      Number(config.get<string>("PAYMENT_FAILURE_THRESHOLD") ?? 0.18)
    );
  }

  authorize(payload: OrderRiskApprovedPayload): PaymentAuthorizationDecision {
    const seed = `${payload.orderId}:${payload.amount.toFixed(2)}:${payload.currency}:${payload.riskScore.toFixed(4)}`;
    const hash = createHash("sha256").update(seed).digest();
    const failureFactor = Number(hash.readBigUInt64BE(0) % 10000n) / 10000;

    if (failureFactor < this.failureThreshold) {
      return {
        authorized: false,
        paymentId: null,
        reason: "payment_provider_declined"
      };
    }

    return {
      authorized: true,
      paymentId: `pay_${hash.subarray(0, 12).toString("hex")}`
    };
  }
}

function normalizeThreshold(value: number): number {
  if (!Number.isFinite(value) || value <= 0 || value >= 1) {
    return 0.18;
  }

  return value;
}
