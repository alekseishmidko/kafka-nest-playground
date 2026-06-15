import { Injectable } from "@nestjs/common";
import { PinoLogger } from "@kafka-playground/observability";

export interface NotificationDeliveryRequest {
  notificationId: string;
  /**
   * Стабильный ключ для API провайдера. Повторный запрос с тем же ключом не
   * должен создавать вторую отправку.
   */
  idempotencyKey: string;
  recipient: string;
  channel: "email" | "push" | "webhook";
  template: string;
  data: Record<string, unknown>;
  correlationId: string;
  causationId: string;
}

@Injectable()
export class NotificationDeliveryService {
  constructor(private readonly logger: PinoLogger) {
    this.logger.setContext(NotificationDeliveryService.name);
  }

  async deliver(request: NotificationDeliveryRequest): Promise<void> {
    this.logger.info(
      {
        notificationId: request.notificationId,
        idempotencyKey: request.idempotencyKey,
        recipient: request.recipient,
        channel: request.channel,
        template: request.template,
        correlationId: request.correlationId,
        causationId: request.causationId,
        data: request.data
      },
      "Mock notification delivered"
    );
  }
}
