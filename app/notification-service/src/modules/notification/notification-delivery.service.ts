import { Injectable } from "@nestjs/common";
import { PinoLogger } from "@kafka-playground/observability";

export interface NotificationDeliveryRequest {
  notificationId: string;
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
