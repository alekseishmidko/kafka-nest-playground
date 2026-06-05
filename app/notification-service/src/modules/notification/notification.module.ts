import { Module } from "@nestjs/common";
import { NotificationConsumer } from "./notification.consumer";
import { NotificationDeliveryService } from "./notification-delivery.service";
import { NotificationService } from "./notification.service";

@Module({
  providers: [NotificationConsumer, NotificationDeliveryService, NotificationService]
})
export class NotificationModule {}
