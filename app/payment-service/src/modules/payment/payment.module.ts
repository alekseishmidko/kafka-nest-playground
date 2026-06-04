import { Module } from "@nestjs/common";
import { PaymentAuthorizer } from "./payment.authorizer";
import { PaymentConsumer } from "./payment.consumer";
import { PaymentService } from "./payment.service";

@Module({
  providers: [PaymentAuthorizer, PaymentConsumer, PaymentService]
})
export class PaymentModule {}
