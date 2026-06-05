import { Module } from "@nestjs/common";
import { GrpcClientsModule } from "../../grpc/grpc-clients.module";
import { OrdersController } from "./orders.controller";
import { OrdersService } from "./orders.service";

@Module({
  imports: [GrpcClientsModule],
  controllers: [OrdersController],
  providers: [OrdersService]
})
export class OrdersModule {}
