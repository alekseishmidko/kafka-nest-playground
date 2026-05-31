import { Module } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { ClientsModule, Transport } from "@nestjs/microservices";
import { join } from "node:path";
import { ORDERS_GRPC_CLIENT } from "./grpc-clients.constants";

@Module({
  imports: [
    ClientsModule.registerAsync([
      {
        name: ORDERS_GRPC_CLIENT,
        inject: [ConfigService],
        useFactory: (config: ConfigService) => ({
          transport: Transport.GRPC,
          options: {
            package: "orders",
            protoPath: join(
              process.cwd(),
              "../../packages/contracts/proto/orders.proto"
            ),
            url: config.getOrThrow<string>("ORDER_SERVICE_GRPC_URL")
          }
        })
      }
    ])
  ],
  exports: [ClientsModule]
})
export class GrpcClientsModule {}
