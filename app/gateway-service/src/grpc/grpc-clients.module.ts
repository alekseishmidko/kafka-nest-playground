import { Module } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { ClientsModule, Transport } from "@nestjs/microservices";
import { join } from "node:path";
import { USERS_GRPC_CLIENT } from "./grpc-clients.constants";

@Module({
  imports: [
    ClientsModule.registerAsync([
      {
        name: USERS_GRPC_CLIENT,
        inject: [ConfigService],
        useFactory: (config: ConfigService) => ({
          transport: Transport.GRPC,
          options: {
            package: "users",
            protoPath: join(process.cwd(), "proto/users.proto"),
            url: config.get<string>("USERS_GRPC_URL", "localhost:50051")
          }
        })
      }
    ])
  ],
  exports: [ClientsModule]
})
export class GrpcClientsModule {}
