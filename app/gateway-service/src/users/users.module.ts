import { Module } from "@nestjs/common";
import { GrpcClientsModule } from "../grpc/grpc-clients.module";
import { UsersController } from "./users.controller";
import { UsersService } from "./users.service";

@Module({
  imports: [GrpcClientsModule],
  controllers: [UsersController],
  providers: [UsersService]
})
export class UsersModule {}
