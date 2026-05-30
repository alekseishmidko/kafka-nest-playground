import { Module } from "@nestjs/common";
import { ConfigModule } from "@nestjs/config";
import { GrpcClientsModule } from "./grpc/grpc-clients.module";
import { HealthModule } from "./health/health.module";
import { UsersModule } from "./users/users.module";

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true
    }),
    GrpcClientsModule,
    HealthModule,
    UsersModule
  ]
})
export class AppModule {}
