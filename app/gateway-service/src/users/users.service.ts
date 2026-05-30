import { Inject, Injectable, OnModuleInit } from "@nestjs/common";
import { ClientGrpc } from "@nestjs/microservices";
import { lastValueFrom } from "rxjs";
import { USERS_GRPC_CLIENT } from "../grpc/grpc-clients.constants";
import { UsersGrpcService } from "./contracts/users-grpc.contract";

@Injectable()
export class UsersService implements OnModuleInit {
  private usersGrpcService!: UsersGrpcService;

  constructor(@Inject(USERS_GRPC_CLIENT) private readonly client: ClientGrpc) {}

  onModuleInit() {
    this.usersGrpcService =
      this.client.getService<UsersGrpcService>("UsersService");
  }

  getUser(id: string) {
    return lastValueFrom(this.usersGrpcService.getUser({ id }));
  }
}
