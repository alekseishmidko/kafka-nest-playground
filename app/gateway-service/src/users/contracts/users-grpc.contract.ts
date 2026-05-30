import { Observable } from "rxjs";

export interface GetUserRequest {
  id: string;
}

export interface UserResponse {
  id: string;
  email: string;
  name: string;
}

export interface UsersGrpcService {
  getUser(request: GetUserRequest): Observable<UserResponse>;
}
