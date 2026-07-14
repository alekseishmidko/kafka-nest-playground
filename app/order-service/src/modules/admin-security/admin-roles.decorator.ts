import { SetMetadata } from "@nestjs/common";
import {
  ADMIN_ROLES_METADATA,
  type AdminRole
} from "./admin-security.types";

/**
 * Ограничивает admin endpoint одной или несколькими ролями.
 */
export const AdminRoles = (...roles: AdminRole[]) =>
  SetMetadata(ADMIN_ROLES_METADATA, roles);
