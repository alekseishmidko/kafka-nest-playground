import { SetMetadata } from "@nestjs/common";
import {
  ADMIN_PERMISSIONS_METADATA,
  type AdminPermission
} from "./admin-security.types";

/**
 * Требует одну или несколько admin permissions.
 *
 * Роли отвечают на вопрос "кто это", permissions - "какое действие он может
 * выполнить". Это позволяет всем `/admin/*` endpoint-ам явно помечать уровень
 * риска: read, write или dangerous.
 */
export const AdminPermissions = (...permissions: AdminPermission[]) =>
  SetMetadata(ADMIN_PERMISSIONS_METADATA, permissions);
