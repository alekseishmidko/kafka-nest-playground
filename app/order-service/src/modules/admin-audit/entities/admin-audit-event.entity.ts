import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn
} from "typeorm";

export enum AdminAuditDecision {
  Allowed = "ALLOWED",
  Denied = "DENIED",
  Failed = "FAILED"
}

/**
 * Неизменяемая запись обращения к административному API.
 *
 * Таблица намеренно общая для всех `/admin/*` endpoint-ов: DLQ, outbox,
 * retention, ручные изменения заказа и будущие maintenance-действия должны
 * оставлять одинаковый след "кто, когда, что сделал и чем закончился запрос".
 */
@Entity({ name: "admin_audit_events" })
export class AdminAuditEventEntity {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ type: "varchar", length: 160, nullable: true })
  actor!: string | null;

  @Column({ type: "varchar", length: 120, nullable: true })
  role!: string | null;

  @Column({ type: "varchar", length: 16 })
  method!: string;

  @Column({ type: "text" })
  path!: string;

  @Column({ type: "varchar", length: 120 })
  action!: string;

  @Column({ name: "entity_type", type: "varchar", length: 120, nullable: true })
  entityType!: string | null;

  @Column({ name: "entity_id", type: "varchar", length: 160, nullable: true })
  entityId!: string | null;

  @Column({ type: "varchar", length: 24 })
  decision!: AdminAuditDecision;

  @Column({ name: "status_code", type: "integer" })
  statusCode!: number;

  @Column({ name: "request_id", type: "varchar", length: 160 })
  requestId!: string;

  @Column({ name: "correlation_id", type: "varchar", length: 160, nullable: true })
  correlationId!: string | null;

  @Column({ type: "inet", nullable: true })
  ip!: string | null;

  @Column({ name: "user_agent", type: "text", nullable: true })
  userAgent!: string | null;

  @Column({ name: "duration_ms", type: "integer" })
  durationMs!: number;

  @Column({ type: "jsonb", nullable: true })
  metadata!: Record<string, unknown> | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt!: Date;
}
