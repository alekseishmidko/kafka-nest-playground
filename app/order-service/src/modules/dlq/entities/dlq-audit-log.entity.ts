import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn
} from "typeorm";
import { DeadLetterEventStatus } from "./dead-letter-event.entity";

export enum DlqAuditAction {
  Reprocess = "REPROCESS",
  Ignore = "IGNORE"
}

/**
 * Неизменяемая запись административного действия над DLQ.
 */
@Entity({ name: "dlq_audit_log" })
export class DlqAuditLogEntity {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ name: "dead_letter_event_id", type: "uuid" })
  deadLetterEventId!: string;

  @Column({ type: "varchar", length: 40 })
  action!: DlqAuditAction;

  @Column({ name: "operator_id", type: "varchar", length: 120 })
  operatorId!: string;

  @Column({ name: "previous_status", type: "varchar", length: 40 })
  previousStatus!: DeadLetterEventStatus;

  @Column({ name: "new_status", type: "varchar", length: 40 })
  newStatus!: DeadLetterEventStatus;

  @Column({ type: "text" })
  comment!: string;

  @Column({
    name: "reprocessed_event_id",
    type: "varchar",
    length: 120,
    nullable: true
  })
  reprocessedEventId!: string | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt!: Date;
}
