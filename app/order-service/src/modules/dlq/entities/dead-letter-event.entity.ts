import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
  VersionColumn
} from "typeorm";
import type {
  DomainEvent,
  KafkaTopicName
} from "@kafka-playground/contracts";

/**
 * Состояние ручной обработки записи Dead Letter Queue.
 */
export enum DeadLetterEventStatus {
  /** Событие ожидает анализа или повторной публикации оператором. */
  New = "NEW",
  /** Исправленная копия события поставлена в transactional outbox. */
  Reprocessed = "REPROCESSED",
  /** Оператор признал событие неактуальным и исключил его из обработки. */
  Ignored = "IGNORED"
}

/**
 * Постоянное представление `DeadLetterEvent`.
 *
 * Kafka topic является транспортным журналом, но не удобен для административных
 * запросов, фильтрации и фиксации ручных решений. Эта entity хранит как
 * техническую причину сбоя, так и исходный domain envelope.
 */
@Entity({ name: "dead_letter_events" })
export class DeadLetterEventEntity {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  /** `eventId` самого `DeadLetterEvent`; unique constraint обеспечивает идемпотентность consumer-а. */
  @Column({
    name: "dead_letter_event_id",
    type: "varchar",
    length: 120,
    unique: true
  })
  deadLetterEventId!: string;

  /** `eventId` исходного доменного события, извлечённый из `rawEvent`. */
  @Column({
    name: "original_event_id",
    type: "varchar",
    length: 120,
    nullable: true
  })
  originalEventId!: string | null;

  /** Topic, в который будет возвращено исправленное событие. */
  @Column({ name: "original_topic", type: "varchar", length: 160 })
  originalTopic!: KafkaTopicName;

  @Column({ name: "original_partition", type: "int" })
  originalPartition!: number;

  @Column({ name: "original_offset", type: "varchar", length: 64 })
  originalOffset!: string;

  /** Kafka key исходного события; сохраняет partition ordering при reprocess. */
  @Column({
    name: "message_key",
    type: "varchar",
    length: 160,
    nullable: true
  })
  messageKey!: string | null;

  /** Машиночитаемый код последней ошибки из `x-error-code`. */
  @Column({ name: "error_code", type: "varchar", length: 120 })
  errorCode!: string;

  @Column({ name: "error_message", type: "text" })
  errorMessage!: string;

  @Column({ name: "error_stack", type: "text", nullable: true })
  errorStack!: string | null;

  @Column({ name: "retry_count", type: "int" })
  retryCount!: number;

  @Column({
    name: "first_failed_at",
    type: "timestamptz",
    nullable: true
  })
  firstFailedAt!: Date | null;

  /** Десериализованный исходный envelope; `null`, если DLQ payload был повреждён. */
  @Column({ name: "original_event", type: "jsonb", nullable: true })
  originalEvent!: DomainEvent | null;

  @Column({
    type: "enum",
    enum: DeadLetterEventStatus,
    enumName: "dead_letter_events_status_enum",
    default: DeadLetterEventStatus.New
  })
  status!: DeadLetterEventStatus;

  /** `eventId` новой копии, поставленной в outbox при reprocess. */
  @Column({
    name: "reprocessed_event_id",
    type: "varchar",
    length: 120,
    nullable: true
  })
  reprocessedEventId!: string | null;

  @Column({
    name: "reprocessed_at",
    type: "timestamptz",
    nullable: true
  })
  reprocessedAt!: Date | null;

  @Column({ name: "ignored_at", type: "timestamptz", nullable: true })
  ignoredAt!: Date | null;

  @Column({ name: "ignore_reason", type: "text", nullable: true })
  ignoreReason!: string | null;

  /** Идентификатор оператора, выполнившего конечное действие. */
  @Column({
    name: "resolved_by",
    type: "varchar",
    length: 120,
    nullable: true
  })
  resolvedBy!: string | null;

  /** Обязательный комментарий оператора для аудита reprocess/ignore. */
  @Column({
    name: "resolution_comment",
    type: "text",
    nullable: true
  })
  resolutionComment!: string | null;

  /**
   * Optimistic version возвращается Admin API и должна передаваться обратно
   * при изменении записи.
   */
  @VersionColumn({ type: "int", default: 1 })
  version!: number;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt!: Date;

  @UpdateDateColumn({ name: "updated_at", type: "timestamptz" })
  updatedAt!: Date;
}
