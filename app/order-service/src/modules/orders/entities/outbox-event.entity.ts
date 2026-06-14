import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn
} from "typeorm";
import type { DomainEvent, KafkaTopicName } from "@kafka-playground/contracts";
import type { TraceCarrier } from "@kafka-playground/observability";

/**
 * Состояние записи в outbox-таблице.
 *
 * Outbox отделяет сохранение бизнес-сущности от отправки Kafka-сообщения:
 * заказ и запись `PENDING` сохраняются в одной транзакции, а публикация
 * выполняется отдельным publisher-циклом уже после commit.
 */
export enum OutboxEventStatus {
  /** Событие сохранено в БД и ожидает первой попытки публикации. */
  Pending = "PENDING",
  /** Событие успешно отправлено в Kafka; повторно публиковать его не нужно. */
  Published = "PUBLISHED",
  /** Последняя попытка публикации упала; событие будет повторено после backoff. */
  Failed = "FAILED"
}

/**
 * Persisted outbox-запись для доменного события.
 *
 * Важная гарантия: если `OrderCreated` сохранён здесь вместе с заказом,
 * событие не потеряется при падении процесса между DB commit и Kafka publish.
 * Это не делает публикацию exactly-once на уровне Kafka, поэтому downstream
 * consumers всё равно должны быть идемпотентными.
 */
@Entity({ name: "outbox_events" })
export class OutboxEventEntity {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  /** Kafka topic, куда publisher отправит событие. */
  @Column({ type: "varchar", length: 160 })
  topic!: KafkaTopicName;

  /** Kafka message key; для order pipeline это `orderId`, чтобы сохранить ordering. */
  @Column({ name: "message_key", type: "varchar", length: 160 })
  messageKey!: string;

  /** Дублируется отдельно от JSON payload, чтобы удобно фильтровать и дебажить outbox. */
  @Column({ name: "event_type", type: "varchar", length: 80 })
  eventType!: DomainEvent["eventType"];

  /** Глобальный id события; unique constraint защищает от случайного дубля outbox-записи. */
  @Column({ name: "event_id", type: "uuid", unique: true })
  eventId!: string;

  /** Полный event envelope, который будет сериализован через Schema Registry codec. */
  @Column({ type: "jsonb" })
  event!: DomainEvent;

  /**
   * W3C context исходной операции.
   *
   * Поле хранится отдельно от domain event: tracing является технической
   * метаинформацией транспорта и не должен менять бизнес-контракт события.
   */
  @Column({ name: "trace_context", type: "jsonb", nullable: true })
  traceContext!: TraceCarrier | null;

  @Column({
    type: "enum",
    enum: OutboxEventStatus,
    default: OutboxEventStatus.Pending
  })
  status!: OutboxEventStatus;

  /** Количество неуспешных попыток публикации. */
  @Column({ type: "int", default: 0 })
  attempts!: number;

  /** Момент, раньше которого failed-событие не нужно пробовать публиковать снова. */
  @Column({ name: "next_attempt_at", type: "timestamptz", nullable: true })
  nextAttemptAt!: Date | null;

  /** Фактическое время успешной отправки в Kafka. */
  @Column({ name: "published_at", type: "timestamptz", nullable: true })
  publishedAt!: Date | null;

  /** Последняя ошибка публикации; хранится для диагностики и ручного разбора. */
  @Column({ name: "last_error", type: "text", nullable: true })
  lastError!: string | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt!: Date;

  @UpdateDateColumn({ name: "updated_at", type: "timestamptz" })
  updatedAt!: Date;
}
