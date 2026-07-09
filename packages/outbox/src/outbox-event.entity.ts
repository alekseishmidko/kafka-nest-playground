import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn
} from "typeorm";
import type {
  KafkaDomainEvent,
  KafkaTopicName
} from "@kafka-playground/kafka";
import type { TraceCarrier } from "@kafka-playground/observability";

/**
 * Состояние записи в transactional outbox.
 *
 * `PENDING` создаётся в той же DB-транзакции, что и бизнес-изменение.
 * `PUBLISHED` ставится только после успешной отправки во внешний transport.
 * `FAILED` означает, что запись не потеряна и будет повторена после backoff.
 */
export enum OutboxEventStatus {
  /** Событие сохранено в БД и ожидает первой попытки публикации. */
  Pending = "PENDING",
  /** Событие успешно отправлено во внешний брокер. */
  Published = "PUBLISHED",
  /** Последняя попытка публикации упала; событие будет повторено позже. */
  Failed = "FAILED",
  /** Оператор явно исключил событие из дальнейшей публикации. */
  Ignored = "IGNORED"
}

/**
 * Persisted outbox-запись для доменного события.
 *
 * Entity намеренно хранит полный event envelope как JSONB. Publisher не
 * пересобирает событие и не вызывает доменную логику повторно: он берёт уже
 * зафиксированный факт из БД и доставляет его в message broker.
 */
@Entity({ name: "outbox_events" })
export class OutboxEventEntity<
  TEvent extends KafkaDomainEvent = KafkaDomainEvent
> {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  /** Topic, куда publisher отправит событие. */
  @Column({ type: "varchar", length: 160 })
  topic!: KafkaTopicName;

  /** Message key, обычно id агрегата, по которому нужен ordering. */
  @Column({ name: "message_key", type: "varchar", length: 160 })
  messageKey!: string;

  /** Дублируется отдельно от JSON payload для индексов, метрик и диагностики. */
  @Column({ name: "event_type", type: "varchar", length: 80 })
  eventType!: string;

  /** Глобальный id события; unique constraint защищает от случайного дубля. */
  @Column({ name: "event_id", type: "uuid", unique: true })
  eventId!: string;

  /** Полный event envelope, который будет опубликован без пересборки. */
  @Column({ type: "jsonb" })
  event!: TEvent;

  /**
   * W3C trace context исходной операции.
   *
   * Поле находится рядом с event, а не внутри event payload: tracing является
   * технической metadata транспорта и не должен менять бизнес-контракт.
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

  /** Момент, раньше которого failed-событие не нужно публиковать снова. */
  @Column({ name: "next_attempt_at", type: "timestamptz", nullable: true })
  nextAttemptAt!: Date | null;

  /**
   * Идентификатор publisher-инстанса, который временно захватил запись.
   *
   * Поле подготавливает outbox к multi-instance publishing: один процесс
   * получает lease, остальные не должны публиковать эту же запись, пока lease
   * не истечёт.
   */
  @Column({ name: "locked_by", type: "varchar", length: 160, nullable: true })
  lockedBy!: string | null;

  /**
   * Время истечения lease.
   *
   * Если publisher упал после захвата записи, другой инстанс сможет повторно
   * забрать запись после `locked_until`. Значение `null` означает, что запись
   * сейчас никем не захвачена.
   */
  @Column({ name: "locked_until", type: "timestamptz", nullable: true })
  lockedUntil!: Date | null;

  /** Фактическое время успешной отправки во внешний transport. */
  @Column({ name: "published_at", type: "timestamptz", nullable: true })
  publishedAt!: Date | null;

  /** Последняя ошибка публикации для диагностики и ручного разбора. */
  @Column({ name: "last_error", type: "text", nullable: true })
  lastError!: string | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt!: Date;

  @UpdateDateColumn({ name: "updated_at", type: "timestamptz" })
  updatedAt!: Date;
}
