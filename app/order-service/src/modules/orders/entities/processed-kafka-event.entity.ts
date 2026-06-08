import { Column, CreateDateColumn, Entity, PrimaryGeneratedColumn } from "typeorm";

/**
 * Таблица идемпотентности для входящих Kafka-событий.
 *
 * Kafka даёт at-least-once доставку: consumer может получить одно и то же
 * событие повторно после rebalance, retry или падения процесса между обработкой
 * и commit offset. Unique constraint на `event_id` превращает повторную
 * доставку в no-op для бизнес-логики `order-service`.
 */
@Entity({ name: "processed_kafka_events" })
export class ProcessedKafkaEventEntity {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  /** `eventId` из domain envelope; главный ключ идемпотентности. */
  @Column({ name: "event_id", type: "uuid", unique: true })
  eventId!: string;

  /** Тип события сохраняется для аудита и удобной диагностики повторов. */
  @Column({ name: "event_type", type: "varchar", length: 80 })
  eventType!: string;

  /** Topic, из которого пришло событие. */
  @Column({ name: "source_topic", type: "varchar", length: 160 })
  sourceTopic!: string;

  /** Kafka offset исходного сообщения; полезен при разборе consumer lag и replay. */
  @Column({ name: "source_offset", type: "varchar", length: 64 })
  sourceOffset!: string;

  @CreateDateColumn({ name: "processed_at", type: "timestamptz" })
  processedAt!: Date;
}
