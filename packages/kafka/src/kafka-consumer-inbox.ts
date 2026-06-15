import {
  Inject,
  Injectable,
  Optional,
  type OnApplicationShutdown
} from "@nestjs/common";
import type { DomainEvent } from "@kafka-playground/contracts";
import {
  ApplicationMetrics,
  PinoLogger
} from "@kafka-playground/observability";
import { createHash, randomUUID } from "node:crypto";
import { Pool, type PoolConfig } from "pg";
import { KAFKA_INBOX_STORE, KAFKA_MODULE_OPTIONS } from "./kafka.tokens";
import type {
  KafkaConsumerMessageContext,
  KafkaModuleOptions
} from "./types";

export type KafkaInboxStatus = "PROCESSING" | "PREPARED" | "COMPLETED";

export interface KafkaInboxRecord<TResult> {
  status: KafkaInboxStatus;
  result: TResult | null;
  lockToken: string | null;
}

export interface KafkaInboxClaim {
  consumerName: string;
  eventId: string;
  eventType: string;
  sourceTopic: string;
  sourcePartition: number;
  sourceOffset: string;
  lockToken: string;
  lockedUntil: Date;
}

/**
 * Контракт долговременного inbox-хранилища.
 *
 * Inbox отделён от конкретной СУБД, чтобы доменные consumers не зависели от
 * PostgreSQL API. Реализация обязана атомарно захватывать событие по паре
 * `consumerName + eventId` и не выдавать одну активную lease двум процессам.
 */
export interface KafkaInboxStore {
  claim<TResult>(claim: KafkaInboxClaim): Promise<KafkaInboxRecord<TResult>>;
  savePrepared<TResult>(params: {
    consumerName: string;
    eventId: string;
    lockToken: string;
    result: TResult;
  }): Promise<void>;
  markCompleted(params: {
    consumerName: string;
    eventId: string;
    lockToken: string;
  }): Promise<void>;
  release(params: {
    consumerName: string;
    eventId: string;
    lockToken: string;
    error: unknown;
  }): Promise<void>;
  close?(): Promise<void>;
}

export interface KafkaIdempotentProcessingResult<TResult> {
  duplicate: boolean;
  result: TResult | null;
}

/**
 * Ошибка означает, что это же событие уже обрабатывает другой экземпляр.
 *
 * Ошибка намеренно retryable: текущий offset нельзя подтверждать, иначе при
 * падении владельца lease событие будет потеряно. Общая retry policy перенесёт
 * сообщение на следующий этап, где оно будет повторно захвачено после lease.
 */
export class KafkaInboxBusyError extends Error {
  constructor(consumerName: string, eventId: string) {
    super(
      `Kafka event ${eventId} is already being processed by ${consumerName}`
    );
    this.name = KafkaInboxBusyError.name;
  }
}

/**
 * Строит стабильный UUID для результата обработки входного события.
 *
 * Одинаковые `namespace` и `sourceEventId` всегда дают одинаковый идентификатор.
 * Это важно для crash recovery: повторная публикация подготовленного результата
 * распознаётся downstream consumer-ом как тот же event, а не как новое событие.
 */
export function createDeterministicEventId(
  namespace: string,
  sourceEventId: string
): string {
  const bytes = createHash("sha256")
    .update(namespace)
    .update("\0")
    .update(sourceEventId)
    .digest()
    .subarray(0, 16);

  // Версия 5 и RFC 4122 variant делают результат корректным UUID.
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");

  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20)
  ].join("-");
}

/**
 * Выполняет Kafka-handler по схеме durable inbox.
 *
 * Алгоритм разделён на две фазы:
 *
 * 1. `prepare` вычисляет результат и сохраняет его в PostgreSQL.
 * 2. `effect` публикует событие или вызывает внешний provider.
 *
 * Если процесс упадёт после `effect`, но до `COMPLETED`, следующая попытка
 * возьмёт уже сохранённый результат. Поэтому исходящее событие должно иметь
 * стабильный `eventId`, а внешний provider должен принимать idempotency key.
 * Это закрывает неизбежное окно между PostgreSQL и внешней системой без
 * распределённой транзакции.
 */
@Injectable()
export class KafkaIdempotentEventProcessor implements OnApplicationShutdown {
  private readonly leaseMs = 30_000;
  private readonly consumerName: string;

  constructor(
    @Inject(KAFKA_MODULE_OPTIONS)
    options: KafkaModuleOptions,
    @Inject(KAFKA_INBOX_STORE)
    private readonly store: KafkaInboxStore,
    private readonly logger: PinoLogger,
    @Optional()
    private readonly metrics?: ApplicationMetrics
  ) {
    this.consumerName = options.serviceName;
    this.logger.setContext(KafkaIdempotentEventProcessor.name);
  }

  /**
   * Обрабатывает событие не более одного раза логически.
   *
   * Физическая повторная публикация возможна после аварийного завершения, но
   * она содержит тот же `eventId`. Downstream consumer отсекает такой дубль по
   * собственному inbox, что соответствует модели at-least-once Kafka.
   */
  async process<TEvent extends DomainEvent, TResult>(
    context: KafkaConsumerMessageContext<TEvent>,
    prepare: () => Promise<TResult> | TResult,
    effect: (result: TResult) => Promise<void>
  ): Promise<KafkaIdempotentProcessingResult<TResult>> {
    const lockToken = randomUUID();
    const record = await this.store.claim<TResult>({
      consumerName: this.consumerName,
      eventId: context.event.eventId,
      eventType: context.event.eventType,
      sourceTopic: context.topic,
      sourcePartition: context.partition,
      sourceOffset: context.offset,
      lockToken,
      lockedUntil: new Date(Date.now() + this.leaseMs)
    });

    if (record.status === "COMPLETED") {
      this.metrics?.recordKafkaDuplicate(
        context.topic,
        context.event.eventType
      );
      this.logger.info(
        {
          eventId: context.event.eventId,
          eventType: context.event.eventType,
          topic: context.topic,
          partition: context.partition,
          offset: context.offset
        },
        "Duplicate Kafka event skipped"
      );

      return {
        duplicate: true,
        result: record.result
      };
    }

    if (!record.lockToken) {
      throw new KafkaInboxBusyError(
        this.consumerName,
        context.event.eventId
      );
    }

    try {
      const result =
        record.status === "PREPARED" && record.result !== null
          ? record.result
          : await prepare();

      if (record.status !== "PREPARED") {
        await this.store.savePrepared({
          consumerName: this.consumerName,
          eventId: context.event.eventId,
          lockToken: record.lockToken,
          result
        });
      }

      await effect(result);
      await this.store.markCompleted({
        consumerName: this.consumerName,
        eventId: context.event.eventId,
        lockToken: record.lockToken
      });

      return {
        duplicate: false,
        result
      };
    } catch (error) {
      await this.store.release({
        consumerName: this.consumerName,
        eventId: context.event.eventId,
        lockToken: record.lockToken,
        error
      });
      throw error;
    }
  }

  async onApplicationShutdown(): Promise<void> {
    await this.store.close?.();
  }
}

/**
 * PostgreSQL-реализация inbox с атомарным захватом через lease.
 *
 * Таблица создаётся миграцией приложения. Класс не выполняет DDL при старте,
 * чтобы production-схема изменялась только контролируемыми миграциями.
 */
export class PostgresKafkaInboxStore implements KafkaInboxStore {
  private readonly pool: Pool;

  constructor(config: PoolConfig) {
    this.pool = new Pool(config);
  }

  async claim<TResult>(
    claim: KafkaInboxClaim
  ): Promise<KafkaInboxRecord<TResult>> {
    const client = await this.pool.connect();

    try {
      await client.query("begin");
      const inserted = await client.query<InboxRow>(
        `
          insert into kafka_consumer_inbox (
            consumer_name,
            event_id,
            event_type,
            source_topic,
            source_partition,
            source_offset,
            status,
            lock_token,
            locked_until,
            attempts
          )
          values ($1, $2, $3, $4, $5, $6, 'PROCESSING', $7, $8, 1)
          on conflict (consumer_name, event_id) do nothing
          returning status, result, lock_token
        `,
        [
          claim.consumerName,
          claim.eventId,
          claim.eventType,
          claim.sourceTopic,
          claim.sourcePartition,
          claim.sourceOffset,
          claim.lockToken,
          claim.lockedUntil
        ]
      );

      if (inserted.rowCount === 1) {
        await client.query("commit");
        return mapInboxRow<TResult>(inserted.rows[0]);
      }

      const acquired = await client.query<InboxRow>(
        `
          update kafka_consumer_inbox
          set
            lock_token = $3,
            locked_until = $4,
            attempts = attempts + 1,
            last_error = null,
            updated_at = now()
          where consumer_name = $1
            and event_id = $2
            and status <> 'COMPLETED'
            and (locked_until is null or locked_until <= now())
          returning status, result, lock_token
        `,
        [
          claim.consumerName,
          claim.eventId,
          claim.lockToken,
          claim.lockedUntil
        ]
      );

      if (acquired.rowCount === 1) {
        await client.query("commit");
        return mapInboxRow<TResult>(acquired.rows[0]);
      }

      const existing = await client.query<InboxRow>(
        `
          select status, result, lock_token
          from kafka_consumer_inbox
          where consumer_name = $1 and event_id = $2
        `,
        [claim.consumerName, claim.eventId]
      );
      await client.query("commit");

      const row = existing.rows[0];

      if (!row) {
        throw new Error(
          `Kafka inbox record disappeared for ${claim.consumerName}/${claim.eventId}`
        );
      }

      if (row.status === "COMPLETED") {
        return {
          ...mapInboxRow<TResult>(row),
          lockToken: null
        };
      }

      return {
        ...mapInboxRow<TResult>(row),
        lockToken: null
      };
    } catch (error) {
      await client.query("rollback");
      throw error;
    } finally {
      client.release();
    }
  }

  async savePrepared<TResult>(params: {
    consumerName: string;
    eventId: string;
    lockToken: string;
    result: TResult;
  }): Promise<void> {
    const updated = await this.pool.query(
      `
        update kafka_consumer_inbox
        set status = 'PREPARED', result = $4::jsonb, updated_at = now()
        where consumer_name = $1
          and event_id = $2
          and lock_token = $3
          and status = 'PROCESSING'
      `,
      [
        params.consumerName,
        params.eventId,
        params.lockToken,
        JSON.stringify(params.result)
      ]
    );

    assertSingleUpdatedRow(updated.rowCount, params);
  }

  async markCompleted(params: {
    consumerName: string;
    eventId: string;
    lockToken: string;
  }): Promise<void> {
    const updated = await this.pool.query(
      `
        update kafka_consumer_inbox
        set
          status = 'COMPLETED',
          lock_token = null,
          locked_until = null,
          completed_at = now(),
          updated_at = now()
        where consumer_name = $1
          and event_id = $2
          and lock_token = $3
          and status = 'PREPARED'
      `,
      [params.consumerName, params.eventId, params.lockToken]
    );

    assertSingleUpdatedRow(updated.rowCount, params);
  }

  async release(params: {
    consumerName: string;
    eventId: string;
    lockToken: string;
    error: unknown;
  }): Promise<void> {
    await this.pool.query(
      `
        update kafka_consumer_inbox
        set
          lock_token = null,
          locked_until = now(),
          last_error = $4,
          updated_at = now()
        where consumer_name = $1
          and event_id = $2
          and lock_token = $3
          and status <> 'COMPLETED'
      `,
      [
        params.consumerName,
        params.eventId,
        params.lockToken,
        params.error instanceof Error
          ? params.error.message
          : String(params.error)
      ]
    );
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}

interface InboxRow {
  status: KafkaInboxStatus;
  result: unknown;
  lock_token: string | null;
}

function mapInboxRow<TResult>(
  row: InboxRow
): KafkaInboxRecord<TResult> {
  return {
    status: row.status,
    result: (row.result ?? null) as TResult | null,
    lockToken: row.lock_token
  };
}

function assertSingleUpdatedRow(
  rowCount: number | null,
  params: { consumerName: string; eventId: string }
): void {
  if (rowCount !== 1) {
    throw new Error(
      `Kafka inbox lease was lost for ${params.consumerName}/${params.eventId}`
    );
  }
}
