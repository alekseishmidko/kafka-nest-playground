import { Pool, type PoolConfig } from "pg";
import type {
  KafkaInboxClaim,
  KafkaInboxRecord,
  KafkaInboxStatus,
  KafkaInboxStore
} from "./kafka-inbox-store";

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

  /**
   * Создаёт новую inbox-запись либо захватывает незавершённую запись после
   * истечения lease. Завершённое событие возвращается без нового lock token.
   */
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

  /**
   * Сохраняет вычисленный результат до выполнения внешнего side effect.
   */
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

  /**
   * Завершает обработку только для владельца текущей lease.
   */
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

  /**
   * Освобождает lease после ошибки и сохраняет диагностическое сообщение.
   */
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

  /**
   * Закрывает PostgreSQL pool при остановке приложения.
   */
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
