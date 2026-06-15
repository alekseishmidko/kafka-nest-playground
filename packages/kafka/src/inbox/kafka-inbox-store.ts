/**
 * Состояние долговременной обработки Kafka-события.
 *
 * `PROCESSING` означает вычисление результата, `PREPARED` фиксирует результат
 * до внешнего side effect, `COMPLETED` запрещает повторную бизнес-обработку.
 */
export type KafkaInboxStatus = "PROCESSING" | "PREPARED" | "COMPLETED";

/**
 * Результат попытки захватить Kafka-событие в durable inbox.
 */
export interface KafkaInboxRecord<TResult> {
  status: KafkaInboxStatus;
  result: TResult | null;
  lockToken: string | null;
}

/**
 * Данные для атомарного захвата входящего события.
 */
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
 * Inbox отделён от конкретной СУБД, чтобы orchestration и доменные consumers
 * не зависели от PostgreSQL API. Реализация обязана атомарно захватывать
 * событие по паре `consumerName + eventId` и не выдавать одну активную lease
 * двум процессам.
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

/**
 * Результат идемпотентной обработки, доступный вызывающему consumer-у.
 */
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
