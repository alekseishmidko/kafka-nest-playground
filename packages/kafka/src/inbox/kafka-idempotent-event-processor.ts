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
import { randomUUID } from "node:crypto";
import { KAFKA_INBOX_STORE, KAFKA_MODULE_OPTIONS } from "../kafka.tokens";
import type {
  KafkaConsumerMessageContext,
  KafkaModuleOptions
} from "../types";
import {
  KafkaInboxBusyError,
  type KafkaIdempotentProcessingResult,
  type KafkaInboxStore
} from "./kafka-inbox-store";

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

  /**
   * Закрывает подключение inbox adapter-а при штатной остановке приложения.
   */
  async onApplicationShutdown(): Promise<void> {
    await this.store.close?.();
  }
}
