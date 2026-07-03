import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit
} from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { DataSource } from "typeorm";
import { PinoLogger } from "@kafka-playground/observability";

interface RetentionTarget {
  /** Human-readable имя таблицы/набора данных для структурированных логов. */
  name: string;

  /**
   * Имя env-переменной, которая задаёт срок хранения в днях для конкретной
   * технической таблицы. Разные таблицы имеют разную диагностическую ценность:
   * outbox обычно можно чистить быстрее, а inbox/idempotency часто полезно
   * держать дольше для расследования дублей и replay-сценариев.
   */
  retentionDaysEnv: string;

  /** Консервативный срок хранения, если env-переменная не задана или неверна. */
  defaultRetentionDays: number;

  /**
   * Удаляет одну ограниченную пачку записей старше cutoff.
   *
   * Метод намеренно не делает полный sweep таблицы за один вызов: большие
   * DELETE без лимита могут долго держать locks, раздувать WAL и мешать
   * основной order pipeline нагрузке.
   */
  deleteBatch(cutoff: Date, batchSize: number): Promise<number>;
}

/**
 * Retention policy — это правило, сколько времени сервис хранит старые
 * технические записи и какие записи можно безопасно удалить автоматически.
 *
 * Для Kafka/outbox-системы retention policy не является простой уборкой места:
 * эти таблицы участвуют в гарантиях доставки и идемпотентности. Поэтому policy
 * обязана различать "рабочие" записи, которые ещё могут восстановить сообщение
 * после сбоя, и "завершённые" записи, которые уже стали историей обработки.
 *
 * В этом сервисе policy применяется только к завершённым данным:
 * `outbox_events.PUBLISHED`, `processed_kafka_events` и
 * `kafka_consumer_inbox.COMPLETED`. Незавершённые записи не удаляются, потому
 * что они участвуют в гарантиях доставки, идемпотентности и crash recovery.
 *
 * Таблицы и смысл очистки:
 *
 * - `outbox_events`: удаляются только успешно опубликованные события. Записи
 *   `PENDING` и `FAILED` являются очередью доставки; их удаление означало бы
 *   потерю события.
 * - `processed_kafka_events`: хранит факт применения lifecycle-событий
 *   `order-service`. После истечения retention window старые `eventId`
 *   больше не защищают от очень поздней повторной доставки, поэтому срок
 *   хранения должен быть больше Kafka retention и ожидаемого replay window.
 * - `kafka_consumer_inbox`: удаляются только `COMPLETED` записи. Состояния
 *   `PROCESSING` и `PREPARED` нужны для восстановления после падения между
 *   вычислением результата, внешним side effect и финальной фиксацией.
 *
 * Очистка выполняется пакетами и использует `FOR UPDATE SKIP LOCKED`, чтобы
 * несколько реплик сервиса могли запустить retention одновременно без ожидания
 * друг друга и без удаления одной и той же строки в конфликтующих транзакциях.
 *
 * В production значения retention следует выбирать вместе с Kafka topic
 * retention, SLA расследования инцидентов, требованиями аудита и объёмом БД.
 * Слишком короткий срок хранения уменьшает стоимость БД, но ухудшает защиту
 * от поздних дублей и усложняет диагностику.
 */
@Injectable()
export class TechnicalRetentionService
  implements OnModuleInit, OnApplicationShutdown
{
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private readonly dataSource: DataSource,
    private readonly config: ConfigService,
    private readonly logger: PinoLogger
  ) {
    this.logger.setContext(TechnicalRetentionService.name);
  }

  /**
   * Запускает retention job при старте приложения и затем повторяет по таймеру.
   *
   * Первый запуск полезен после длительного простоя сервиса: если приложение
   * было выключено несколько дней, устаревшие записи не ждут следующего
   * суточного окна. Ошибки внутри cleanup не пробрасываются наружу, поэтому
   * retention не должен ронять бизнес-сервис.
   */
  onModuleInit(): void {
    void this.cleanup();
    this.timer = setInterval(() => {
      void this.cleanup();
    }, this.cleanupIntervalMs);
  }

  /**
   * Останавливает periodic timer при shutdown.
   *
   * Сам cleanup не получает отдельной cancellation token: запросы короткие и
   * пакетные, а NestJS shutdown hooks всё равно дождутся завершения текущих
   * promise-ов, если они уже выполняются.
   */
  onApplicationShutdown(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Выполняет один проход retention policy по всем техническим таблицам.
   *
   * Один проход удаляет максимум `TECHNICAL_RETENTION_BATCH_SIZE` строк из
   * каждой таблицы. Если накопился большой backlog старых записей, он будет
   * очищаться постепенно в последующих циклах. Такой подход снижает влияние
   * maintenance-операции на latency основного Kafka/order flow.
   */
  async cleanup(): Promise<void> {
    const batchSize = this.readPositiveInteger(
      "TECHNICAL_RETENTION_BATCH_SIZE",
      1000
    );

    for (const target of this.targets) {
      await this.cleanupTarget(target, batchSize);
    }
  }

  /**
   * Применяет retention policy к одному target.
   *
   * Cutoff вычисляется как "сейчас минус retentionDays". Всё, что новее cutoff,
   * остаётся в БД. Всё, что старше cutoff, всё равно удаляется только если
   * SQL-запрос target-а считает состояние записи безопасным для удаления.
   */
  private async cleanupTarget(
    target: RetentionTarget,
    batchSize: number
  ): Promise<void> {
    const retentionDays = this.readPositiveInteger(
      target.retentionDaysEnv,
      target.defaultRetentionDays
    );
    const cutoff = new Date(
      Date.now() - retentionDays * 24 * 60 * 60 * 1000
    );

    try {
      const deleted = await target.deleteBatch(cutoff, batchSize);

      if (deleted > 0) {
        this.logger.info(
          {
            target: target.name,
            deleted,
            cutoff,
            retentionDays,
            batchSize
          },
          "Technical records deleted by retention policy"
        );
      }
    } catch (error) {
      this.logger.warn(
        {
          target: target.name,
          error: error instanceof Error ? error.message : String(error)
        },
        "Technical retention cleanup failed"
      );
    }
  }

  /**
   * Интервал запуска cleanup job.
   *
   * Значение задаётся в часах, потому что retention обычно является
   * низкочастотной maintenance-задачей. При необходимости локального теста
   * можно поставить `TECHNICAL_RETENTION_INTERVAL_HOURS=1`; значения меньше
   * одного часа намеренно не поддерживаются текущим парсером.
   */
  private get cleanupIntervalMs(): number {
    return (
      this.readPositiveInteger("TECHNICAL_RETENTION_INTERVAL_HOURS", 24) *
      60 *
      60 *
      1000
    );
  }

  /**
   * Описание всех таблиц, на которые распространяется технический retention.
   *
   * Список находится в коде, а не полностью в конфиге, чтобы случайная
   * env-ошибка не позволила удалить произвольную таблицу или изменить SQL.
   * Конфигурация управляет только сроками и размером пачки.
   */
  private get targets(): RetentionTarget[] {
    return [
      {
        name: "outbox_events",
        retentionDaysEnv: "OUTBOX_RETENTION_DAYS",
        defaultRetentionDays: 30,
        deleteBatch: (cutoff, batchSize) =>
          this.deletePublishedOutboxBefore(cutoff, batchSize)
      },
      {
        name: "processed_kafka_events",
        retentionDaysEnv: "PROCESSED_KAFKA_EVENTS_RETENTION_DAYS",
        defaultRetentionDays: 90,
        deleteBatch: (cutoff, batchSize) =>
          this.deleteProcessedKafkaEventsBefore(cutoff, batchSize)
      },
      {
        name: "kafka_consumer_inbox",
        retentionDaysEnv: "KAFKA_CONSUMER_INBOX_RETENTION_DAYS",
        defaultRetentionDays: 90,
        deleteBatch: (cutoff, batchSize) =>
          this.deleteCompletedInboxBefore(cutoff, batchSize)
      }
    ];
  }

  /**
   * Удаляет опубликованные outbox-события старше cutoff.
   *
   * Безопасное состояние только `PUBLISHED`: событие уже отправлено в Kafka,
   * а downstream идемпотентность защищает от повторной доставки. `FAILED` и
   * `PENDING` не трогаются, потому что они являются durable очередью доставки.
   */
  private async deletePublishedOutboxBefore(
    cutoff: Date,
    batchSize: number
  ): Promise<number> {
    const result = await this.dataSource.query(
      `
        with candidates as (
          select id
          from outbox_events
          where status = 'PUBLISHED'
            and published_at is not null
            and published_at < $1
          order by published_at asc, id asc
          limit $2
          for update skip locked
        ),
        deleted as (
          delete from outbox_events outbox
          using candidates
          where outbox.id = candidates.id
          returning outbox.id
        )
        select count(*)::int as count from deleted
      `,
      [cutoff, batchSize]
    );

    return readDeletedCount(result);
  }

  /**
   * Удаляет старые записи идемпотентности order lifecycle consumer-а.
   *
   * Эта таблица не имеет статуса: наличие строки означает, что eventId уже был
   * принят к обработке. После удаления очень старый дубль теоретически может
   * быть обработан как новый, поэтому retention window должен быть длиннее
   * максимального срока хранения/replay Kafka-сообщений для соответствующих
   * topics.
   */
  private async deleteProcessedKafkaEventsBefore(
    cutoff: Date,
    batchSize: number
  ): Promise<number> {
    const result = await this.dataSource.query(
      `
        with candidates as (
          select id
          from processed_kafka_events
          where processed_at < $1
          order by processed_at asc, id asc
          limit $2
          for update skip locked
        ),
        deleted as (
          delete from processed_kafka_events processed
          using candidates
          where processed.id = candidates.id
          returning processed.id
        )
        select count(*)::int as count from deleted
      `,
      [cutoff, batchSize]
    );

    return readDeletedCount(result);
  }

  /**
   * Удаляет завершённые durable inbox-записи Kafka workers.
   *
   * `COMPLETED` означает, что prepare/effect cycle завершён и повторное
   * входное сообщение можно считать дублем. `PREPARED` нельзя удалять: оно
   * хранит результат, который нужен после crash между external side effect и
   * `markCompleted`. `PROCESSING` тоже нельзя удалять, пока lease/retry логика
   * может восстановить обработку.
   */
  private async deleteCompletedInboxBefore(
    cutoff: Date,
    batchSize: number
  ): Promise<number> {
    const result = await this.dataSource.query(
      `
        with candidates as (
          select consumer_name, event_id
          from kafka_consumer_inbox
          where status = 'COMPLETED'
            and completed_at is not null
            and completed_at < $1
          order by completed_at asc, consumer_name asc, event_id asc
          limit $2
          for update skip locked
        ),
        deleted as (
          delete from kafka_consumer_inbox inbox
          using candidates
          where inbox.consumer_name = candidates.consumer_name
            and inbox.event_id = candidates.event_id
          returning inbox.event_id
        )
        select count(*)::int as count from deleted
      `,
      [cutoff, batchSize]
    );

    return readDeletedCount(result);
  }

  /**
   * Читает положительное целое число из env-конфига.
   *
   * Неверные значения не отключают retention полностью и не приводят к
   * неожиданному сроку хранения: сервис логирует проблему и использует
   * fallback. Это важно для production, где ошибка в env не должна превращать
   * cleanup job в destructive operation.
   */
  private readPositiveInteger(name: string, fallback: number): number {
    const value = Number(this.config.get<string>(name, String(fallback)));

    if (!Number.isInteger(value) || value < 1) {
      this.logger.warn(
        {
          name,
          value,
          fallback
        },
        "Invalid retention configuration value, fallback is used"
      );
      return fallback;
    }

    return value;
  }
}

/**
 * Достаёт количество удалённых строк из результата `select count(*)`.
 *
 * PostgreSQL drivers часто возвращают числовые агрегаты строками, поэтому
 * нормализация к `number` централизована в одном месте.
 */
function readDeletedCount(result: Array<{ count: number | string }>): number {
  return Number(result[0]?.count ?? 0);
}
