import {
  Controller,
  DynamicModule,
  Get,
  Global,
  Header,
  Inject,
  Injectable,
  Module
} from "@nestjs/common";
import {
  collectDefaultMetrics,
  Counter,
  Gauge,
  Histogram,
  Registry
} from "prom-client";

const METRICS_OPTIONS = Symbol("METRICS_OPTIONS");

export interface MetricsModuleOptions {
  /** Стабильное имя сервиса, добавляемое ко всем прикладным метрикам. */
  serviceName: string;
  /** Включает стандартные метрики Node.js process/runtime. */
  collectDefaultMetrics?: boolean;
}

/**
 * Централизованный facade прикладных Prometheus-метрик.
 *
 * Feature-код не получает прямой доступ к `prom-client`. Благодаря этому имена
 * метрик, labels и правила кардинальности остаются единообразными. В labels
 * намеренно отсутствуют `eventId`, `orderId`, correlation ID и тексты ошибок:
 * уникальные значения быстро делают Prometheus дорогим и нестабильным.
 */
@Injectable()
export class ApplicationMetrics {
  private readonly registry = new Registry();
  private readonly consumed: Counter<"service" | "topic" | "event_type">;
  private readonly failed: Counter<
    "service" | "topic" | "event_type" | "error_code"
  >;
  private readonly retries: Counter<
    "service" | "original_topic" | "destination_topic" | "stage"
  >;
  private readonly dlq: Counter<
    "service" | "original_topic" | "error_code"
  >;
  private readonly processingDuration: Histogram<
    "service" | "topic" | "event_type" | "result"
  >;
  private readonly outboxPending: Gauge<"service" | "status">;
  private readonly outboxPendingTotal: Gauge<"service">;
  private readonly dlqNew: Gauge<"service">;
  private readonly consumerLag: Gauge<
    "service" | "group" | "topic" | "partition"
  >;
  private readonly outboxPublish: Counter<
    "service" | "topic" | "result"
  >;
  private readonly duplicates: Counter<
    "service" | "topic" | "event_type"
  >;

  constructor(
    @Inject(METRICS_OPTIONS)
    private readonly options: MetricsModuleOptions
  ) {
    this.registry.setDefaultLabels({
      service: options.serviceName
    });

    if (options.collectDefaultMetrics !== false) {
      collectDefaultMetrics({
        register: this.registry,
        prefix: "nodejs_"
      });
    }

    this.consumed = new Counter({
      name: "kafka_events_consumed_total",
      help: "Количество успешно переданных handler-у Kafka-событий.",
      labelNames: ["service", "topic", "event_type"],
      registers: [this.registry]
    });
    this.failed = new Counter({
      name: "kafka_events_failed_total",
      help: "Количество ошибок обработки Kafka-событий.",
      labelNames: ["service", "topic", "event_type", "error_code"],
      registers: [this.registry]
    });
    this.retries = new Counter({
      name: "kafka_retry_events_total",
      help: "Количество сообщений, опубликованных на следующий retry-этап.",
      labelNames: [
        "service",
        "original_topic",
        "destination_topic",
        "stage"
      ],
      registers: [this.registry]
    });
    this.dlq = new Counter({
      name: "kafka_dlq_events_total",
      help: "Количество событий, направленных или сохранённых в DLQ.",
      labelNames: ["service", "original_topic", "error_code"],
      registers: [this.registry]
    });
    this.processingDuration = new Histogram({
      name: "kafka_event_processing_duration_seconds",
      help: "Полная длительность обработки Kafka-события handler-ом.",
      labelNames: ["service", "topic", "event_type", "result"],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15],
      registers: [this.registry]
    });
    this.outboxPending = new Gauge({
      name: "outbox_events",
      help: "Текущее количество outbox-записей по рабочим статусам.",
      labelNames: ["service", "status"],
      registers: [this.registry]
    });
    this.outboxPendingTotal = new Gauge({
      name: "outbox_pending_events",
      help: "Текущее количество outbox-записей в статусе PENDING.",
      labelNames: ["service"],
      registers: [this.registry]
    });
    this.dlqNew = new Gauge({
      name: "dlq_new_events",
      help: "Текущее количество DLQ-записей в статусе NEW.",
      labelNames: ["service"],
      registers: [this.registry]
    });
    this.consumerLag = new Gauge({
      name: "kafka_consumer_lag",
      help: "Разница между latest offset topic-а и committed offset consumer group.",
      labelNames: ["service", "group", "topic", "partition"],
      registers: [this.registry]
    });
    this.outboxPublish = new Counter({
      name: "outbox_publish_attempts_total",
      help: "Количество попыток публикации transactional outbox.",
      labelNames: ["service", "topic", "result"],
      registers: [this.registry]
    });
    this.duplicates = new Counter({
      name: "kafka_duplicate_events_total",
      help: "Количество Kafka-событий, пропущенных durable inbox как дубли.",
      labelNames: ["service", "topic", "event_type"],
      registers: [this.registry]
    });
  }

  recordKafkaConsumed(topic: string, eventType: string): void {
    this.consumed.inc({
      service: this.options.serviceName,
      topic,
      event_type: eventType
    });
  }

  recordKafkaFailed(
    topic: string,
    eventType: string,
    errorCode: string
  ): void {
    this.failed.inc({
      service: this.options.serviceName,
      topic,
      event_type: eventType,
      error_code: errorCode
    });
  }

  recordKafkaDuplicate(topic: string, eventType: string): void {
    this.duplicates.inc({
      service: this.options.serviceName,
      topic,
      event_type: eventType
    });
  }

  recordKafkaRetry(params: {
    originalTopic: string;
    destinationTopic: string;
    stage: string;
  }): void {
    this.retries.inc({
      service: this.options.serviceName,
      original_topic: params.originalTopic,
      destination_topic: params.destinationTopic,
      stage: params.stage
    });
  }

  recordKafkaDlq(originalTopic: string, errorCode: string): void {
    this.dlq.inc({
      service: this.options.serviceName,
      original_topic: originalTopic,
      error_code: errorCode
    });
  }

  observeKafkaProcessing(params: {
    topic: string;
    eventType: string;
    result: "success" | "failure";
    durationSeconds: number;
  }): void {
    this.processingDuration.observe(
      {
        service: this.options.serviceName,
        topic: params.topic,
        event_type: params.eventType,
        result: params.result
      },
      params.durationSeconds
    );
  }

  setOutboxCount(status: string, count: number): void {
    this.outboxPending.set(
      {
        service: this.options.serviceName,
        status
      },
      count
    );

    if (status === "PENDING") {
      this.outboxPendingTotal.set(
        { service: this.options.serviceName },
        count
      );
    }
  }

  recordOutboxPublish(topic: string, result: "success" | "failure"): void {
    this.outboxPublish.inc({
      service: this.options.serviceName,
      topic,
      result
    });
  }

  setDlqNewCount(count: number): void {
    this.dlqNew.set({ service: this.options.serviceName }, count);
  }

  setConsumerLag(params: {
    group: string;
    topic: string;
    partition: number;
    lag: number;
  }): void {
    this.consumerLag.set(
      {
        service: this.options.serviceName,
        group: params.group,
        topic: params.topic,
        partition: String(params.partition)
      },
      params.lag
    );
  }

  /**
   * Возвращает Prometheus text exposition format.
   */
  async render(): Promise<string> {
    return this.registry.metrics();
  }

  get contentType(): string {
    return this.registry.contentType;
  }
}

/**
 * Технический endpoint для Prometheus scrape.
 */
@Controller("metrics")
export class MetricsController {
  constructor(private readonly metrics: ApplicationMetrics) {}

  @Get()
  @Header("Cache-Control", "no-store")
  @Header(
    "Content-Type",
    "text/plain; version=0.0.4; charset=utf-8"
  )
  async getMetrics(): Promise<string> {
    return this.metrics.render();
  }
}

@Global()
@Module({})
export class MetricsModule {
  static register(options: MetricsModuleOptions): DynamicModule {
    return {
      global: true,
      module: MetricsModule,
      providers: [
        {
          provide: METRICS_OPTIONS,
          useValue: options
        },
        ApplicationMetrics
      ],
      controllers: [MetricsController],
      exports: [ApplicationMetrics]
    };
  }
}
