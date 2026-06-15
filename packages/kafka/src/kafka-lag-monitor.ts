import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  Optional
} from "@nestjs/common";
import { ApplicationMetrics } from "@kafka-playground/observability";
import { Kafka, logLevel, type Admin } from "kafkajs";
import { KafkaConsumerRunner } from "./consumer/kafka-consumer-runner";
import { KAFKA_MODULE_OPTIONS } from "./kafka.tokens";
import type { KafkaModuleOptions } from "./types";

/**
 * Периодически вычисляет lag consumer group через Kafka Admin API.
 *
 * Lag partition-а равен разнице между latest offset и committed offset.
 * Если group ещё не имеет committed offset (`-1`), расчёт начинается с low
 * offset topic-а, чтобы gauge отражал реальное число доступных сообщений.
 */
@Injectable()
export class KafkaLagMonitor
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(KafkaLagMonitor.name);
  private readonly intervalMs = 15_000;
  private admin: Admin | null = null;
  private timer: NodeJS.Timeout | null = null;
  private collecting = false;

  constructor(
    @Inject(KAFKA_MODULE_OPTIONS)
    private readonly options: KafkaModuleOptions,
    private readonly runner: KafkaConsumerRunner,
    @Optional()
    private readonly metrics?: ApplicationMetrics
  ) {}

  onApplicationBootstrap(): void {
    if (!this.metrics || !this.options.consumerGroupId) {
      return;
    }

    this.admin = new Kafka({
      clientId: `${this.options.clientId}-lag-monitor`,
      brokers: this.options.brokers,
      logLevel: logLevel.NOTHING
    }).admin();

    void this.collect();
    this.timer = setInterval(() => {
      void this.collect();
    }, this.intervalMs);
  }

  async onApplicationShutdown(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    await this.admin?.disconnect().catch(() => undefined);
  }

  /**
   * Обновляет gauge для каждой partition всех зарегистрированных topics.
   */
  async collect(): Promise<void> {
    if (
      this.collecting ||
      !this.admin ||
      !this.metrics ||
      !this.options.consumerGroupId
    ) {
      return;
    }

    this.collecting = true;

    try {
      await this.admin.connect();
      const topics = this.runner.getSubscriptionTopics();
      const committed = await this.admin.fetchOffsets({
        groupId: this.options.consumerGroupId,
        topics
      });

      for (const topicOffsets of committed) {
        const latest = await this.admin.fetchTopicOffsets(
          topicOffsets.topic
        );
        const latestByPartition = new Map(
          latest.map((partition) => [partition.partition, partition])
        );

        for (const partition of topicOffsets.partitions) {
          const bounds = latestByPartition.get(partition.partition);

          if (!bounds) {
            continue;
          }

          this.metrics.setConsumerLag({
            group: this.options.consumerGroupId,
            topic: topicOffsets.topic,
            partition: partition.partition,
            lag: calculateConsumerLag({
              high: bounds.high,
              low: bounds.low,
              committed: partition.offset
            })
          });
        }
      }
    } catch (error) {
      this.logger.warn({
        message: "Kafka consumer lag collection failed",
        groupId: this.options.consumerGroupId,
        error:
          error instanceof Error ? error.message : String(error)
      });
    } finally {
      this.collecting = false;
    }
  }
}

/**
 * Вычисляет lag без потери точности на Kafka offsets размером больше 32 bit.
 */
export function calculateConsumerLag(params: {
  high: string;
  low: string;
  committed: string;
}): number {
  const committedOffset =
    params.committed === "-1"
      ? BigInt(params.low)
      : BigInt(params.committed);
  const lag = maxBigInt(
    0n,
    BigInt(params.high) - committedOffset
  );

  return toSafeNumber(lag);
}

function maxBigInt(left: bigint, right: bigint): bigint {
  return left > right ? left : right;
}

function toSafeNumber(value: bigint): number {
  return value > BigInt(Number.MAX_SAFE_INTEGER)
    ? Number.MAX_SAFE_INTEGER
    : Number(value);
}
