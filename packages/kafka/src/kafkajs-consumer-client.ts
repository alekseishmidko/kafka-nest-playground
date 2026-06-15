import { Kafka, logLevel, type Consumer, type IHeaders } from "kafkajs";
import type { KafkaConsumerClient, KafkaEachMessagePayload, KafkaHeaders } from "./types";

export interface KafkaJsConsumerClientOptions {
  clientId: string;
  brokers: string[];
  groupId: string;
}

export class KafkaJsConsumerClient implements KafkaConsumerClient {
  private readonly consumer: Consumer;
  private connectPromise: Promise<void> | null = null;

  constructor(options: KafkaJsConsumerClientOptions) {
    this.consumer = new Kafka({
      clientId: options.clientId,
      brokers: options.brokers,
      logLevel: logLevel.NOTHING
    }).consumer({
      groupId: options.groupId
    });
  }

  async subscribe(options: { topic: string; fromBeginning?: boolean }): Promise<void> {
    await this.connect();

    await this.consumer.subscribe({
      topic: options.topic,
      fromBeginning: options.fromBeginning
    });
  }

  async run(options: {
    eachMessage(message: KafkaEachMessagePayload): Promise<void>;
  }): Promise<void> {
    await this.connect();

    await this.consumer.run({
      // Частый commit уменьшает объём повторной доставки после аварийного
      // завершения. Durable inbox всё равно остаётся обязательной защитой:
      // commit offset и бизнес-side effect не являются одной транзакцией.
      autoCommit: true,
      autoCommitInterval: 1_000,
      autoCommitThreshold: 1,
      eachMessage: async ({ topic, partition, message, heartbeat }) => {
        await options.eachMessage({
          topic: topic as KafkaEachMessagePayload["topic"],
          partition,
          heartbeat,
          message: {
            key: message.key,
            value: message.value,
            offset: message.offset,
            headers: normalizeHeaders(message.headers)
          }
        });
      }
    });
  }

  /**
   * Инициирует штатный выход из consumer group.
   */
  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
    this.connectPromise = null;
  }

  private async connect(): Promise<void> {
    this.connectPromise ??= this.consumer.connect().catch((error: unknown) => {
      this.connectPromise = null;
      throw error;
    });

    return this.connectPromise;
  }
}

function normalizeHeaders(headers: IHeaders | undefined): KafkaHeaders | undefined {
  if (!headers) {
    return undefined;
  }

  return Object.fromEntries(
    Object.entries(headers)
      .map(([key, value]) => [key, Array.isArray(value) ? value[0] : value] as const)
      .filter((entry): entry is readonly [string, string | Buffer] => entry[1] !== undefined)
  );
}
