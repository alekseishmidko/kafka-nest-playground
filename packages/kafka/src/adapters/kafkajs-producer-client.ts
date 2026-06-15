import { Kafka, logLevel, Partitioners, type Producer } from "kafkajs";
import type {
  KafkaProducerClient,
  KafkaProducerClientMessage,
  KafkaProducerRecordMetadata
} from "../types";

/**
 * Параметры низкоуровневого KafkaJS producer adapter-а.
 */
export interface KafkaJsProducerClientOptions {
  clientId: string;
  brokers: string[];
}

/**
 * Адаптирует KafkaJS Producer к внутреннему `KafkaProducerClient`.
 *
 * Соединение открывается лениво при первой публикации и переиспользуется между
 * вызовами. Ошибка подключения сбрасывает promise, разрешая следующую попытку.
 */
export class KafkaJsProducerClient implements KafkaProducerClient {
  private readonly producer: Producer;
  private connectPromise: Promise<void> | null = null;

  constructor(options: KafkaJsProducerClientOptions) {
    this.producer = new Kafka({
      clientId: options.clientId,
      brokers: options.brokers,
      logLevel: logLevel.NOTHING
    }).producer({
      createPartitioner: Partitioners.LegacyPartitioner
    });
  }

  /**
   * Публикует подготовленную запись и возвращает нормализованную metadata.
   */
  async send(message: KafkaProducerClientMessage): Promise<KafkaProducerRecordMetadata[]> {
    await this.connect();

    return this.producer.send(message);
  }

  private async connect(): Promise<void> {
    this.connectPromise ??= this.producer.connect().catch((error: unknown) => {
      this.connectPromise = null;
      throw error;
    });

    return this.connectPromise;
  }
}
