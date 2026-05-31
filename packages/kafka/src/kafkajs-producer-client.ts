import { Kafka, type Producer } from "kafkajs";
import type {
  KafkaProducerClient,
  KafkaProducerClientMessage,
  KafkaProducerRecordMetadata
} from "./types";

export interface KafkaJsProducerClientOptions {
  clientId: string;
  brokers: string[];
}

export class KafkaJsProducerClient implements KafkaProducerClient {
  private readonly producer: Producer;
  private connectPromise: Promise<void> | null = null;

  constructor(options: KafkaJsProducerClientOptions) {
    this.producer = new Kafka({
      clientId: options.clientId,
      brokers: options.brokers
    }).producer();
  }

  async send(message: KafkaProducerClientMessage): Promise<KafkaProducerRecordMetadata[]> {
    await this.connect();

    return this.producer.send(message);
  }

  private async connect(): Promise<void> {
    this.connectPromise ??= this.producer.connect();

    return this.connectPromise;
  }
}
