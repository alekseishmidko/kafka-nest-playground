import { Inject, Injectable } from "@nestjs/common";
import { EVENT_SCHEMA_SUBJECTS } from "@kafka-playground/contracts";
import { buildKafkaHeaders } from "./kafka-headers";
import { KafkaEventLogger } from "./kafka-logger";
import { KAFKA_PRODUCER_CLIENT, SCHEMA_REGISTRY_CODEC } from "./kafka.tokens";
import type {
  KafkaProducerClient,
  KafkaPublishOptions,
  SchemaRegistryCodec
} from "./types";

@Injectable()
export class KafkaProducerService {
  constructor(
    @Inject(KAFKA_PRODUCER_CLIENT)
    private readonly producer: KafkaProducerClient,
    @Inject(SCHEMA_REGISTRY_CODEC)
    private readonly codec: SchemaRegistryCodec,
    private readonly logger: KafkaEventLogger
  ) {}

  async publish(options: KafkaPublishOptions): Promise<void> {
    const value = await this.codec.serialize(this.getSubject(options), options.event);
    const headers = buildKafkaHeaders(options.event, {
      ...options.headers,
      "x-correlation-id": options.correlationId ?? options.event.correlationId,
      "x-causation-id": options.causationId ?? options.event.causationId ?? undefined
    });

    const metadata = await this.producer.send({
      topic: options.topic,
      messages: [
        {
          key: options.key,
          value,
          headers
        }
      ]
    });

    const firstRecord = metadata[0];

    this.logger.logProduced({
      topic: firstRecord?.topicName ?? firstRecord?.topic ?? options.topic,
      partition: firstRecord?.partition,
      offset: firstRecord?.offset,
      event: options.event
    });
  }

  private getSubject(options: KafkaPublishOptions): string {
    return EVENT_SCHEMA_SUBJECTS[options.event.eventType] ?? `${options.topic}-value`;
  }
}
