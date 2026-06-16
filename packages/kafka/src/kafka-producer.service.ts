import { Inject, Injectable } from "@nestjs/common";
import {
  injectTraceContext,
  runInTraceSpan,
  SpanKind
} from "@kafka-playground/observability";
import { buildKafkaHeaders } from "./kafka-headers";
import { KafkaEventLogger } from "./kafka-logger";
import {
  KAFKA_MODULE_OPTIONS,
  KAFKA_PRODUCER_CLIENT,
  SCHEMA_REGISTRY_CODEC
} from "./kafka.tokens";
import type {
  KafkaProducerClient,
  KafkaModuleOptions,
  KafkaPublishOptions,
  SchemaRegistryCodec
} from "./types";

/**
 * Высокоуровневый producer для доменных событий.
 *
 * Сервис не привязан к конкретному набору eventType. Subject для Schema
 * Registry вычисляется через `KafkaModuleOptions.subjectResolver`, а если
 * resolver не задан, используется универсальное имя
 * `${topic}-${eventType}-value`.
 */
@Injectable()
export class KafkaProducerService {
  constructor(
    @Inject(KAFKA_MODULE_OPTIONS)
    private readonly options: KafkaModuleOptions,
    @Inject(KAFKA_PRODUCER_CLIENT)
    private readonly producer: KafkaProducerClient,
    @Inject(SCHEMA_REGISTRY_CODEC)
    private readonly codec: SchemaRegistryCodec,
    private readonly logger: KafkaEventLogger
  ) {}

  async publish(options: KafkaPublishOptions): Promise<void> {
    return runInTraceSpan(
      `${options.topic} publish`,
      {
        kind: SpanKind.PRODUCER,
        attributes: {
          "messaging.system": "kafka",
          "messaging.destination.name": options.topic,
          "messaging.operation.name": "publish",
          "messaging.message.id": options.event.eventId,
          "messaging.kafka.message.key": options.key,
          "event.type": options.event.eventType
        }
      },
      async () => {
        const value = await this.codec.serialize(
          this.getSubject(options),
          options.event
        );
        const traceHeaders = injectTraceContext({});
        const headers = buildKafkaHeaders(options.event, {
          ...options.headers,
          ...traceHeaders,
          "x-correlation-id":
            options.correlationId ?? options.event.correlationId,
          "x-causation-id":
            options.causationId ??
            options.event.causationId ??
            undefined
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
          topic:
            firstRecord?.topicName ??
            firstRecord?.topic ??
            options.topic,
          partition: firstRecord?.partition,
          offset: firstRecord?.offset,
          event: options.event
        });
      }
    );
  }

  private getSubject(options: KafkaPublishOptions): string {
    return (
      this.options.subjectResolver?.({
        topic: options.topic,
        eventType: options.event.eventType,
        event: options.event
      }) ?? `${options.topic}-${options.event.eventType}-value`
    );
  }
}
