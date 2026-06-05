import { Injectable, Logger } from "@nestjs/common";
import type { DomainEvent } from "@kafka-playground/contracts";

@Injectable()
export class KafkaEventLogger {
  private readonly logger = new Logger(KafkaEventLogger.name);

  logProduced(params: {
    topic: string;
    partition?: number;
    offset?: string;
    event: DomainEvent;
  }): void {
    this.logger.log({
      message: "Kafka event produced",
      topic: params.topic,
      partition: params.partition,
      offset: params.offset,
      eventType: params.event.eventType,
      eventId: params.event.eventId,
      correlationId: params.event.correlationId
    });
  }

  logConsumed(params: {
    topic: string;
    partition: number;
    offset: string;
    event: DomainEvent;
  }): void {
    this.logger.log({
      message: "Kafka event consumed",
      topic: params.topic,
      partition: params.partition,
      offset: params.offset,
      eventType: params.event.eventType,
      eventId: params.event.eventId,
      correlationId: params.event.correlationId
    });
  }

  logFailed(params: {
    topic: string;
    partition: number;
    offset: string;
    eventType?: string;
    eventId?: string;
    error: unknown;
  }): void {
    const error = params.error instanceof Error ? params.error : new Error(String(params.error));

    this.logger.error(
      {
        message: "Kafka event handling failed",
        topic: params.topic,
        partition: params.partition,
        offset: params.offset,
        eventType: params.eventType,
        eventId: params.eventId,
        errorMessage: error.message
      },
      error.stack
    );
  }

  logConsumerStartFailed(params: {
    topics: string[];
    retryInMs: number;
    error: unknown;
  }): void {
    const error = params.error instanceof Error ? params.error : new Error(String(params.error));

    this.logger.warn(
      {
        message: "Kafka consumer startup failed",
        topics: params.topics,
        retryInMs: params.retryInMs,
        errorMessage: error.message
      }
    );
  }
}
