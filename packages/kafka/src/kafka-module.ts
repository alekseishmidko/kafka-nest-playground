import { DynamicModule, Global, Module, Provider, Type } from "@nestjs/common";
import { KafkaConsumerRunner } from "./kafka-consumer-runner";
import { KafkaEventLogger } from "./kafka-logger";
import { KafkaProducerService } from "./kafka-producer.service";
import { KafkaRetryDispatcher } from "./kafka-retry-dispatcher";
import { KafkaRetryPolicy } from "./kafka-retry-policy";
import { KafkaLagMonitor } from "./kafka-lag-monitor";
import {
  KafkaIdempotentEventProcessor,
  type KafkaInboxStore
} from "./kafka-consumer-inbox";
import {
  KAFKA_CONSUMER_CLIENT,
  KAFKA_INBOX_STORE,
  KAFKA_MODULE_OPTIONS,
  KAFKA_PRODUCER_CLIENT,
  SCHEMA_REGISTRY_CODEC
} from "./kafka.tokens";
import { SchemaRegistryAvroCodec } from "./schema-registry-avro-codec";
import type { KafkaConsumerClient, KafkaModuleOptions, KafkaProducerClient } from "./types";

export interface KafkaModuleRegisterOptions extends KafkaModuleOptions {
  producerClient?: KafkaProducerClient;
  consumerClient?: KafkaConsumerClient;
  /** Durable inbox обязателен только сервисам, использующим idempotent processor. */
  inboxStore?: KafkaInboxStore;
}

export interface KafkaModuleAsyncOptions {
  imports?: DynamicModule["imports"];
  inject?: Array<string | symbol | Type<unknown>>;
  useFactory: (
    ...args: unknown[]
  ) => KafkaModuleRegisterOptions | Promise<KafkaModuleRegisterOptions>;
}

@Global()
@Module({})
export class KafkaModule {
  static register(options: KafkaModuleRegisterOptions): DynamicModule {
    const providers: Provider[] = [
      {
        provide: KAFKA_MODULE_OPTIONS,
        useValue: options
      },
      {
        provide: KAFKA_PRODUCER_CLIENT,
        useValue: options.producerClient ?? createMissingProducerClient()
      },
      {
        provide: KAFKA_CONSUMER_CLIENT,
        useValue: options.consumerClient ?? createMissingConsumerClient()
      },
      {
        provide: KAFKA_INBOX_STORE,
        useValue: options.inboxStore ?? createMissingInboxStore()
      },
      {
        provide: SCHEMA_REGISTRY_CODEC,
        useClass: SchemaRegistryAvroCodec
      },
      KafkaEventLogger,
      KafkaProducerService,
      KafkaRetryPolicy,
      KafkaRetryDispatcher,
      KafkaConsumerRunner,
      KafkaLagMonitor,
      KafkaIdempotentEventProcessor
    ];

    return {
      module: KafkaModule,
      providers,
      exports: [
        KAFKA_MODULE_OPTIONS,
        KAFKA_PRODUCER_CLIENT,
        KAFKA_CONSUMER_CLIENT,
        KAFKA_INBOX_STORE,
        SCHEMA_REGISTRY_CODEC,
        KafkaProducerService,
        KafkaRetryPolicy,
        KafkaRetryDispatcher,
        KafkaConsumerRunner,
        KafkaLagMonitor,
        KafkaIdempotentEventProcessor
      ]
    };
  }

  static registerAsync(options: KafkaModuleAsyncOptions): DynamicModule {
    const providers: Provider[] = [
      {
        provide: KAFKA_MODULE_OPTIONS,
        inject: options.inject ?? [],
        useFactory: options.useFactory
      },
      {
        provide: KAFKA_PRODUCER_CLIENT,
        inject: [KAFKA_MODULE_OPTIONS],
        useFactory: (moduleOptions: KafkaModuleRegisterOptions) =>
          moduleOptions.producerClient ?? createMissingProducerClient()
      },
      {
        provide: KAFKA_CONSUMER_CLIENT,
        inject: [KAFKA_MODULE_OPTIONS],
        useFactory: (moduleOptions: KafkaModuleRegisterOptions) =>
          moduleOptions.consumerClient ?? createMissingConsumerClient()
      },
      {
        provide: KAFKA_INBOX_STORE,
        inject: [KAFKA_MODULE_OPTIONS],
        useFactory: (moduleOptions: KafkaModuleRegisterOptions) =>
          moduleOptions.inboxStore ?? createMissingInboxStore()
      },
      {
        provide: SCHEMA_REGISTRY_CODEC,
        useClass: SchemaRegistryAvroCodec
      },
      KafkaEventLogger,
      KafkaProducerService,
      KafkaRetryPolicy,
      KafkaRetryDispatcher,
      KafkaConsumerRunner,
      KafkaLagMonitor,
      KafkaIdempotentEventProcessor
    ];

    return {
      module: KafkaModule,
      imports: options.imports,
      providers,
      exports: [
        KAFKA_MODULE_OPTIONS,
        KAFKA_PRODUCER_CLIENT,
        KAFKA_CONSUMER_CLIENT,
        KAFKA_INBOX_STORE,
        SCHEMA_REGISTRY_CODEC,
        KafkaProducerService,
        KafkaRetryPolicy,
        KafkaRetryDispatcher,
        KafkaConsumerRunner,
        KafkaLagMonitor,
        KafkaIdempotentEventProcessor
      ]
    };
  }
}

function createMissingInboxStore(): KafkaInboxStore {
  const missing = async (): Promise<never> => {
    throw new Error("Kafka inbox store is not configured");
  };

  return {
    claim: missing,
    savePrepared: missing,
    markCompleted: missing,
    release: missing
  };
}

function createMissingProducerClient(): KafkaProducerClient {
  return {
    async send(): Promise<never> {
      throw new Error("Kafka producer client is not configured");
    }
  };
}

function createMissingConsumerClient(): KafkaConsumerClient {
  return {
    async subscribe(): Promise<never> {
      throw new Error("Kafka consumer client is not configured");
    },
    async run(): Promise<never> {
      throw new Error("Kafka consumer client is not configured");
    }
  };
}
