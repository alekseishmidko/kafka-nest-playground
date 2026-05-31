import { Inject, Injectable } from "@nestjs/common";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { KAFKA_MODULE_OPTIONS } from "./kafka.tokens";
import type { KafkaModuleOptions, SchemaRegistryCodec } from "./types";

@Injectable()
export class SchemaRegistryAvroCodec implements SchemaRegistryCodec {
  private readonly registry: SchemaRegistry;
  private readonly schemaIdBySubject = new Map<string, number>();

  constructor(
    @Inject(KAFKA_MODULE_OPTIONS)
    options: KafkaModuleOptions
  ) {
    this.registry = new SchemaRegistry({
      host: options.schemaRegistryUrl
    });
  }

  async serialize(subject: string, payload: unknown): Promise<Buffer> {
    const schemaId = await this.getLatestSchemaId(subject);

    return this.registry.encode(schemaId, payload);
  }

  async deserialize<T>(payload: Buffer): Promise<T> {
    return this.registry.decode(payload) as Promise<T>;
  }

  private async getLatestSchemaId(subject: string): Promise<number> {
    const cached = this.schemaIdBySubject.get(subject);

    if (cached !== undefined) {
      return cached;
    }

    const schemaId = await this.registry.getLatestSchemaId(subject);
    this.schemaIdBySubject.set(subject, schemaId);

    return schemaId;
  }
}
