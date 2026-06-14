import type { IncomingMessage } from "node:http";
import { LoggerModule, type Params } from "nestjs-pino";
import type { Options as PinoHttpOptions } from "pino-http";
import { randomUUID } from "node:crypto";
import { getActiveTraceLogFields } from "./tracing";

export interface ServiceLoggerOptions {
  serviceName: string;
  environment: "local" | "prod" | string;
}

export const LOG_HEADER_NAMES = {
  correlationId: "x-correlation-id",
  requestId: "x-request-id"
} as const;

export function createServiceLoggerModule(options: ServiceLoggerOptions) {
  return LoggerModule.forRoot(createServiceLoggerParams(options));
}

export function createServiceLoggerParams(options: ServiceLoggerOptions): Params {
  const isLocal = options.environment === "local";

  return {
    pinoHttp: {
      level: process.env.LOG_LEVEL ?? (isLocal ? "debug" : "info"),
      mixin: () => getActiveTraceLogFields(),
      genReqId: (request) =>
        getHeader(request, LOG_HEADER_NAMES.correlationId) ??
        getHeader(request, LOG_HEADER_NAMES.requestId) ??
        randomUUID(),
      customProps: (request) => ({
        service: options.serviceName,
        environment: options.environment,
        correlationId:
          getHeader(request, LOG_HEADER_NAMES.correlationId) ??
          getHeader(request, LOG_HEADER_NAMES.requestId)
      }),
      customSuccessMessage: (request, response) =>
        `${request.method} ${request.url} ${response.statusCode}`,
      customErrorMessage: (request, response) =>
        `${request.method} ${request.url} ${response.statusCode}`,
      serializers: {
        req(request) {
          return {
            id: request.id,
            method: request.method,
            url: request.url,
            remoteAddress: request.remoteAddress,
            remotePort: request.remotePort
          };
        },
        res(response) {
          return {
            statusCode: response.statusCode
          };
        }
      },
      transport: isLocal
        ? {
            target: "pino-pretty",
            options: {
              colorize: true,
              singleLine: true,
              translateTime: "SYS:standard"
            }
          }
        : undefined
    } satisfies PinoHttpOptions
  };
}

export function getHeader(request: IncomingMessage, name: string): string | undefined {
  const value = request.headers[name];

  if (Array.isArray(value)) {
    return value[0];
  }

  return value;
}

export function logServiceStarted(
  logger: { log(message: unknown, context?: string): void },
  params: {
    serviceName: string;
    transport: "http" | "grpc" | "worker";
    host?: string;
    port?: number;
    environment: string;
    url?: string;
    docsUrl?: string;
    grpcUrl?: string;
  }
): void {
  logger.log({
    message: "Service started",
    service: params.serviceName,
    transport: params.transport,
    host: params.host,
    port: params.port,
    environment: params.environment,
    url: params.url,
    docsUrl: params.docsUrl,
    grpcUrl: params.grpcUrl,
    context: "Bootstrap"
  });
}
