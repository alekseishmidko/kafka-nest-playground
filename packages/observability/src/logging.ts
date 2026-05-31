import type { IncomingMessage } from "node:http";
import { LoggerModule, type Params } from "nestjs-pino";
import type { Options as PinoHttpOptions } from "pino-http";
import { randomUUID } from "node:crypto";

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
  logger: { log(message: string, context?: string): void },
  params: {
    serviceName: string;
    host: string;
    port: number;
    environment: string;
    grpcUrl?: string;
  }
): void {
  logger.log(
    JSON.stringify({
      message: "Service started",
      service: params.serviceName,
      host: params.host,
      port: params.port,
      environment: params.environment,
      grpcUrl: params.grpcUrl
    }),
    "Bootstrap"
  );
}
