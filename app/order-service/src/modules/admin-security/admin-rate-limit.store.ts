import { Socket, createConnection } from "node:net";

export const ADMIN_RATE_LIMIT_STORE = Symbol("ADMIN_RATE_LIMIT_STORE");

export interface AdminRateLimitIncrement {
  requests: number;
}

export interface AdminRateLimitStore {
  increment(key: string, windowMs: number): Promise<AdminRateLimitIncrement>;
}

interface RateBucket {
  windowStartedAt: number;
  requests: number;
}

/**
 * Process-local fixed-window storage для local/dev режима.
 *
 * Такой storage не подходит для нескольких replicas: каждая replica будет
 * считать лимит отдельно. Для production используйте Redis backend.
 */
export class InMemoryAdminRateLimitStore implements AdminRateLimitStore {
  private readonly buckets = new Map<string, RateBucket>();

  async increment(
    key: string,
    windowMs: number
  ): Promise<AdminRateLimitIncrement> {
    const now = Date.now();
    const bucket = this.buckets.get(key);

    if (!bucket || now - bucket.windowStartedAt >= windowMs) {
      this.buckets.set(key, {
        windowStartedAt: now,
        requests: 1
      });
      return { requests: 1 };
    }

    bucket.requests += 1;
    return { requests: bucket.requests };
  }
}

/**
 * Redis-backed fixed-window storage для нескольких order-service replicas.
 *
 * Redis выполняет `INCR` и `PEXPIRE` в одном Lua script-е, поэтому все replicas
 * видят один общий счётчик для fingerprint-а admin API key. Если Redis
 * недоступен, вызывающий guard должен fail-closed: лучше временно вернуть 429,
 * чем открыть admin endpoints без общего limiter-а.
 */
export class RedisAdminRateLimitStore implements AdminRateLimitStore {
  private readonly client: RedisCommandClient;
  private readonly keyPrefix: string;

  constructor(options: RedisAdminRateLimitStoreOptions) {
    this.client = new RedisCommandClient(options.url);
    this.keyPrefix = options.keyPrefix ?? "kafka-playground:admin-rate-limit";
  }

  async increment(
    key: string,
    windowMs: number
  ): Promise<AdminRateLimitIncrement> {
    const requests = await this.client.evalNumber(
      [
        "local current = redis.call('INCR', KEYS[1])",
        "if current == 1 then",
        "  redis.call('PEXPIRE', KEYS[1], ARGV[1])",
        "end",
        "return current"
      ].join("\n"),
      [`${this.keyPrefix}:${key}`],
      [String(windowMs)]
    );

    return { requests };
  }
}

interface RedisAdminRateLimitStoreOptions {
  url: string;
  keyPrefix?: string;
}

export function createAdminRateLimitStoreFromEnv(): AdminRateLimitStore {
  const backend = (process.env.ADMIN_RATE_LIMIT_BACKEND ?? "memory")
    .trim()
    .toLowerCase();

  if (backend === "redis") {
    return new RedisAdminRateLimitStore({
      url: readRedisUrl(),
      keyPrefix: process.env.ADMIN_RATE_LIMIT_REDIS_KEY_PREFIX
    });
  }

  return new InMemoryAdminRateLimitStore();
}

function readRedisUrl(): string {
  const explicitUrl =
    process.env.ADMIN_RATE_LIMIT_REDIS_URL ?? process.env.REDIS_URL;

  if (explicitUrl && explicitUrl.trim() !== "") {
    return explicitUrl;
  }

  return `redis://localhost:${process.env.REDIS_PORT ?? "6379"}`;
}

type RedisReply = string | number | null | RedisReply[];

interface PendingRedisCommand {
  resolve(reply: RedisReply): void;
  reject(error: Error): void;
}

class RedisCommandClient {
  private readonly url: URL;
  private socket: Socket | null = null;
  private connectPromise: Promise<void> | null = null;
  private buffer = "";
  private readonly pending: PendingRedisCommand[] = [];

  constructor(url: string) {
    this.url = new URL(url);

    if (this.url.protocol !== "redis:") {
      throw new Error("Only redis:// URLs are supported for admin rate limit");
    }
  }

  async evalNumber(
    script: string,
    keys: string[],
    args: string[]
  ): Promise<number> {
    const reply = await this.sendCommand([
      "EVAL",
      script,
      String(keys.length),
      ...keys,
      ...args
    ]);

    if (typeof reply !== "number") {
      throw new Error(`Unexpected Redis EVAL reply: ${String(reply)}`);
    }

    return reply;
  }

  private async sendCommand(command: string[]): Promise<RedisReply> {
    await this.ensureConnected();

    return new Promise((resolve, reject) => {
      this.pending.push({ resolve, reject });
      this.socket?.write(encodeRedisCommand(command));
    });
  }

  private async ensureConnected(): Promise<void> {
    if (this.socket && !this.socket.destroyed) {
      return;
    }

    if (this.connectPromise) {
      return this.connectPromise;
    }

    this.connectPromise = this.connect();

    try {
      await this.connectPromise;
    } finally {
      this.connectPromise = null;
    }
  }

  private connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const socket = createConnection({
        host: this.url.hostname,
        port: Number(this.url.port || 6379)
      });

      const onConnectError = (error: Error) => {
        socket.destroy();
        reject(error);
      };

      socket.once("error", onConnectError);
      socket.once("connect", () => {
        socket.off("error", onConnectError);
        this.socket = socket;
        socket.on("data", (chunk) => this.onData(chunk));
        socket.on("error", (error) => this.rejectPending(error));
        socket.on("close", () => {
          if (this.socket === socket) {
            this.socket = null;
          }
        });
        this.runConnectCommands().then(resolve, reject);
      });
    });
  }

  private async runConnectCommands(): Promise<void> {
    if (this.url.password) {
      const username = decodeURIComponent(this.url.username);
      const password = decodeURIComponent(this.url.password);
      await this.sendCommand(
        username ? ["AUTH", username, password] : ["AUTH", password]
      );
    }

    const database = this.url.pathname.replace("/", "");

    if (database) {
      await this.sendCommand(["SELECT", database]);
    }
  }

  private onData(chunk: Buffer): void {
    try {
      this.buffer += chunk.toString("utf8");

      while (this.pending.length > 0) {
        const parsed = parseRedisReply(this.buffer);

        if (!parsed) {
          return;
        }

        this.buffer = this.buffer.slice(parsed.nextOffset);
        const command = this.pending.shift();
        command?.resolve(parsed.reply);
      }
    } catch (error) {
      this.rejectPending(
        error instanceof Error ? error : new Error(String(error))
      );
      this.socket?.destroy();
    }
  }

  private rejectPending(error: Error): void {
    while (this.pending.length > 0) {
      this.pending.shift()?.reject(error);
    }
  }
}

function encodeRedisCommand(parts: string[]): string {
  return [
    `*${parts.length}`,
    ...parts.flatMap((part) => [`$${Buffer.byteLength(part)}`, part])
  ].join("\r\n") + "\r\n";
}

function parseRedisReply(
  input: string,
  offset = 0
): { reply: RedisReply; nextOffset: number } | null {
  const type = input[offset];

  if (!type) {
    return null;
  }

  if (type === "+" || type === "-" || type === ":") {
    const lineEnd = input.indexOf("\r\n", offset);

    if (lineEnd === -1) {
      return null;
    }

    const value = input.slice(offset + 1, lineEnd);

    if (type === "-") {
      throw new Error(`Redis error: ${value}`);
    }

    return {
      reply: type === ":" ? Number(value) : value,
      nextOffset: lineEnd + 2
    };
  }

  if (type === "$") {
    const lineEnd = input.indexOf("\r\n", offset);

    if (lineEnd === -1) {
      return null;
    }

    const length = Number(input.slice(offset + 1, lineEnd));

    if (length === -1) {
      return { reply: null, nextOffset: lineEnd + 2 };
    }

    const valueStart = lineEnd + 2;
    const valueEnd = valueStart + length;

    if (input.length < valueEnd + 2) {
      return null;
    }

    return {
      reply: input.slice(valueStart, valueEnd),
      nextOffset: valueEnd + 2
    };
  }

  if (type === "*") {
    const lineEnd = input.indexOf("\r\n", offset);

    if (lineEnd === -1) {
      return null;
    }

    const length = Number(input.slice(offset + 1, lineEnd));
    const items: RedisReply[] = [];
    let nextOffset = lineEnd + 2;

    for (let index = 0; index < length; index += 1) {
      const parsed = parseRedisReply(input, nextOffset);

      if (!parsed) {
        return null;
      }

      items.push(parsed.reply);
      nextOffset = parsed.nextOffset;
    }

    return {
      reply: items,
      nextOffset
    };
  }

  throw new Error(`Unsupported Redis reply type: ${type}`);
}
