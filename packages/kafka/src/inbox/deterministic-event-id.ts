import { createHash } from "node:crypto";

/**
 * Строит стабильный UUID для результата обработки входного события.
 *
 * Одинаковые `namespace` и `sourceEventId` всегда дают одинаковый идентификатор.
 * Это важно для crash recovery: повторная публикация подготовленного результата
 * распознаётся downstream consumer-ом как тот же event, а не как новое событие.
 */
export function createDeterministicEventId(
  namespace: string,
  sourceEventId: string
): string {
  const bytes = createHash("sha256")
    .update(namespace)
    .update("\0")
    .update(sourceEventId)
    .digest()
    .subarray(0, 16);

  // Версия 5 и RFC 4122 variant делают результат корректным UUID.
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");

  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20)
  ].join("-");
}
