import { KafkaNonRetryableError } from "@kafka-playground/kafka";

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Проверяет идентификатор заказа до выполнения SQL-запроса.
 *
 * Колонка `orders.id` имеет PostgreSQL-тип `uuid`. Если передать строку вроде
 * `test-order-id-2`, PostgreSQL завершит запрос `22P02`, а consumer будет считать
 * это временной ошибкой инфраструктуры. Явная проверка дает понятный код ошибки
 * и позволяет сразу направить неисправимое сообщение в DLQ.
 */
export function assertValidOrderId(orderId: string): void {
  if (!UUID_PATTERN.test(orderId)) {
    throw new KafkaNonRetryableError(
      "INVALID_ORDER_ID",
      `Order id must be a UUID, received: ${JSON.stringify(orderId)}`
    );
  }
}
