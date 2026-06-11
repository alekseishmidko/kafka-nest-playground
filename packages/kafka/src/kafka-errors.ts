/**
 * Ошибка, повторная обработка которой не может привести к успеху.
 *
 * Примеры: поврежденный бизнес-идентификатор, отсутствующее обязательное поле
 * или несовместимая версия payload. Такие ошибки сразу направляются в DLQ,
 * потому что ожидание 5 секунд, 30 секунд и 5 минут не изменит сообщение.
 */
export class KafkaNonRetryableError extends Error {
  constructor(
    public readonly errorCode: string,
    message: string,
    options?: ErrorOptions
  ) {
    super(message, options);
    this.name = "KafkaNonRetryableError";
  }
}
