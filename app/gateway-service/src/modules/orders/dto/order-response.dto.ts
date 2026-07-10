import { ApiProperty } from "@nestjs/swagger";

/**
 * Статус заказа, который gateway возвращает клиенту после создания заказа.
 *
 * Статус отражает текущее состояние заказа в order-service. Сразу после
 * создания заказ обычно находится в `PENDING`, а финальное состояние меняется
 * позже после обработки risk-service и payment-service.
 */
export enum OrderStatusDto {
  /** Заказ создан и ожидает асинхронной проверки риска и оплаты. */
  PENDING = "PENDING",

  /** Risk-service одобрил заказ, следующий этап - авторизация оплаты. */
  RISK_APPROVED = "RISK_APPROVED",

  /** Risk-service отклонил заказ, оплата не должна выполняться. */
  RISK_REJECTED = "RISK_REJECTED",

  /** Payment-service успешно авторизовал оплату заказа. */
  PAYMENT_AUTHORIZED = "PAYMENT_AUTHORIZED",

  /** Payment-service не смог авторизовать оплату заказа. */
  PAYMENT_FAILED = "PAYMENT_FAILED",

  /** Заказ подтверждён после успешной оплаты. */
  CONFIRMED = "CONFIRMED",

  /** Заказ отменён бизнес-правилом или пользовательской командой. */
  CANCELLED = "CANCELLED"
}

/**
 * Ответ gateway-service на успешное создание заказа.
 *
 * Gateway возвращает агрегированную информацию, полученную из order-service
 * по gRPC. Этот ответ не означает завершение всей событийной цепочки, потому
 * что risk/payment обработка выполняется асинхронно.
 */
export class OrderResponseDto {
  /**
   * Уникальный идентификатор заказа, созданный order-service.
   *
   * Используется клиентом для последующего чтения состояния заказа или
   * сопоставления событий в логах и трассировке.
   */
  @ApiProperty({
    description: "Уникальный идентификатор созданного заказа.",
    example: "f8f1d0c9-1b6d-4a9d-8e6f-4a7f9f9b2a10"
  })
  id!: string;

  /**
   * Текущий статус заказа.
   *
   * После HTTP-запроса заказ возвращается в состоянии, которое известно
   * order-service на момент ответа gateway.
   */
  @ApiProperty({
    description: "Текущий статус заказа.",
    enum: OrderStatusDto,
    example: OrderStatusDto.PENDING
  })
  status!: OrderStatusDto;

  /**
   * Идентификатор пользователя, для которого создан заказ.
   *
   * Значение повторяет входной `userId` и помогает клиенту проверить, что
   * ответ относится к ожидаемому пользователю.
   */
  @ApiProperty({
    description: "Идентификатор пользователя, для которого создан заказ.",
    example: "user-123"
  })
  userId!: string;

  /**
   * Валюта заказа.
   *
   * Используется для интерпретации суммарной стоимости и цен позиций.
   */
  @ApiProperty({
    description: "Трехбуквенный ISO-код валюты заказа.",
    example: "USD"
  })
  currency!: string;

  /**
   * Итоговая сумма заказа.
   *
   * Рассчитывается order-service как сумма `quantity * unitPrice` по всем
   * позициям заказа.
   */
  @ApiProperty({
    description: "Итоговая сумма заказа.",
    example: 39.98
  })
  totalAmount!: number;

  /**
   * Количество товарных позиций в заказе.
   *
   * Это количество строк заказа, а не сумма всех единиц товара.
   */
  @ApiProperty({
    description: "Количество товарных позиций в заказе.",
    example: 1
  })
  itemCount!: number;

  /**
   * Время создания заказа в ISO 8601.
   *
   * Значение формируется order-service и возвращается gateway без изменения.
   */
  @ApiProperty({
    description: "Дата и время создания заказа в формате ISO 8601.",
    example: "2026-06-05T16:45:07.123Z"
  })
  createdAt!: string;
}
