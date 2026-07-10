import { ApiProperty } from "@nestjs/swagger";
import { IsString, Length } from "class-validator";

/**
 * Публичная команда отмены заказа через gateway.
 */
export class CancelOrderDto {
  /**
   * Причина отмены, которую order-service сохранит в событиях
   * `OrderCancellationRequested` и `OrderCancelled`/`OrderCancellationRejected`.
   */
  @ApiProperty({
    description: "Причина отмены заказа.",
    example: "Пользователь отменил заказ до оплаты",
    minLength: 5,
    maxLength: 1000
  })
  @IsString()
  @Length(5, 1000)
  reason!: string;
}

export enum CancellationStatusDto {
  ACCEPTED = "ACCEPTED",
  REJECTED = "REJECTED"
}

/**
 * Ответ на команду отмены заказа.
 */
export class CancelOrderResponseDto {
  @ApiProperty({
    description: "Идентификатор заказа.",
    example: "f8f1d0c9-1b6d-4a9d-8e6f-4a7f9f9b2a10"
  })
  id!: string;

  @ApiProperty({
    description: "Текущий статус заказа после обработки команды.",
    example: "CANCELLED"
  })
  status!: string;

  @ApiProperty({
    description: "Итог обработки команды отмены.",
    enum: CancellationStatusDto
  })
  cancellationStatus!: CancellationStatusDto;

  @ApiProperty({
    description: "Причина отмены из запроса."
  })
  reason!: string;

  @ApiProperty({
    description: "Кто запросил отмену.",
    example: "user"
  })
  requestedBy!: "user" | "operator";

  @ApiProperty({
    description: "Статус, относительно которого принято решение."
  })
  currentStatus!: string;
}
