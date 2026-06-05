import { Type } from "class-transformer";
import {
  ArrayMinSize,
  IsArray,
  IsInt,
  IsNumber,
  IsString,
  Length,
  Matches,
  Min,
  ValidateNested
} from "class-validator";
import { ApiProperty } from "@nestjs/swagger";

/**
 * Позиция заказа, которую клиент передает через gateway-service.
 *
 * Одна позиция описывает конкретный товар, количество единиц товара и цену
 * одной единицы в валюте заказа. Gateway валидирует эту структуру до отправки
 * команды во внутренний order-service по gRPC.
 */
export class CreateOrderItemDto {
  /**
   * Внешний идентификатор товара в каталоге или тестовом наборе данных.
   *
   * Значение должно быть строкой, чтобы gateway не зависел от конкретного
   * формата идентификаторов продуктового домена.
   */
  @ApiProperty({
    description: "Идентификатор товара, который должен быть добавлен в заказ.",
    example: "product-001",
    minLength: 1
  })
  @IsString()
  @Length(1, 128)
  productId!: string;

  /**
   * Количество единиц товара в заказе.
   *
   * Допускаются только целые положительные значения. Нулевое или дробное
   * количество не имеет смысла для текущего доменного сценария.
   */
  @ApiProperty({
    description: "Количество единиц товара в позиции заказа.",
    example: 2,
    minimum: 1
  })
  @IsInt()
  @Min(1)
  quantity!: number;

  /**
   * Цена одной единицы товара в валюте заказа.
   *
   * Gateway принимает число и передает его дальше как денежное значение
   * учебного сценария. В production-коде такие значения обычно хранятся
   * в минимальных единицах валюты или decimal-типах.
   */
  @ApiProperty({
    description: "Цена одной единицы товара в валюте заказа.",
    example: 19.99,
    minimum: 0
  })
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0)
  unitPrice!: number;
}

/**
 * Запрос на создание заказа во внешнем REST API gateway-service.
 *
 * Эта сущность является публичным контрактом клиента с gateway. После успешной
 * HTTP-валидации gateway вызывает order-service по gRPC, а дальнейшая обработка
 * заказа идет через событийную цепочку order -> risk -> payment -> order.
 */
export class CreateOrderDto {
  /**
   * Идентификатор пользователя, который создает заказ.
   *
   * Gateway не проверяет существование пользователя, но гарантирует базовую
   * структурную корректность значения перед передачей команды во внутренние
   * сервисы.
   */
  @ApiProperty({
    description: "Идентификатор пользователя, от имени которого создается заказ.",
    example: "user-123",
    minLength: 1
  })
  @IsString()
  @Length(1, 128)
  userId!: string;

  /**
   * Трехбуквенный ISO-код валюты заказа.
   *
   * Валюта применяется ко всем позициям заказа. Разные валюты внутри одного
   * заказа не поддерживаются текущей моделью.
   */
  @ApiProperty({
    description: "Трехбуквенный ISO-код валюты заказа.",
    example: "USD",
    pattern: "^[A-Z]{3}$"
  })
  @IsString()
  @Matches(/^[A-Z]{3}$/)
  currency!: string;

  /**
   * Список товарных позиций заказа.
   *
   * Заказ должен содержать минимум одну позицию. Каждая позиция валидируется
   * как вложенная сущность, чтобы некорректные товары не уходили во внутренний
   * gRPC-контур.
   */
  @ApiProperty({
    description: "Список товарных позиций заказа.",
    type: () => [CreateOrderItemDto],
    minItems: 1
  })
  @IsArray()
  @ArrayMinSize(1)
  @ValidateNested({ each: true })
  @Type(() => CreateOrderItemDto)
  items!: CreateOrderItemDto[];
}
