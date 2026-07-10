import {
  Body,
  Controller,
  HttpCode,
  Param,
  ParseUUIDPipe,
  Post
} from "@nestjs/common";
import {
  ApiBadRequestResponse,
  ApiBody,
  ApiConsumes,
  ApiCreatedResponse,
  ApiInternalServerErrorResponse,
  ApiOkResponse,
  ApiOperation,
  ApiProduces,
  ApiTags
} from "@nestjs/swagger";
import { CreateOrderDto } from "./dto/create-order.dto";
import {
  CancelOrderDto,
  CancelOrderResponseDto
} from "./dto/cancel-order.dto";
import { OrderResponseDto } from "./dto/order-response.dto";
import { OrdersService } from "./orders.service";

@ApiTags("Заказы")
@Controller("orders")
export class OrdersController {
  constructor(private readonly ordersService: OrdersService) {}

  @Post()
  @ApiOperation({
    summary: "Создать заказ",
    description:
      "Принимает заказ через публичный REST API gateway-service, валидирует тело запроса и передает команду во внутренний order-service по gRPC. Дальнейшие этапы risk и payment выполняются асинхронно через Kafka."
  })
  @ApiConsumes("application/json")
  @ApiProduces("application/json")
  @ApiBody({
    type: CreateOrderDto,
    description: "Данные заказа, который нужно создать."
  })
  @ApiCreatedResponse({
    type: OrderResponseDto,
    description:
      "Заказ создан в order-service. Ответ отражает состояние заказа на момент ответа gateway."
  })
  @ApiBadRequestResponse({
    description:
      "Тело запроса не прошло валидацию: отсутствуют обязательные поля, неверная валюта или некорректные позиции заказа."
  })
  @ApiInternalServerErrorResponse({
    description:
      "Gateway не смог создать заказ из-за ошибки внутреннего gRPC-вызова или инфраструктуры."
  })
  createOrder(@Body() dto: CreateOrderDto): Promise<OrderResponseDto> {
    return this.ordersService.createOrder(dto);
  }

  @Post(":id/cancel")
  @HttpCode(200)
  @ApiOperation({
    summary: "Отменить заказ",
    description:
      "Запрашивает пользовательскую отмену заказа. order-service атомарно сохраняет OrderCancellationRequested и либо OrderCancelled, либо OrderCancellationRejected в transactional outbox."
  })
  @ApiConsumes("application/json")
  @ApiProduces("application/json")
  @ApiBody({
    type: CancelOrderDto,
    description: "Причина отмены заказа."
  })
  @ApiOkResponse({
    type: CancelOrderResponseDto,
    description:
      "Команда обработана order-service. cancellationStatus показывает, была ли отмена принята."
  })
  @ApiBadRequestResponse({
    description: "Некорректный id заказа или причина отмены."
  })
  cancelOrder(
    @Param("id", new ParseUUIDPipe()) id: string,
    @Body() dto: CancelOrderDto
  ): Promise<CancelOrderResponseDto> {
    return this.ordersService.cancelOrder(id, dto);
  }
}
