import { Controller, Get } from "@nestjs/common";
import { ApiOkResponse, ApiOperation, ApiProperty, ApiTags } from "@nestjs/swagger";

/**
 * Ответ health endpoint gateway-service.
 *
 * Endpoint показывает, что HTTP-приложение gateway запущено и способно
 * отвечать на запросы. Он не проверяет доступность Kafka, PostgreSQL или
 * внутренних gRPC-сервисов.
 */
class HealthResponseDto {
  /**
   * Текущий технический статус gateway-service.
   *
   * Значение `ok` означает только работоспособность HTTP-процесса gateway.
   */
  @ApiProperty({
    description: "Технический статус HTTP-процесса gateway-service.",
    example: "ok"
  })
  status!: "ok";
}

@ApiTags("Служебные проверки")
@Controller("health")
export class HealthController {
  @Get()
  @ApiOperation({
    summary: "Проверить доступность gateway-service",
    description:
      "Возвращает простой health-check HTTP-процесса gateway. Endpoint не является глубокой проверкой зависимостей."
  })
  @ApiOkResponse({
    type: HealthResponseDto,
    description: "Gateway-service принимает HTTP-запросы."
  })
  getHealth(): HealthResponseDto {
    return {
      status: "ok"
    };
  }
}
