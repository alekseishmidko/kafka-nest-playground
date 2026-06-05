import { type INestApplication } from "@nestjs/common";
import { DocumentBuilder, SwaggerModule } from "@nestjs/swagger";

export function setupSwagger(app: INestApplication) {
  const config = new DocumentBuilder()
    .setTitle("Kafka Nest Playground Gateway API")
    .setDescription(
      "Публичный REST API gateway-service. Gateway принимает HTTP-запросы клиентов и проксирует команды во внутренние gRPC-сервисы. Бизнес-флоу заказа после создания продолжается асинхронно через Kafka: order -> risk -> payment -> order."
    )
    .setVersion("0.1.0")
    .addTag("Заказы", "Операции публичного API для создания и сопровождения заказов.")
    .addTag(
      "Служебные проверки",
      "Технические endpoint'ы для проверки доступности gateway-service."
    )
    .build();

  const document = SwaggerModule.createDocument(app, config);

  SwaggerModule.setup("docs", app, document, {
    customSiteTitle: "Kafka Nest Playground Gateway API",
    swaggerOptions: {
      persistAuthorization: true
    }
  });
}
