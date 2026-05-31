# План внедрения Kafka в NestJS + Go микросервисы

Цель проекта: собрать учебное приложение с достаточно тяжелой бизнес-логикой, чтобы на практике изучить Kafka как event streaming платформу: топики, партиции, consumer groups, retry, DLQ, идемпотентность, outbox, schema evolution, ordering, backpressure, observability и взаимодействие сервисов на разных языках.

## 1. Выбрать предметную область

Лучше не делать абстрактные `users/orders`, а взять домен, где естественно появляются длинные процессы, конкуренция, пересчеты и ошибки.

Рекомендуемый сценарий: **маркетплейс с risk scoring и fulfillment pipeline**.

Сервисы:

- `gateway-service` на NestJS: публичный HTTP/gRPC API, входная точка для клиентов.
- `identity-service` на NestJS: пользователи, роли, профили, auth events.
- `catalog-service` на NestJS: товары, цены, остатки, события изменения каталога.
- `order-service` на NestJS: создание заказа, оркестрация статуса заказа.
- `payment-service` на NestJS или Go: авторизация платежей, возвраты, payment events.
- `risk-service` на Go: тяжелый scoring заказа, антифрод правила, async обработка.
- `pricing-service` на Go: пересчет динамических цен, применение скидок, batch calculations.
- `notification-service` на NestJS: email/push/webhook уведомления.
- `analytics-service` на Go: агрегации, materialized views, статистика.

Почему этот домен полезен для Kafka:

- У заказа есть жизненный цикл из многих событий.
- Часть логики синхронная, часть асинхронная.
- Есть тяжелые вычисления: risk scoring, pricing, analytics.
- Нужны retry, DLQ, идемпотентность и защита от дублей.
- Есть разные требования к ordering: по `orderId`, `userId`, `productId`.
- Можно тренировать cross-language контракты между NestJS и Go.

## 2. Подготовить локальную инфраструктуру

Создать `docker-compose.yml` для локальной разработки:

- Kafka broker.
- Kafka UI.
- Schema Registry.
- PostgreSQL для сервисов.
- Redis, если понадобится cache/rate limit.
- OpenTelemetry Collector.
- Prometheus + Grafana, если захочется метрик.

Рекомендуемый стек для простого старта:

- Kafka: Bitnami Kafka или Confluent Platform.
- UI: `provectuslabs/kafka-ui`.
- Schema Registry: Confluent Schema Registry.
- Формат сообщений: Avro или Protobuf.

Минимальный результат этапа:

- Kafka поднимается одной командой.
- Kafka UI показывает broker, topics, consumer groups.
- Можно вручную создать topic и отправить тестовое сообщение.

## 3. Зафиксировать структуру монорепозитория

Текущий workspace уже поддерживает `app/*` и `packages/*`. Рекомендуемая структура:

```text
app/
  gateway-service/
  identity-service/
  catalog-service/
  order-service/
  payment-service/
  notification-service/
  risk-service-go/
  pricing-service-go/
  analytics-service-go/

packages/
  contracts/
  config/
  kafka/
  observability/
  testing/
```

Назначение общих пакетов:

- `packages/contracts`: схемы событий, generated types, topic names.
- `packages/config`: общая загрузка env и validation.
- `packages/kafka`: NestJS Kafka client helpers, producer/consumer wrappers.
- `packages/observability`: tracing/logging conventions.
- `packages/testing`: testcontainers, fixtures, contract tests.

## 4. Спроектировать события и топики

Сначала описать бизнес-события, а не технические команды.

Базовые топики:

```text
identity.user-events
catalog.product-events
catalog.inventory-events
order.order-events
payment.payment-events
risk.risk-events
pricing.price-events
notification.notification-commands
analytics.domain-events
dead-letter.events
```

Примеры событий:

- `UserRegistered`
- `ProductCreated`
- `InventoryReserved`
- `InventoryReservationFailed`
- `OrderCreated`
- `OrderRiskCheckRequested`
- `OrderRiskApproved`
- `OrderRiskRejected`
- `PaymentAuthorized`
- `PaymentFailed`
- `OrderConfirmed`
- `OrderCancelled`
- `NotificationRequested`

Правила проектирования:

- Topic name описывает поток, event type лежит внутри сообщения.
- Key выбирается по сущности, для которой нужен ordering.
- Для order pipeline использовать key = `orderId`.
- Для inventory events можно использовать key = `productId`.
- Для user activity можно использовать key = `userId`.
- Каждое сообщение должно иметь `eventId`, `eventType`, `eventVersion`, `occurredAt`, `correlationId`, `causationId`.

Базовый envelope:

```json
{
  "eventId": "uuid",
  "eventType": "OrderCreated",
  "eventVersion": 1,
  "occurredAt": "2026-05-31T10:00:00.000Z",
  "correlationId": "uuid",
  "causationId": "uuid",
  "producer": "order-service",
  "payload": {}
}
```

## 5. Ввести schema-first контракты

Для проекта с NestJS и Go лучше не полагаться на произвольный JSON.

Варианты:

- Avro + Schema Registry: хорошо подходит для Kafka и schema evolution.
- Protobuf: удобно для Go и уже близко к gRPC, если проект использует proto.

Рекомендация для тренировки Kafka: начать с **Avro + Schema Registry**, затем отдельно попробовать Protobuf для сравнения.

Что сделать:

- Создать `packages/contracts/schemas`.
- Описать Avro-схемы для ключевых событий.
- Настроить генерацию TypeScript типов.
- Настроить генерацию Go типов.
- Добавить contract tests на backward compatibility.

Что изучить на этом этапе:

- Совместимость схем: backward, forward, full.
- Эволюция события через `eventVersion`.
- Добавление optional fields.
- Запрет breaking changes.

## 6. Подключить Kafka в NestJS

Для NestJS можно использовать:

- `@nestjs/microservices` Kafka transport для базового старта.
- `kafkajs` напрямую, если нужен более точный контроль producer/consumer behavior.

Рекомендация: использовать `kafkajs` как низкоуровневую библиотеку и обернуть ее в `packages/kafka`, чтобы явно управлять:

- producer send.
- consumer subscribe.
- manual commit.
- retry policy.
- headers.
- serialization/deserialization.
- tracing.

Минимальные задачи:

- Добавить общий `KafkaModule`.
- Добавить `KafkaProducerService`.
- Добавить `KafkaConsumerRunner`.
- Добавить serializer/deserializer через Schema Registry.
- Прокидывать `correlationId` через headers.
- Логировать `topic`, `partition`, `offset`, `eventType`, `eventId`.

## 7. Подключить Kafka в Go сервисах

Для Go сервисов выбрать одну библиотеку:

- `segmentio/kafka-go`: проще стартовать.
- `confluent-kafka-go`: ближе к production Kafka, использует librdkafka.

Рекомендация:

- Для первого этапа: `segmentio/kafka-go`.
- Для второго этапа: попробовать `confluent-kafka-go` и сравнить поведение.

Минимальные задачи для каждого Go сервиса:

- Конфигурация Kafka через env.
- Consumer group.
- Graceful shutdown.
- Manual commit после успешной обработки.
- Retry через отдельный retry topic.
- DLQ после исчерпания попыток.
- Structured logs.
- Prometheus metrics.

## 8. Реализовать первый end-to-end flow

Сценарий: создание заказа.

Шаги:

1. Клиент вызывает `POST /orders` в `gateway-service`.
2. `gateway-service` синхронно вызывает `order-service`.
3. `order-service` сохраняет заказ в PostgreSQL со статусом `PENDING`.
4. `order-service` публикует `OrderCreated`.
5. `risk-service-go` читает `OrderCreated`.
6. `risk-service-go` выполняет тяжелый scoring.
7. `risk-service-go` публикует `OrderRiskApproved` или `OrderRiskRejected`.
8. `payment-service` читает `OrderRiskApproved`.
9. `payment-service` авторизует платеж и публикует `PaymentAuthorized` или `PaymentFailed`.
10. `order-service` читает payment events и меняет статус заказа.
11. `notification-service` получает финальный статус и отправляет уведомление.
12. `analytics-service-go` читает все события и строит read model.

На этом flow можно тренировать:

- event-driven communication.
- consumer groups.
- ordering per order.
- идемпотентность обработчиков.
- retry и DLQ.
- distributed tracing.

## 9. Добавить transactional outbox

Нельзя сначала записывать в БД, потом публиковать в Kafka без защиты: сервис может упасть между этими действиями.

Решение: transactional outbox.

Для каждого сервиса, который публикует события:

1. В одной DB transaction сохранить бизнес-изменение.
2. В той же transaction записать событие в таблицу `outbox_events`.
3. Отдельный publisher периодически читает unpublished events.
4. Publisher отправляет событие в Kafka.
5. После успешной отправки помечает запись как published.

Что изучить:

- Exactly-once illusion на уровне приложения.
- At-least-once delivery.
- Почему consumer все равно должен быть идемпотентным.
- Как обрабатывать повторную публикацию.

## 10. Добавить идемпотентность consumers

Каждый consumer должен безопасно переживать повторную доставку сообщения.

Подход:

- В каждой БД добавить таблицу `processed_events`.
- Перед обработкой проверять `eventId`.
- Обрабатывать сообщение и сохранять `eventId` в одной transaction.
- Commit offset делать только после успешной transaction.

Важно:

- Kafka может доставить сообщение повторно.
- Producer может отправить дубль.
- Consumer может упасть после обработки, но до commit offset.
- Поэтому обработчики должны быть at-least-once safe.

## 11. Реализовать retry и DLQ

Не все ошибки одинаковые.

Типы ошибок:

- Transient: временная ошибка сети, БД, внешнего API.
- Business: платеж отклонен, товара нет на складе.
- Poison message: событие невалидно или ломает handler.

Рекомендуемая схема:

```text
order.order-events
order.order-events.retry.1m
order.order-events.retry.5m
order.order-events.retry.30m
dead-letter.events
```

Правила:

- Business errors превращать в доменные события, а не в DLQ.
- Transient errors отправлять в retry topic.
- Poison messages отправлять в DLQ с причиной ошибки.
- В DLQ писать original topic, partition, offset, headers, error message, stack trace.

## 12. Добавить тяжелую логику для тренировки

Чтобы Kafka была не декоративной, нужны реальные нагрузки.

Идеи для `risk-service-go`:

- Scoring заказа по 50-100 правилам.
- Проверка velocity: сколько заказов пользователь сделал за период.
- Проверка подозрительных комбинаций товаров.
- Проверка billing/shipping distance.
- Stateful aggregate по пользователю.
- Artificial CPU-heavy scoring для теста backpressure.

Идеи для `pricing-service-go`:

- Динамическая цена на основе спроса.
- Batch пересчет цен при изменении каталога.
- Пересчет скидок при изменении правил.
- Публикация `PriceRecalculated` для тысяч товаров.

Идеи для `analytics-service-go`:

- Подсчет revenue по минутам.
- Conversion funnel.
- Fraud rate.
- Top products.
- Lag-aware materialized views.

Идеи для `order-service`:

- Saga-like state machine.
- Компенсации: cancel payment, release inventory.
- Timeout заказа, если payment не пришел вовремя.

## 13. Изучить partitioning и consumer groups

Практические эксперименты:

- Создать topic `order.order-events` с 3 partitions.
- Запустить 1 consumer instance и посмотреть lag.
- Запустить 3 consumer instances и увидеть распределение partitions.
- Запустить 5 consumer instances и увидеть, что лишние consumers простаивают.
- Поменять key с `orderId` на `userId` и увидеть изменение ordering.
- Искусственно сделать hot key и увидеть перекос нагрузки.

Что понять:

- Ordering гарантируется только внутри partition.
- Количество partitions задает верхнюю границу параллелизма consumer group.
- Неправильный key может создать bottleneck.
- Увеличение partitions может изменить распределение keys.

## 14. Добавить observability

Минимальный набор:

- Structured logs во всех сервисах.
- Correlation ID во всех событиях.
- OpenTelemetry traces.
- Kafka consumer lag metrics.
- Producer send duration.
- Handler processing duration.
- Retry count.
- DLQ count.

Полезные dashboards:

- Kafka broker health.
- Consumer group lag.
- Events per second by topic.
- Failed events by service.
- Handler latency p95/p99.
- Outbox unpublished events.
- DLQ by error type.

## 15. Добавить тестирование

Уровни тестов:

- Unit tests для business logic.
- Contract tests для Avro/Protobuf схем.
- Integration tests с Kafka через Testcontainers.
- End-to-end test для order flow.
- Load tests для producer/consumer throughput.

Что обязательно проверить:

- Consumer не обрабатывает один `eventId` дважды.
- Offset commit происходит только после успешной обработки.
- Retry topic получает временные ошибки.
- DLQ получает poison message.
- Schema compatibility не ломается.
- Go consumer читает события, созданные NestJS producer.
- NestJS consumer читает события, созданные Go producer.

## 16. Порядок реализации

### Этап 1: базовая инфраструктура

- Добавить `docker-compose.yml`.
- Поднять Kafka, Kafka UI, PostgreSQL, Schema Registry.
- Создать первый topic.
- Проверить отправку и чтение сообщения вручную.

### Этап 2: контракты

- Создать `packages/contracts`.
- Описать event envelope.
- Описать первые события: `OrderCreated`, `OrderRiskApproved`, `OrderRiskRejected`.
- Настроить генерацию TS/Go типов.

### Этап 3: общий Kafka слой

- Создать `packages/kafka`.
- Добавить producer helper для NestJS.
- Добавить consumer helper для NestJS.
- Добавить serializer/deserializer.
- Добавить correlation headers.

### Этап 4: первый NestJS producer

- Создать или расширить `order-service`.
- Добавить endpoint создания заказа.
- Сохранять заказ в БД.
- Публиковать `OrderCreated`.

### Этап 5: первый Go consumer/producer

- Создать `risk-service-go`.
- Читать `OrderCreated`.
- Выполнять scoring.
- Публиковать `OrderRiskApproved` или `OrderRiskRejected`.

### Этап 6: продолжение pipeline

- Добавить `payment-service`.
- Добавить `notification-service`.
- Добавить `analytics-service-go`.
- Довести order lifecycle до финальных статусов.

### Этап 7: надежность

- Добавить transactional outbox.
- Добавить processed events table.
- Добавить retry topics.
- Добавить DLQ.
- Добавить reprocessing tool для DLQ.

### Этап 8: нагрузка и эксперименты

- Добавить генератор заказов.
- Добавить CPU-heavy risk rules.
- Добавить batch price recalculation.
- Измерить lag, throughput и latency.
- Изменять partitions и consumer instances.

### Этап 9: production-like практики

- Добавить graceful shutdown.
- Добавить health checks.
- Добавить readiness checks на Kafka и DB.
- Добавить OpenTelemetry.
- Добавить dashboards.
- Добавить contract compatibility checks в CI.

## 17. Практические задания для изучения Kafka

1. Создать 10 000 заказов и посмотреть, как растет consumer lag.
2. Увеличить количество partitions и сравнить throughput.
3. Запустить несколько replicas `risk-service-go`.
4. Сделать один `orderId` hot key и увидеть проблему partition skew.
5. Сломать schema compatibility и убедиться, что contract test падает.
6. Уронить consumer после записи в БД, но до commit offset, и проверить идемпотентность.
7. Отправить poison message и убедиться, что оно попадает в DLQ.
8. Перезапустить Kafka и проверить восстановление consumers.
9. Добавить новый optional field в событие и проверить старых consumers.
10. Сделать reprocess DLQ обратно в исходный topic.

## 18. Что должно получиться в итоге

В конце проекта должно быть приложение, где Kafka используется не только как message broker, а как основа event-driven архитектуры:

- Несколько NestJS микросервисов.
- Несколько Go микросервисов.
- Общие schema-first контракты.
- Реальный order pipeline.
- Тяжелые async workers.
- Transactional outbox.
- Идемпотентные consumers.
- Retry и DLQ.
- Consumer groups и partitioning experiments.
- Observability по traces, logs и metrics.
- E2E и integration tests с Kafka.

Такой проект даст практику с большинством задач, которые встречаются при внедрении Kafka в микросервисной архитектуре.
