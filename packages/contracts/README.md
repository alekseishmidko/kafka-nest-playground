# @kafka-playground/contracts

`@kafka-playground/contracts` - общий пакет контрактов для всех микросервисов проекта.

Его задача - быть единым источником правды для Kafka topics, event types, event envelope, Schema Registry subject names и Avro-схем. NestJS и Go сервисы должны опираться на эти контракты, а не придумывать структуру сообщений локально внутри каждого сервиса.

## Зачем нужен этот пакет

Kafka связывает сервисы через сообщения. Если каждый сервис отправляет произвольный JSON, быстро появляются проблемы:

- producer отправляет поле `order_id`, а consumer ожидает `orderId`;
- один сервис меняет структуру события и ломает другие сервисы;
- невозможно понять, кто владеет topic;
- сложно проверить backward compatibility;
- сложно генерировать типы для TypeScript и Go;
- DLQ и reprocessing становятся хаотичными.

Этот пакет решает это через schema-first подход:

1. Сначала описывается контракт события.
2. Потом producer обязан отправлять событие по этому контракту.
3. Consumer читает событие как типизированный контракт.
4. Schema Registry хранит версии схем и проверяет совместимость.

## Структура пакета

```text
packages/contracts/
  README.md
  package.json
  tsconfig.json

  src/
    index.ts
    topics.ts
    events.ts

  schemas/
    avro/
      order-created.v1.avsc
      order-risk-approved.v1.avsc
      order-risk-rejected.v1.avsc
      payment-authorized.v1.avsc
      payment-failed.v1.avsc
      dead-letter-event.v1.avsc

  generated/
    ts/
    go/
```

## `src/topics.ts`

Файл `src/topics.ts` описывает Kafka topics и связанные metadata.

Главные экспорты:

- `KAFKA_TOPICS`
- `KafkaTopicName`
- `KAFKA_TOPIC_NAMES`
- `TOPIC_OWNERS`
- `TOPIC_KEY_STRATEGY`

### `KAFKA_TOPICS`

`KAFKA_TOPICS` - типизированный список topic names.

```ts
export const KAFKA_TOPICS = {
  identityUserEvents: "identity.user-events",
  catalogProductEvents: "catalog.product-events",
  catalogInventoryEvents: "catalog.inventory-events",
  orderOrderEvents: "order.order-events",
  paymentPaymentEvents: "payment.payment-events",
  riskRiskEvents: "risk.risk-events",
  pricingPriceEvents: "pricing.price-events",
  notificationNotificationCommands: "notification.notification-commands",
  analyticsDomainEvents: "analytics.domain-events",
  deadLetterEvents: "dead-letter.events"
} as const;
```

Использование:

```ts
import { KAFKA_TOPICS } from "@kafka-playground/contracts";

const topic = KAFKA_TOPICS.orderOrderEvents;
```

Такой подход лучше строк руками:

```ts
// хуже
const topic = "order.order-events";
```

Если topic переименуется, TypeScript поможет найти места использования.

### `KafkaTopicName`

`KafkaTopicName` - union type всех допустимых topic names.

```ts
export type KafkaTopicName = (typeof KAFKA_TOPICS)[keyof typeof KAFKA_TOPICS];
```

Он нужен, чтобы producer/consumer не могли случайно использовать неизвестный topic.

### `KAFKA_TOPIC_NAMES`

`KAFKA_TOPIC_NAMES` - массив всех topic names.

Подходит для:

- bootstrap scripts;
- создания topics при старте инфраструктуры;
- тестов;
- validation;
- документации.

### `TOPIC_OWNERS`

`TOPIC_OWNERS` показывает, какой сервис владеет topic.

Пример:

```ts
export const TOPIC_OWNERS = {
  "order.order-events": "order-service",
  "risk.risk-events": "risk-service-go",
  "dead-letter.events": "platform"
};
```

Правило владения:

- owner сервиса решает, какие события публикуются в topic;
- другие сервисы могут читать topic, но не должны менять его контракт без согласования;
- `dead-letter.events` считается platform-level topic.

### `TOPIC_KEY_STRATEGY`

`TOPIC_KEY_STRATEGY` описывает рекомендуемый Kafka message key для каждого topic.

Примеры:

- `order.order-events` -> `orderId`
- `payment.payment-events` -> `orderId`
- `catalog.inventory-events` -> `productId`
- `identity.user-events` -> `userId`

Это важно для ordering.

Kafka гарантирует порядок только внутри одной partition. Если все события одного заказа должны обрабатываться последовательно, они должны иметь одинаковый key:

```text
key = orderId
topic = order.order-events
```

Тогда события одного заказа попадут в одну partition.

## `src/events.ts`

Файл `src/events.ts` описывает TypeScript-типы доменных событий.

Главные экспорты:

- `EventEnvelope`
- payload-типы событий;
- event-типы событий;
- `DomainEvent`
- `DomainEventType`
- `EVENT_TOPIC_MAP`
- `EVENT_SCHEMA_SUBJECTS`

## Event Envelope

Все события должны иметь общий envelope:

```ts
export interface EventEnvelope<TPayload, TEventType extends string = string> {
  eventId: string;
  eventType: TEventType;
  eventVersion: number;
  occurredAt: string;
  correlationId: string;
  causationId: string | null;
  producer: string;
  payload: TPayload;
}
```

Назначение полей:

- `eventId`: уникальный id события. Используется для идемпотентности.
- `eventType`: тип события, например `OrderCreated`.
- `eventVersion`: версия события на уровне приложения.
- `occurredAt`: время возникновения события в ISO-формате.
- `correlationId`: id всей цепочки действий, полезен для tracing.
- `causationId`: id события или команды, которая вызвала текущее событие.
- `producer`: сервис, который создал событие.
- `payload`: бизнес-данные события.

Пример события:

```json
{
  "eventId": "01JYEXAMPLE0000000000000001",
  "eventType": "OrderCreated",
  "eventVersion": 1,
  "occurredAt": "2026-05-31T10:00:00.000Z",
  "correlationId": "01JYEXAMPLE0000000000000002",
  "causationId": null,
  "producer": "order-service",
  "payload": {
    "orderId": "order-123",
    "userId": "user-456",
    "currency": "USD",
    "totalAmount": 199.99,
    "itemCount": 3
  }
}
```

## Текущие события

Сейчас описаны базовые события для первого order pipeline.

### `OrderCreated`

Topic:

```text
order.order-events
```

Producer:

```text
order-service
```

Consumers:

```text
risk-service-go
analytics-service-go
notification-service
```

Payload:

```ts
export interface OrderCreatedPayload {
  orderId: string;
  userId: string;
  currency: string;
  totalAmount: number;
  itemCount: number;
}
```

### `OrderRiskApproved`

Topic:

```text
risk.risk-events
```

Producer:

```text
risk-service-go
```

Consumers:

```text
order-service
payment-service
analytics-service-go
```

Payload:

```ts
export interface OrderRiskApprovedPayload {
  orderId: string;
  riskScore: number;
  approvedBy: string;
}
```

### `OrderRiskRejected`

Topic:

```text
risk.risk-events
```

Producer:

```text
risk-service-go
```

Consumers:

```text
order-service
notification-service
analytics-service-go
```

Payload:

```ts
export interface OrderRiskRejectedPayload {
  orderId: string;
  riskScore: number;
  reason: string;
  rejectedBy: string;
}
```

### `PaymentAuthorized`

Topic:

```text
payment.payment-events
```

Producer:

```text
payment-service
```

Consumers:

```text
order-service
notification-service
analytics-service-go
```

Payload:

```ts
export interface PaymentAuthorizedPayload {
  paymentId: string;
  orderId: string;
  amount: number;
  currency: string;
  provider: string;
}
```

### `PaymentFailed`

Topic:

```text
payment.payment-events
```

Producer:

```text
payment-service
```

Consumers:

```text
order-service
notification-service
analytics-service-go
```

Payload:

```ts
export interface PaymentFailedPayload {
  paymentId: string | null;
  orderId: string;
  reason: string;
  provider: string;
}
```

### `DeadLetterEvent`

Topic:

```text
dead-letter.events
```

Producer:

```text
any service or platform error handler
```

Consumers:

```text
DLQ reprocessing tools
observability tools
analytics-service-go
```

Payload:

```ts
export interface DeadLetterPayload {
  originalTopic: KafkaTopicName;
  originalPartition: number;
  originalOffset: string;
  errorMessage: string;
  errorStack: string | null;
  rawEvent: string | null;
}
```

## `DomainEvent`

`DomainEvent` - union всех известных событий:

```ts
export type DomainEvent =
  | OrderCreatedEvent
  | OrderRiskApprovedEvent
  | OrderRiskRejectedEvent
  | PaymentAuthorizedEvent
  | PaymentFailedEvent
  | DeadLetterEvent;
```

Этот тип используется в общем Kafka producer/consumer слое.

Например, producer принимает только известные события:

```ts
async publish(event: DomainEvent) {
  // ...
}
```

Если добавить новое событие, его нужно добавить в `DomainEvent`, иначе общий Kafka слой не будет считать его допустимым контрактом.

## `EVENT_TOPIC_MAP`

`EVENT_TOPIC_MAP` связывает event type с topic.

```ts
export const EVENT_TOPIC_MAP = {
  OrderCreated: KAFKA_TOPICS.orderOrderEvents,
  OrderRiskApproved: KAFKA_TOPICS.riskRiskEvents,
  OrderRiskRejected: KAFKA_TOPICS.riskRiskEvents,
  PaymentAuthorized: KAFKA_TOPICS.paymentPaymentEvents,
  PaymentFailed: KAFKA_TOPICS.paymentPaymentEvents,
  DeadLetterEvent: KAFKA_TOPICS.deadLetterEvents
} as const;
```

Это нужно, чтобы код мог определить, куда публиковать событие, не размазывая routing rules по сервисам.

## `EVENT_SCHEMA_SUBJECTS`

`EVENT_SCHEMA_SUBJECTS` задает имена subject в Schema Registry.

```ts
export const EVENT_SCHEMA_SUBJECTS = {
  OrderCreated: "order.order-events-OrderCreated-value",
  OrderRiskApproved: "risk.risk-events-OrderRiskApproved-value",
  OrderRiskRejected: "risk.risk-events-OrderRiskRejected-value",
  PaymentAuthorized: "payment.payment-events-PaymentAuthorized-value",
  PaymentFailed: "payment.payment-events-PaymentFailed-value",
  DeadLetterEvent: "dead-letter.events-DeadLetterEvent-value"
} as const;
```

Subject - это имя схемы в Schema Registry.

Почему subject включает и topic, и event type:

- в одном topic может жить несколько event types;
- у каждого event type своя Avro-схема;
- Schema Registry может версионировать каждую схему отдельно;
- consumer может понимать, какое событие он получил.

Пример:

```text
topic:   order.order-events
event:   OrderCreated
subject: order.order-events-OrderCreated-value
```

## Avro-схемы

Avro-схемы лежат в:

```text
packages/contracts/schemas/avro
```

Сейчас есть:

```text
order-created.v1.avsc
order-risk-approved.v1.avsc
order-risk-rejected.v1.avsc
payment-authorized.v1.avsc
payment-failed.v1.avsc
dead-letter-event.v1.avsc
```

Каждая `.avsc` схема описывает структуру события, включая envelope и payload.

Пример фрагмента:

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "kafka_playground.order.events.v1",
  "fields": [
    { "name": "eventId", "type": "string" },
    { "name": "eventType", "type": "string", "default": "OrderCreated" },
    { "name": "eventVersion", "type": "int", "default": 1 },
    { "name": "occurredAt", "type": "string" },
    { "name": "correlationId", "type": "string" },
    { "name": "causationId", "type": ["null", "string"], "default": null },
    { "name": "producer", "type": "string" }
  ]
}
```

## Как Avro и TypeScript-типы связаны

Сейчас TypeScript-типы в `src/events.ts` и Avro-схемы в `schemas/avro` описывают один и тот же контракт.

В дальнейшем нужно добавить генерацию:

```text
Avro schemas -> generated TypeScript types
Avro schemas -> generated Go types
```

После этого ручное дублирование типов можно будет убрать или сократить.

Целевая модель:

```text
schemas/avro/*.avsc
  -> generated/ts
  -> generated/go
```

Пока генерация не настроена, при изменении схемы нужно вручную обновлять:

- `.avsc` файл;
- TypeScript payload/event type в `src/events.ts`;
- `DomainEvent`, если добавлен новый event type;
- `EVENT_TOPIC_MAP`;
- `EVENT_SCHEMA_SUBJECTS`.

## Как это используется в NestJS

Пример создания события:

```ts
import {
  EVENT_TOPIC_MAP,
  type OrderCreatedEvent
} from "@kafka-playground/contracts";

const event: OrderCreatedEvent = {
  eventId: crypto.randomUUID(),
  eventType: "OrderCreated",
  eventVersion: 1,
  occurredAt: new Date().toISOString(),
  correlationId: correlationId,
  causationId: null,
  producer: "order-service",
  payload: {
    orderId: "order-123",
    userId: "user-456",
    currency: "USD",
    totalAmount: 199.99,
    itemCount: 3
  }
};

const topic = EVENT_TOPIC_MAP[event.eventType];
```

Дальше это событие передается в `KafkaProducerService` из `@kafka-playground/kafka`.

Producer использует:

- `event.eventType`;
- `event.eventId`;
- `event.correlationId`;
- `EVENT_SCHEMA_SUBJECTS`;
- Avro serializer через Schema Registry.

## Как это будет использоваться в Go

Go сервисы должны использовать те же Avro-схемы из `schemas/avro`.

Целевой процесс:

1. Сгенерировать Go-типы из `.avsc`.
2. Сервис читает Kafka message.
3. Schema Registry deserializer декодирует Avro payload.
4. Handler получает типизированное событие.
5. Handler проверяет `eventId` для идемпотентности.
6. Handler обрабатывает payload.

Пример для `risk-service-go`:

```text
consume order.order-events
  -> decode OrderCreated
  -> run risk scoring
  -> publish OrderRiskApproved or OrderRiskRejected
```

## Schema Registry

Schema Registry хранит версии Avro-схем.

Для каждого subject:

```text
order.order-events-OrderCreated-value
```

Schema Registry хранит версии:

```text
v1, v2, v3...
```

Producer при отправке:

1. Берет subject из `EVENT_SCHEMA_SUBJECTS`.
2. Получает latest schema id из Schema Registry.
3. Кодирует payload в Avro.
4. Отправляет бинарное Kafka message.

Consumer при чтении:

1. Получает бинарное Kafka message.
2. Достает schema id из сообщения.
3. Загружает schema из Schema Registry.
4. Декодирует Avro payload.

## Как регистрировать схемы

На следующем этапе стоит добавить script:

```text
packages/contracts/scripts/register-avro-schemas.ts
```

Он должен:

1. Прочитать `.avsc` файлы.
2. Взять subject из `EVENT_SCHEMA_SUBJECTS`.
3. Зарегистрировать схему в Schema Registry.
4. Выставить compatibility mode.
5. Упасть, если схема несовместима.

Пример целевой команды:

```bash
pnpm --filter @kafka-playground/contracts schemas:register
```

## Правила изменения контрактов

Контракты нельзя менять как обычный внутренний код. Любое изменение события влияет на producer, consumer, DLQ, reprocessing и исторические данные в Kafka.

### Безопасные изменения

Обычно безопасно:

- добавить optional field с default;
- добавить nullable field с default `null`;
- добавить новый event type;
- добавить новый topic;
- расширить enum-like string values, если consumers готовы к unknown values.

Пример безопасного поля:

```json
{
  "name": "comment",
  "type": ["null", "string"],
  "default": null
}
```

### Опасные изменения

Обычно нельзя:

- удалять поле;
- переименовывать поле;
- менять тип поля;
- делать optional field required;
- менять смысл существующего поля;
- переиспользовать старый `eventType` для нового смысла;
- менять key strategy без миграционного плана.

Плохо:

```json
{ "name": "totalAmount", "type": "string" }
```

если раньше было:

```json
{ "name": "totalAmount", "type": "double" }
```

## Версионирование событий

В проекте есть два уровня версионирования.

### Avro schema version

Это версия схемы в Schema Registry.

Она растет автоматически при регистрации новой версии subject.

Пример:

```text
subject: order.order-events-OrderCreated-value
versions: 1, 2, 3
```

### `eventVersion`

Это версия события внутри payload.

```json
{
  "eventType": "OrderCreated",
  "eventVersion": 1
}
```

`eventVersion` нужен бизнес-коду, чтобы handler мог явно различать версии события, если это понадобится.

Рекомендация:

- мелкие backward-compatible изменения делать через Schema Registry versions;
- серьезное изменение смысла события делать через новый `eventType`, например `OrderCreatedV2` или более точное доменное имя.

## Correlation и causation

Каждое событие содержит:

```ts
correlationId: string;
causationId: string | null;
```

`correlationId` связывает всю цепочку действий.

Пример:

```text
HTTP POST /orders
  correlationId = abc
  -> OrderCreated correlationId = abc
  -> OrderRiskApproved correlationId = abc
  -> PaymentAuthorized correlationId = abc
```

`causationId` показывает, какое событие стало причиной нового события.

Пример:

```text
OrderCreated.eventId = event-1
OrderRiskApproved.causationId = event-1
```

Это полезно для:

- distributed tracing;
- debugging;
- audit log;
- reprocessing;
- построения event graph.

## Идемпотентность

`eventId` должен быть уникальным.

Consumer должен сохранять обработанные `eventId`, например в таблицу:

```text
processed_events
```

Правило:

```text
если eventId уже обработан -> пропустить
если eventId новый -> обработать и сохранить eventId
```

Это обязательно, потому что Kafka delivery обычно проектируется как at-least-once.

Один и тот же event может прийти повторно, если:

- consumer упал после обработки, но до commit offset;
- producer отправил duplicate;
- сообщение переобрабатывается из DLQ;
- offset был сброшен вручную.

## DLQ contract

`DeadLetterEvent` описывает событие, которое не удалось обработать.

DLQ должен содержать минимум:

- исходный topic;
- partition;
- offset;
- текст ошибки;
- stack trace, если есть;
- raw event, если его удалось сохранить.

DLQ не должен использоваться для нормальных бизнес-ошибок.

Примеры:

- платеж отклонен - это `PaymentFailed`, не DLQ;
- заказ не прошел risk scoring - это `OrderRiskRejected`, не DLQ;
- consumer не может распарсить сообщение - это DLQ;
- payload не соответствует схеме - это DLQ;
- handler падает из-за poison message - это DLQ.

## Как добавить новое событие

Пример: нужно добавить `InventoryReserved`.

Шаги:

1. Добавить Avro-схему:

```text
schemas/avro/inventory-reserved.v1.avsc
```

2. Добавить payload type в `src/events.ts`:

```ts
export interface InventoryReservedPayload {
  reservationId: string;
  orderId: string;
  productId: string;
  quantity: number;
}
```

3. Добавить event type:

```ts
export type InventoryReservedEvent = EventEnvelope<
  InventoryReservedPayload,
  "InventoryReserved"
>;
```

4. Добавить событие в `DomainEvent`.

5. Добавить mapping в `EVENT_TOPIC_MAP`.

6. Добавить subject в `EVENT_SCHEMA_SUBJECTS`.

7. Проверить topic owner и key strategy в `src/topics.ts`.

8. Зарегистрировать Avro-схему в Schema Registry.

9. Добавить/обновить contract tests.

10. Обновить producer и consumers.

## Как добавить новый topic

Шаги:

1. Добавить topic в `KAFKA_TOPICS`.
2. Добавить owner в `TOPIC_OWNERS`.
3. Добавить key strategy в `TOPIC_KEY_STRATEGY`.
4. Добавить topic в infrastructure/bootstrap script, когда он появится.
5. Описать producers и consumers в README соответствующих сервисов.

## Contract tests

На следующем этапе нужно добавить contract tests.

Минимальный набор:

- все `.avsc` файлы являются валидным JSON;
- все `eventType` из `DomainEvent` имеют mapping в `EVENT_TOPIC_MAP`;
- все `eventType` имеют subject в `EVENT_SCHEMA_SUBJECTS`;
- все topic из `EVENT_TOPIC_MAP` существуют в `KAFKA_TOPICS`;
- все topic имеют owner;
- все topic имеют key strategy;
- новая Avro-схема backward-compatible с предыдущей версией;
- Schema Registry принимает регистрацию схемы.

Целевая команда:

```bash
pnpm --filter @kafka-playground/contracts test
```

## Текущие команды

Проверка TypeScript:

```bash
pnpm --filter @kafka-playground/contracts lint
```

Сборка:

```bash
pnpm --filter @kafka-playground/contracts build
```

## Важные правила для проекта

- Не отправлять в Kafka произвольный JSON.
- Не писать topic name руками в сервисах.
- Не создавать event type без Avro-схемы.
- Не менять существующую схему без проверки совместимости.
- Не удалять поля из событий, которые уже могли попасть в Kafka.
- Не использовать DLQ для нормальных бизнес-результатов.
- Всегда заполнять `eventId`, `eventType`, `eventVersion`, `occurredAt`, `correlationId`, `producer`.
- Для order pipeline использовать Kafka key = `orderId`.
- Для inventory pipeline использовать Kafka key = `productId`.
- Для user pipeline использовать Kafka key = `userId`.

## Что еще нужно реализовать

Пакет уже содержит базовые контракты, но для production-like процесса еще нужны:

- script регистрации Avro-схем в Schema Registry;
- генерация TypeScript типов из Avro;
- генерация Go типов из Avro;
- contract tests;
- compatibility checks;
- bootstrap script для создания Kafka topics;
- README с матрицей producers/consumers по всем сервисам.
