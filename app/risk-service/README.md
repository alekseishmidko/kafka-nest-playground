# risk-service

NestJS-сервис для CPU-heavy scoring риска заказа.

Сервис слушает доменные события заказов, выполняет детерминированный risk scoring для каждого события `OrderCreated` и публикует событие с решением для downstream-сервисов.

## Зона Ответственности

- Читать `OrderCreated` из `order.order-events`.
- Рассчитывать CPU-heavy risk score для заказа.
- Публиковать `OrderRiskApproved` или `OrderRiskRejected` в `risk.risk-events`.
- Сохранять `correlationId` и выставлять `causationId` равным исходному `OrderCreated.eventId`.
- Использовать общую Kafka/Avro-инфраструктуру из `@kafka-playground/kafka`.

## Поток Событий

1. `order-service` создает pending order.
2. `order-service` публикует `OrderCreated` в `order.order-events`.
3. `risk-service` читает `OrderCreated`.
4. `RiskScorer` рассчитывает score на основе суммы заказа, количества товаров и детерминированного hash.
5. `risk-service` публикует одно событие в `risk.risk-events`:
   - `OrderRiskApproved`, если `score < RISK_SCORE_THRESHOLD`
   - `OrderRiskRejected`, если `score >= RISK_SCORE_THRESHOLD`

Kafka key равен `orderId`, поэтому события одного заказа сохраняют порядок внутри partition.

## Входящие События

Topic:

- `order.order-events`

Поддерживаемое событие:

- `OrderCreated`

Поля payload, которые используются для scoring:

- `orderId`
- `userId`
- `currency`
- `totalAmount`
- `itemCount`

## Исходящие События

Topic:

- `risk.risk-events`

Событие approval:

```ts
{
  eventType: "OrderRiskApproved",
  payload: {
    orderId: string,
    amount: number,
    currency: string,
    riskScore: number,
    approvedBy: "risk-service"
  }
}
```

Событие rejection:

```ts
{
  eventType: "OrderRiskRejected",
  payload: {
    orderId: string,
    riskScore: number,
    reason: "risk_score_threshold_exceeded",
    rejectedBy: "risk-service"
  }
}
```

Полные контракты событий находятся в `packages/contracts/src/events.ts`. Avro-схемы находятся в `packages/contracts/schemas/avro`.

## Логика Scoring

`RiskScorer` специально сделан CPU-heavy, чтобы имитировать реалистичную fraud/risk-нагрузку.

Score детерминированный:

- Входной seed строится из `orderId`, `userId`, `currency`, `totalAmount` и `itemCount`.
- Сервис много раз хэширует seed через SHA-256.
- Итоговый score объединяет:
  - amount factor: `totalAmount / 2000`, максимум `1`
  - item factor: `itemCount / 20`, максимум `1`
  - детерминированный hash factor

Формула:

```ts
score = 0.5 * amountFactor + 0.25 * itemFactor + 0.25 * hashFactor
```

Score округляется до 4 знаков после запятой.

Правило принятия решения:

```ts
score >= RISK_SCORE_THRESHOLD => OrderRiskRejected
score < RISK_SCORE_THRESHOLD  => OrderRiskApproved
```

## Конфигурация

Сервис использует такой же подход к env-файлам, как `order-service`:

- `.env.local`
- `.env.prod`
- `.env.example`

Обязательные переменные:

```env
APP_ENV=local
LOG_LEVEL=debug
KAFKA_CLIENT_ID=risk-service
KAFKA_BROKERS=localhost:9092
KAFKA_CONSUMER_GROUP_ID=risk-service
SCHEMA_REGISTRY_URL=http://localhost:8081

RISK_SCORE_THRESHOLD=0.72
RISK_SCORE_ITERATIONS=300000
```

Важные настройки:

- `KAFKA_BROKERS`: список Kafka brokers через запятую.
- `KAFKA_CONSUMER_GROUP_ID`: consumer group для `risk-service`.
- `SCHEMA_REGISTRY_URL`: endpoint Confluent Schema Registry.
- `RISK_SCORE_THRESHOLD`: порог отказа, должен быть между `0` и `1`.
- `RISK_SCORE_ITERATIONS`: количество hash-итераций для имитации тяжелого scoring.

## Локальный Запуск

Запустить инфраструктуру:

```bash
pnpm infra:up
```

Зарегистрировать Avro-схемы:

```bash
pnpm contracts:schemas:register
```

Запустить order-service:

```bash
pnpm dev:order
```

Запустить risk-service:

```bash
pnpm dev:risk
```

Также можно запустить сервис напрямую:

```bash
pnpm --filter risk-service dev
```

## Сборка И Проверки

```bash
pnpm --filter risk-service lint
pnpm --filter risk-service build
```

Сейчас у сервиса нет отдельного unit-test script, кроме placeholder по workspace-паттерну.

## Runtime

`risk-service` is a background Kafka worker. It does not expose HTTP endpoints.

Локальный адрес по умолчанию:

```text
http://localhost:3002
```

## Детали Реализации

- `RiskConsumer` запускает Kafka subscription в `onModuleInit`.
- `KafkaConsumerRunner` декодирует Avro-сообщения через Schema Registry.
- `RiskService` отвечает за доменный workflow: посчитать риск заказа, собрать risk event, опубликовать событие.
- `RiskScorer` отвечает за CPU-heavy детерминированный scoring algorithm.
- `KafkaProducerService` сериализует исходящие события в Avro и добавляет Kafka headers.
- `correlationId` сохраняется из исходного события для tracing.
- `causationId` выставляется равным исходному `OrderCreated.eventId`.

## Эксплуатационные Заметки

- Scoring синхронный и CPU-heavy. Большие значения `RISK_SCORE_ITERATIONS` могут блокировать Node.js event loop.
- Для реальной production-нагрузки тяжелый scoring лучше вынести в worker threads, отдельный compute service или Go/Rust-сервис.
- Consumer group risk-сервиса должна быть отдельной от других сервисов, чтобы risk decisions публиковались именно instances этого сервиса.
- Если `risk-service` выбросит ошибку во время обработки сообщения, KafkaJS не посчитает сообщение успешно обработанным, и consumer сможет повторить обработку согласно поведению KafkaJS.

## Связанные Сервисы

- `order-service`: публикует `OrderCreated`.
- `risk-service-go`: Go-версия такого же risk scoring service.
- `payment-service`: ожидаемый downstream-consumer после risk approval в order pipeline.
