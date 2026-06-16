# @kafka-playground/kafka

Переиспользуемый Kafka toolkit для публикации, обработки, retry/DLQ и durable
inbox. Пакет можно использовать целиком в NestJS-сервисах текущего playground
или частично через универсальное ядро `@kafka-playground/kafka/core`.

Пакет содержит:

- типизированные producer и consumer wrappers;
- Avro-сериализацию через Schema Registry;
- стандартные tracing headers;
- структурированные Kafka-логи;
- конфигурируемую retry policy без привязки к конкретным topics;
- готовый order-flow preset для текущего проекта;
- публикацию необработанных событий в `dead-letter.events`.

## Структура пакета

```text
src/
  core/       # framework-agnostic entrypoint: типы, headers, errors, retry policy
  adapters/   # KafkaJS clients и другие внешние транспортные адаптеры
  consumer/   # orchestration подписок и message loop
  inbox/      # durable inbox: контракты, processor и PostgreSQL adapter
  retry/      # configurable policy, order-flow preset и публикация retry/DLQ
```

Корневой `index.ts` сохраняет единый публичный API пакета. Приложения импортируют
компоненты через `@kafka-playground/kafka` и не зависят от внутреннего
расположения файлов.

## Переиспользование в других проектах

Для проектов, которым нужны только типы, headers и чистая retry policy, есть
отдельный entrypoint:

```ts
import {
  ConfigurableKafkaRetryPolicy,
  KAFKA_HEADER_NAMES,
  KafkaDomainEvent
} from "@kafka-playground/kafka/core";
```

`core` не требует NestJS-модуля, PostgreSQL inbox adapter-а или contracts
текущего playground-проекта. Минимальный контракт события описан типом
`KafkaDomainEvent`: `eventId`, `eventType`, `eventVersion`, `occurredAt`,
`correlationId`, `causationId`, `producer`, `payload`.

Пример retry-цепочки для другого bounded context:

```ts
const retryPolicy = new ConfigurableKafkaRetryPolicy({
  sourceTopics: ["billing.invoice-events"],
  stages: [
    {
      topic: "billing.invoice-events.retry-10s",
      delayMs: 10_000
    },
    {
      topic: "billing.invoice-events.retry-1m",
      delayMs: 60_000
    }
  ],
  deadLetterTopic: "billing.dead-letter"
});
```

NestJS-слой остаётся опциональным adapter-ом. При регистрации модуля можно
передать свои clients, inbox store, DLQ topic и resolver Schema Registry subject:

```ts
KafkaModule.register({
  clientId: "billing-service",
  serviceName: "billing-service",
  brokers: ["localhost:9092"],
  schemaRegistryUrl: "http://localhost:8081",
  deadLetterTopic: "billing.dead-letter",
  subjectResolver: ({ topic, eventType }) => `${topic}.${eventType}.v1`,
  producerClient,
  consumerClient,
  inboxStore
});
```

Если `subjectResolver` не задан, producer использует универсальное имя
`${topic}-${eventType}-value`. Текущий playground order-flow продолжает
использовать этот формат, поэтому существующие Avro subjects не меняются.

## Durable consumer inbox

`KafkaIdempotentEventProcessor` защищает `risk-service`, `payment-service` и
`notification-service` от повторного выполнения одного события после
перезапуска consumer-а или повторной доставки Kafka.

Producer-side пара для этого механизма находится в `@kafka-playground/outbox`.
Обе части вместе закрывают основной message-flow:

```text
DB transaction -> outbox -> Kafka -> inbox -> handler
```

Outbox отвечает за то, чтобы событие не потерялось после DB commit. Inbox
отвечает за то, чтобы повторная доставка Kafka не выполнила бизнес-side effect
повторно.

Обработка разделена на две фазы:

1. Consumer атомарно захватывает `(consumer_name, event_id)` с временной lease.
2. Доменный сервис вычисляет результат без внешнего side effect.
3. Результат сохраняется в `kafka_consumer_inbox` со статусом `PREPARED`.
4. Выполняется публикация Kafka-события или вызов notification provider.
5. Inbox переводится в `COMPLETED`.

Если процесс завершился между шагами 4 и 5, следующая попытка использует
сохранённый результат. Risk/payment события получают детерминированный
`eventId`, поэтому downstream consumer распознает повторную публикацию как
дубль. Notification provider получает `idempotencyKey`, равный входному
`eventId`.

Это не распределённая транзакция между PostgreSQL и Kafka. Гарантия строится
на стандартной модели at-least-once:

- входное событие может физически прийти повторно;
- бизнес-решение не вычисляется повторно;
- повторный downstream event имеет тот же `eventId`;
- каждый downstream consumer также обязан иметь inbox;
- внешний API должен поддерживать idempotency key.

Перед запуском workers необходимо применить миграции:

```bash
pnpm --filter order-service migration:run
```

Для восстановления зависшей обработки используется lease 30 секунд. Пока
lease активна, параллельная попытка завершается retryable-ошибкой и проходит
через настроенные retry topics.

## Retry-цепочка

Для входных order-flow topics настроен следующий маршрут:

```text
order.order-events | risk.risk-events | payment.payment-events
  -> order.order-events.retry-5s
  -> order.order-events.retry-30s
  -> order.order-events.retry-5m
  -> dead-letter.events
```

Переход выполняется только после ошибки consumer handler-а. Успешно обработанное
сообщение никуда дополнительно не публикуется.

Все три исходных topic используют общие retry topics. Значение исходного topic
сохраняется в `x-original-topic`, поэтому DLQ и диагностика не теряют источник
события.

| Текущий topic | Действие после ошибки | `x-retry-count` |
| --- | --- | --- |
| `order.order-events` | публикация в `retry-5s` | `1` |
| `order.order-events.retry-5s` | публикация в `retry-30s` | `2` |
| `order.order-events.retry-30s` | публикация в `retry-5m` | `3` |
| `order.order-events.retry-5m` | создание `DeadLetterEvent` | `4` |

Ошибки класса `KafkaNonRetryableError` пропускают промежуточные retry topics и
сразу создают `DeadLetterEvent`. Это используется для неисправимых сообщений,
например события с `orderId`, который не является UUID.

`KafkaConsumerRunner` автоматически подписывает consumer на retry topics.
Сервису по-прежнему достаточно подписаться только на основной topic:

```ts
await consumerRunner.subscribe(
  {
    topic: KAFKA_TOPICS.orderOrderEvents
  },
  (context) => orderHandler.handle(context)
);
```

## Как создается задержка

Kafka не предоставляет отложенную доставку сообщения. Retry topic хранит
сообщение сразу, а задержку реализует consumer:

1. Получает сообщение из retry topic.
2. Определяет задержку по `KafkaRetryPolicy`.
3. Ожидает 5 секунд, 30 секунд или 5 минут.
4. Во время ожидания каждые 3 секунды отправляет heartbeat.
5. После задержки запускает обычный handler.

Heartbeat нужен, чтобы broker не исключил consumer из consumer group во время
пятиминутного ожидания.

Текущая реализация проста и подходит для учебного проекта. В production-системе
большое количество ожидающих сообщений может занять все partitions. Для высокой
нагрузки обычно применяют отдельные retry consumers, pause/resume partitions или
внешний scheduler.

## Kafka headers

Каждое повторно опубликованное сообщение сохраняет исходный `eventId`,
`correlationId`, `causationId`, key и payload.

| Header | Назначение |
| --- | --- |
| `x-retry-count` | Число выполненных переходов по retry-цепочке |
| `x-original-topic` | Topic, в котором сообщение обрабатывалось впервые |
| `x-first-failed-at` | ISO-время первой ошибки; не меняется при новых попытках |
| `x-error-code` | Нормализованный тип последней ошибки, например `TYPE_ERROR` |

Также сохраняются стандартные headers:

- `x-correlation-id`;
- `x-causation-id`;
- `x-event-id`;
- `x-event-type`;
- `x-event-version`.

### Tracing headers

| Header | Назначение |
| --- | --- |
| `traceparent` | Стандартный W3C parent context |
| `tracestate` | Vendor-specific W3C state |
| `x-trace-id` | Trace id для Kafka UI и диагностики |
| `x-span-id` | Span producer-а, записавшего сообщение |

Producer создаёт `<topic> publish` с `SpanKind.PRODUCER`. Consumer
восстанавливает parent и создаёт `<topic> process` с `SpanKind.CONSUMER`.
Retry создаёт дочерний `kafka retry dispatch`, а терминальный переход -
`kafka dlq dispatch`. Все этапы сохраняют один `traceId`.

## Ответственность компонентов

### `ConfigurableKafkaRetryPolicy`

Чистая политика маршрутизации. Она не использует Kafka и таймеры, а только:

- возвращает список topics для подписки;
- возвращает задержку для retry topic;
- определяет следующий topic;
- рассчитывает retry headers;
- определяет момент перехода в DLQ.

### `KafkaRetryPolicy`

Готовый NestJS provider для текущего order-flow. Он наследует
`ConfigurableKafkaRetryPolicy` и подставляет topics из
`@kafka-playground/contracts`. Для нового проекта предпочтительнее создать
собственный instance `ConfigurableKafkaRetryPolicy` со своими topics.

### `KafkaRetryDispatcher`

Публикует исходное событие в следующий retry topic. После последней неуспешной
попытки создает новый `DeadLetterEvent`, содержащий исходный topic, partition,
offset, описание ошибки и сериализованное исходное событие.

### `KafkaConsumerRunner`

Подписывается на основной и retry topics, выполняет задержку с heartbeat,
вызывает handler и передает его ошибки dispatcher-у.

Все feature-consumers одного процесса регистрируются в runner-е во время
`onModuleInit`. После инициализации NestJS запускается единственный
`consumer.run()`. Retry-сообщение направляется нужному handler-у по
`x-original-topic`; это особенно важно, потому что order, risk и payment
используют общие retry topics.

При наличии `ApplicationMetrics` runner записывает:

- успешные и неуспешные обработки;
- нормализованный error code;
- duration deserialize + handler;
- retry и DLQ counters.

Retry delay намеренно не включается в processing duration.

### `KafkaLagMonitor`

Если заданы `consumerGroupId` и metrics provider, monitor каждые 15 секунд
использует Kafka Admin API для обновления `kafka_consumer_lag`. Расчёты
выполняются через `BigInt`, потому что Kafka offsets могут превышать безопасный
32-bit диапазон.

Offset исходного сообщения фиксируется только после успешной публикации в
следующий retry topic или DLQ. Если Kafka producer не смог выполнить публикацию,
ошибка пробрасывается в KafkaJS, поэтому исходное сообщение будет доставлено
повторно.

## Ограничения

Retry policy сейчас настроена для `order.order-events`, `risk.risk-events` и
`payment.payment-events`.

Сообщение должно успешно десериализоваться из Avro до вызова handler-а. Если
payload пустой или поврежден, runner не знает надежный Schema Registry subject
для повторной публикации. Такая ошибка пробрасывается в KafkaJS, а offset не
фиксируется. Для poison messages на уровне десериализации нужен отдельный raw
DLQ-механизм, который хранит исходный `Buffer`.

## Проверка

Модульные тесты:

```bash
pnpm --filter @kafka-playground/kafka test
```

Они проверяют:

- полный маршрут `main -> 5s -> 30s -> 5m -> DLQ`;
- задержки каждого этапа;
- увеличение `x-retry-count`;
- сохранение `x-first-failed-at`;
- сохранение исходного события и `eventId`;
- содержимое `DeadLetterEvent`.

Ручная проверка через Kafka UI:

1. Запустить инфраструктуру и сервисы.
2. Временно выбросить ошибку в handler-е события из `order.order-events`.
3. Создать заказ.
4. Открыть `http://localhost:8080`.
5. Проверить появление сообщения последовательно в retry topics.
6. Проверить четыре retry header-а.
7. Оставить ошибку до последней попытки и проверить `DeadLetterEvent` в
   `dead-letter.events`.

В локальной инфраструктуре topics создаются автоматически, потому что broker
запущен с `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`.
