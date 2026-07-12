# Kafka Retry and DLQ

Retry подсистема маршрутизирует ошибки Kafka handler-а:

```text
source topic -> retry topics -> dead-letter.events
```

## Компоненты

| Файл | Назначение |
| --- | --- |
| `configurable-kafka-retry-policy.ts` | Чистая policy без NestJS/KafkaJS зависимостей. |
| `kafka-retry-policy.ts` | Готовый provider с order-flow topics. |
| `kafka-retry-dispatcher.ts` | Публикует retry message или `DeadLetterEvent`. |

## Настройка policy для другого bounded context

```ts
const policy = new ConfigurableKafkaRetryPolicy({
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

## Как появляется задержка

Kafka не умеет delayed delivery. Сообщение публикуется в retry topic сразу, а
`KafkaConsumerRunner` перед handler-ом ждёт `delayMs` и отправляет heartbeat.

## Headers

Retry сохраняет исходный event и добавляет/обновляет:

| Header | Назначение |
| --- | --- |
| `x-retry-count` | Номер retry-перехода. |
| `x-original-topic` | Topic первого handler-а. |
| `x-first-failed-at` | Время первой ошибки. |
| `x-error-code` | Стабильный код последней ошибки. |

## Non-retryable ошибки

Если handler выбрасывает:

```ts
throw new KafkaNonRetryableError(
  "INVALID_ORDER_ID",
  "payload.orderId must be a UUID"
);
```

dispatcher пропускает retry topics и сразу создаёт `DeadLetterEvent`.

## DLQ payload

`DeadLetterEvent.payload` содержит:

```text
originalTopic
originalPartition
originalOffset
errorMessage
errorStack
rawEvent
```

`rawEvent` хранит декодированный event envelope как JSON. Это позволяет
admin/DLQ flow показать оператору исходное событие и безопасно reprocess его.
