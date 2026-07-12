# Durable Kafka Inbox

Inbox защищает consumer side effects от повторного выполнения одного Kafka
event после rebalance, retry или crash.

## Модель

```text
Kafka event
  -> claim (consumer_name, event_id)
  -> prepare deterministic result
  -> save PREPARED result
  -> effect(result)
  -> mark COMPLETED
```

Если процесс упал после `effect`, но до `COMPLETED`, следующая попытка не
пересчитывает result, а повторяет effect с сохранённым result.

## Использование

```ts
await this.idempotentProcessor.process(
  context,
  () => {
    return {
      event: createPaymentAuthorizedEvent(context.event),
      topic: KAFKA_TOPICS.paymentPaymentEvents,
      key: context.event.payload.orderId
    };
  },
  (result) =>
    this.kafkaProducer.publish({
      topic: result.topic,
      key: result.key,
      event: result.event
    })
);
```

Для внешнего provider:

```ts
await this.idempotentProcessor.process(
  context,
  () => ({
    idempotencyKey: context.event.eventId,
    recipient,
    template,
    payload
  }),
  (notification) => this.emailProvider.send(notification)
);
```

## Требования к `prepare`

`prepare` должен быть детерминированным относительно входного event. Если он
создаёт downstream event, используйте стабильный `eventId`. Для этого есть:

```ts
import { deterministicEventId } from "@kafka-playground/kafka";
```

## Требования к `effect`

`effect` может выполниться повторно после crash window. Поэтому:

- Kafka downstream event должен иметь тот же `eventId`;
- downstream consumer тоже должен иметь inbox;
- внешний HTTP/provider должен принимать idempotency key;
- не кладите в `effect` недетерминированную бизнес-логику.

## PostgreSQL adapter

`PostgresKafkaInboxStore` хранит записи в `kafka_consumer_inbox`.

Миграции order-service создают таблицу и retention indexes. Перед запуском
workers:

```bash
pnpm --filter order-service migration:run
```

## Busy lease

Если другая реплика уже обрабатывает `(consumer_name, event_id)`, store
возвращает busy состояние. Processor выбрасывает `KafkaInboxBusyError`, и
runner отправляет сообщение в retry chain.
