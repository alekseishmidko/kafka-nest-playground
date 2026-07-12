# Kafka Consumer Runner

`KafkaConsumerRunner` координирует все Kafka subscriptions одного NestJS
процесса.

## Зачем нужен runner

KafkaJS consumer запускается через один `consumer.run()`. Если каждый feature
module будет запускать свой loop, появятся гонки и конфликт подписок. Runner
решает это так:

```text
feature module onModuleInit
  -> consumerRunner.subscribe(topic, handler)

application bootstrap
  -> runner subscribes to main + retry topics
  -> runner starts one consumer.run()
```

## Использование

```ts
@Injectable()
export class PaymentConsumer implements OnModuleInit {
  constructor(
    private readonly consumerRunner: KafkaConsumerRunner,
    private readonly paymentService: PaymentService
  ) {}

  async onModuleInit(): Promise<void> {
    await this.consumerRunner.subscribe(
      { topic: KAFKA_TOPICS.riskRiskEvents },
      (context) => this.paymentService.handleRiskEvent(context)
    );
  }
}
```

Для нескольких topics с одним handler:

```ts
await this.consumerRunner.subscribeMany(
  {
    topics: [
      KAFKA_TOPICS.riskRiskEvents,
      KAFKA_TOPICS.paymentPaymentEvents
    ]
  },
  (context) => this.orderService.handleLifecycleEvent(context)
);
```

## Retry integration

Сервис подписывается только на основной topic. Runner сам добавляет retry topics
из `KafkaRetryPolicy`:

```text
order.order-events
order.order-events.retry-5s
order.order-events.retry-30s
order.order-events.retry-5m
```

Когда сообщение пришло из общего retry topic, runner восстанавливает исходный
handler через `x-original-topic`.

## Ошибки handler-а

```text
handler success
  -> KafkaJS commits offset

handler throws
  -> KafkaRetryDispatcher publishes retry or DLQ
  -> offset commits only after dispatch success
```

Если retry/DLQ publish не удался, ошибка пробрасывается в KafkaJS. Исходное
сообщение будет доставлено повторно.

## Ограничения

- Подписки нужно регистрировать до application bootstrap.
- Один topic может иметь только один handler в рамках процесса.
- Poison messages, которые нельзя десериализовать из Avro, пока не попадают в
  DLQ, потому что runner не может надёжно восстановить event envelope.
