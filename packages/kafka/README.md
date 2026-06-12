# @kafka-playground/kafka

Общий NestJS-пакет для публикации и обработки Kafka-событий через KafkaJS и
Confluent Schema Registry.

Пакет содержит:

- типизированные producer и consumer wrappers;
- Avro-сериализацию через Schema Registry;
- стандартные tracing headers;
- структурированные Kafka-логи;
- retry policy для событий жизненного цикла заказа;
- публикацию необработанных событий в `dead-letter.events`.

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

## Ответственность компонентов

### `KafkaRetryPolicy`

Чистая политика маршрутизации. Она не использует Kafka и таймеры, а только:

- возвращает список topics для подписки;
- возвращает задержку для retry topic;
- определяет следующий topic;
- рассчитывает retry headers;
- определяет момент перехода в DLQ.

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
