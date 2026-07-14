# Project Visualization

Mermaid-диаграммы текущей архитектуры проекта. Файл можно смотреть в GitHub,
WebStorm Markdown preview или любом Mermaid renderer-е.

## System Context

```mermaid
flowchart LR
  Client[Client / API Consumer]

  subgraph Apps["Applications"]
    Gateway[gateway-service<br/>REST API + Swagger]
    Order[order-service<br/>gRPC + Admin API]
    RiskNest[risk-service<br/>NestJS worker]
    RiskGo[risk-service-go<br/>Go worker]
    Payment[payment-service<br/>Kafka worker]
    Notification[notification-service<br/>Kafka worker]
  end

  subgraph Platform["Local Platform"]
    Kafka[(Kafka)]
    SchemaRegistry[Schema Registry]
    Postgres[(PostgreSQL)]
    Prometheus[Prometheus]
    Grafana[Grafana]
    Tempo[Tempo]
    KafkaUI[Kafka UI]
  end

  Client -->|HTTP /orders| Gateway
  Gateway -->|gRPC OrdersService| Order

  Order <--> Postgres
  Order -->|outbox publish| Kafka
  RiskNest --> Kafka
  RiskGo --> Kafka
  Payment --> Kafka
  Notification --> Kafka

  RiskNest <--> SchemaRegistry
  RiskGo <--> SchemaRegistry
  Payment <--> SchemaRegistry
  Notification <--> SchemaRegistry
  Order <--> SchemaRegistry

  Prometheus -->|scrape /metrics| Order
  Prometheus -->|scrape /metrics| RiskNest
  Prometheus -->|scrape /metrics| Payment
  Prometheus -->|scrape /metrics| Notification
  Grafana --> Prometheus
  Grafana --> Tempo
  KafkaUI --> Kafka
```

## Order Pipeline

```mermaid
sequenceDiagram
  autonumber
  participant C as Client
  participant G as gateway-service
  participant O as order-service
  participant DB as PostgreSQL
  participant OB as OutboxPublisher
  participant K as Kafka
  participant R as risk-service / risk-service-go
  participant P as payment-service
  participant N as notification-service

  C->>G: POST /orders
  G->>O: gRPC CreateOrder
  O->>DB: transaction: insert order + outbox OrderCreated
  O-->>G: PENDING order response
  G-->>C: 201 Created

  OB->>DB: claim PENDING outbox row
  OB->>K: publish OrderCreated<br/>key = orderId
  OB->>DB: mark PUBLISHED

  K-->>R: OrderCreated
  R->>K: OrderRiskApproved / OrderRiskRejected

  K-->>P: OrderRiskApproved
  P->>K: PaymentAuthorized / PaymentFailed

  K-->>O: Risk/Payment lifecycle event
  O->>DB: transaction: inbox + order status + final outbox
  OB->>K: publish OrderConfirmed / OrderCancelled

  K-->>N: final order event
  N->>DB: durable inbox marks notification consumed
```

## Order State Machine

```mermaid
stateDiagram-v2
  [*] --> PENDING: CreateOrder

  PENDING --> RISK_APPROVED: OrderRiskApproved
  PENDING --> CANCELLED: OrderRiskRejected
  PENDING --> CANCELLED: user/operator cancel

  RISK_APPROVED --> CONFIRMED: PaymentAuthorized
  RISK_APPROVED --> CANCELLED: PaymentFailed
  RISK_APPROVED --> CANCELLED: user/operator cancel

  CONFIRMED --> CONFIRMED: cancellation rejected
  CANCELLED --> CANCELLED: duplicate/late events ignored

  CONFIRMED --> [*]
  CANCELLED --> [*]
```

## Transactional Outbox

```mermaid
flowchart TD
  A[Business command] --> B[PostgreSQL transaction]
  B --> C[Save business rows]
  B --> D[Save outbox_events row<br/>status = PENDING]
  B --> E[Commit]

  E --> F[OutboxPublisherService]
  F --> G{findPublishable}
  G -->|FOR UPDATE SKIP LOCKED| H[Set locked_by / locked_until]
  H --> I[KafkaProducerService.publish]

  I -->|success| J[status = PUBLISHED<br/>clear lease]
  I -->|failure| K[status = FAILED<br/>attempts + 1<br/>next_attempt_at]

  K --> L[Retry after backoff]
  L --> G

  M[Admin Outbox API] -->|retry| N[clear next_attempt_at + lease]
  N --> G
  M -->|ignore| O[status = IGNORED]
```

## Durable Inbox

```mermaid
flowchart TD
  A[Kafka message] --> B[KafkaConsumerRunner]
  B --> C[KafkaIdempotentEventProcessor]
  C --> D{claim consumer_name + event_id}

  D -->|COMPLETED| E[Duplicate skipped]
  D -->|busy lease| F[Throw retryable error]
  D -->|claimed| G[prepare deterministic result]

  G --> H[Save PREPARED result]
  H --> I[Run effect<br/>publish event or call provider]
  I --> J[Mark COMPLETED]

  I -->|process crash before completed| K[Kafka redelivery]
  K --> C
  C -->|PREPARED exists| I
```

## Retry And DLQ

```mermaid
flowchart LR
  Main[Main topic<br/>order/risk/payment]
  Retry5[retry-5s]
  Retry30[retry-30s]
  Retry5m[retry-5m]
  DLQ[dead-letter.events]
  Admin[DLQ Admin API]
  Outbox[(outbox_events)]

  Main -->|handler throws| Retry5
  Retry5 -->|delay + handler throws| Retry30
  Retry30 -->|delay + handler throws| Retry5m
  Retry5m -->|handler throws| DLQ

  Main -->|KafkaNonRetryableError| DLQ

  DLQ -->|DLQ consumer| DB[(dead_letter_events)]
  Admin -->|reprocess corrected payload| Outbox
  Outbox -->|publisher| Main
  Admin -->|ignore| DB
```

## Admin Surface

```mermaid
flowchart TD
  Request[HTTP /admin/* request]
  AuditStart[AdminAuditMiddleware attaches finish listener]
  Auth[AdminApiKeyGuard]
  Rate[AdminRateLimitGuard]
  RBAC[AdminRoles metadata check]
  Controller[Admin Controller]
  Audit[(admin_audit_events)]

  Request --> AuditStart
  AuditStart --> Auth
  Auth --> Rate
  Rate --> RBAC
  RBAC --> Controller
  Controller --> Response[HTTP response]
  Response --> Audit

  Auth -->|401| Response
  RBAC -->|403| Response
  Rate -->|429| Response

  subgraph Controllers["Current Admin Controllers"]
    DLQ[/admin/dlq]
    Outbox[/admin/outbox]
    AuditEvents[/admin/audit-events]
  end

  Controller --> DLQ
  Controller --> Outbox
  Controller --> AuditEvents
```

## Package Dependencies

```mermaid
flowchart TD
  Contracts["@kafka-playground/contracts"]
  Config["@kafka-playground/config"]
  Observability["@kafka-playground/observability"]
  Kafka["@kafka-playground/kafka"]
  Outbox["@kafka-playground/outbox"]
  Testing["@kafka-playground/testing"]

  Kafka --> Contracts
  Kafka --> Observability
  Outbox --> Kafka
  Outbox --> Observability

  Apps["app/* services"] --> Config
  Apps --> Contracts
  Apps --> Kafka
  Apps --> Outbox
  Apps --> Observability

  Testing --> Contracts
  Testing --> Kafka
```

## Local Development Flow

```mermaid
flowchart TD
  Setup[pnpm setup:local]
  Install[pnpm install]
  Infra[pnpm infra:up]
  Schemas[pnpm contracts:schemas:register]
  Migrations[pnpm --filter order-service migration:run]

  Verify[pnpm verify:local]
  StartServices[start gateway/order/risk/payment/notification]
  Readiness[wait for health and metrics endpoints]
  Gate[pnpm test:e2e:gate]
  Stop[stop child processes]

  Setup --> Install --> Infra --> Schemas --> Migrations
  Verify --> StartServices --> Readiness --> Gate --> Stop
```

## Load Baseline

```mermaid
flowchart LR
  K6[k6 order-pipeline baseline]
  Gateway[gateway-service]
  Prometheus[Prometheus snapshot]
  Postgres[(PostgreSQL pg_stat_activity)]
  Report[baseline.json]
  Saved[order-pipeline.local.json]

  K6 -->|POST /orders| Gateway
  K6 -->|summary export| Report
  Prometheus -->|Kafka lag / CPU / RAM / outbox| Report
  Postgres -->|connections samples| Report
  Report --> Saved
```
