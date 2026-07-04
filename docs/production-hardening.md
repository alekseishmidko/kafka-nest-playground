# Production Hardening

<!--
  Документ объясняет назначение production-hardening файлов. Это не runbook
  инцидента, а краткая архитектурная заметка: зачем нужны Dockerfiles, CI,
  dependency audit, security scan и pinning образов.
-->

Этот документ фиксирует базовые меры, которые нужны перед развитием проекта в
production-направлении.

## Dockerfiles

Dockerfile превращает сервис в воспроизводимый runtime artifact. Это важно,
потому что production должен запускать не локальную папку разработчика, а один
и тот же image, собранный из lockfile и конкретной версии toolchain.

- `docker/node-service.Dockerfile` собирает NestJS-сервисы по `SERVICE_NAME`.
- `app/risk-service-go/Dockerfile` собирает Go risk-service в multi-stage image.
- `.dockerignore` исключает локальные зависимости, `dist`, env-файлы и caches,
  чтобы секреты и machine-specific артефакты не попадали в image.

Примеры локальной сборки:

```bash
docker build -f docker/node-service.Dockerfile \
  --build-arg SERVICE_NAME=order-service \
  -t kafka-playground-order-service:local .

docker build -f app/risk-service-go/Dockerfile \
  -t kafka-playground-risk-service-go:local .
```

## CI Pipeline

CI нужен как общий контракт качества: код не должен попадать дальше, если он не
собирается, не проходит тесты или содержит известные high/critical уязвимости.

Workflow `.github/workflows/ci.yml` выполняет:

- `pnpm install --frozen-lockfile`, чтобы lockfile был источником истины;
- `pnpm lint`, `pnpm build`, `pnpm test`;
- `pnpm audit --audit-level high`;
- `go test ./...`;
- `govulncheck ./...`;
- Docker build для сервисных images;
- Trivy scan Docker images;
- Trivy filesystem scan для dependencies, secrets и IaC/config issues.

## Security Scan

Security scan ловит проблемы, которые обычные unit-тесты не видят:

- CVE в base images и OS packages;
- CVE в npm/go dependencies;
- случайно добавленные secrets;
- небезопасные infrastructure/config defaults.

Scan не заменяет threat modeling и code review, но даёт автоматический базовый
барьер для pull request.

## Dependency Audit

Dependency audit проверяет lockfile и Go module graph на известные уязвимости.
Это важно для Kafka/Nest/Go проекта, потому что большая часть attack surface
находится в runtime libraries: HTTP, serialization, Kafka clients, logging,
OpenTelemetry и database drivers.

## Image Pinning

Image pinning означает, что base/runtime images указываются конкретными тегами,
а не `latest`. Это нужно для воспроизводимости:

- локальный запуск и CI используют одинаковую версию image;
- обновление base image становится явным pull request;
- проще расследовать regression после обновления зависимости.

В `infrastructure/docker-compose.yml` убран `provectuslabs/kafka-ui:latest` и
уточнены tags для PostgreSQL/Redis. Для строгого production-подхода следующий
шаг — pin by digest (`image@sha256:...`) и автоматические PR-обновления через
Renovate или Dependabot.
