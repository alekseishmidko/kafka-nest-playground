# syntax=docker/dockerfile:1.7

# Multi-stage Go image:
# - builder содержит Go toolchain;
# - runtime image содержит только скомпилированный binary и CA certificates.
# Так image получается меньше, а build tools не попадают в production runtime.

ARG GO_IMAGE=golang:1.24-alpine3.21
ARG ALPINE_IMAGE=alpine:3.21.3

FROM ${GO_IMAGE} AS build

WORKDIR /src

COPY app/risk-service-go/go.mod app/risk-service-go/go.sum ./
RUN go mod download

COPY app/risk-service-go ./

# CGO отключён, чтобы binary запускался в минимальном Alpine runtime image.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/risk-service ./cmd/risk-service

FROM ${ALPINE_IMAGE} AS runtime

# Создаём отдельного non-root пользователя: сервису не нужны root-права внутри
# контейнера, а это уменьшает blast radius при компрометации процесса.
RUN addgroup -S app && adduser -S -G app app \
  && apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=build --chown=app:app /out/risk-service /app/risk-service

USER app

# Порт health/readiness HTTP endpoint-а risk-service-go.
EXPOSE 3002

# Запускаем только собранный binary, без shell-обёртки.
CMD ["/app/risk-service"]
