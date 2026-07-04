# syntax=docker/dockerfile:1.7

# Этот Dockerfile собирает любой NestJS-сервис из pnpm workspace.
# Один параметризованный Dockerfile нужен потому, что gateway/order/risk/payment/
# notification используют одинаковую Node.js модель сборки. Так политика image
# остаётся единой, а сопровождать пять почти одинаковых файлов не приходится.

ARG NODE_IMAGE=node:22.13.1-bookworm-slim
ARG PNPM_VERSION=9.15.4

FROM ${NODE_IMAGE} AS build

ARG PNPM_VERSION
ARG SERVICE_NAME

WORKDIR /workspace

# Corepack закрепляет версию package manager внутри image. Без этого Docker
# может незаметно использовать другую pnpm-версию, чем локальная разработка и CI.
RUN corepack enable && corepack prepare pnpm@${PNPM_VERSION} --activate

COPY . .

# Frozen lockfile заставляет сборку упасть, если package.json и lockfile
# разошлись. Это намеренное поведение: production images должны быть
# воспроизводимыми.
RUN pnpm install --frozen-lockfile

# Собираем только запрошенный сервис. Его package script предварительно собирает
# shared workspace packages, от которых зависит сервис.
RUN test -n "${SERVICE_NAME}" && pnpm --filter "${SERVICE_NAME}" build

FROM ${NODE_IMAGE} AS runtime

ARG PNPM_VERSION
ARG SERVICE_NAME
ENV NODE_ENV=production
ENV SERVICE_NAME=${SERVICE_NAME}

WORKDIR /workspace

RUN corepack enable && corepack prepare pnpm@${PNPM_VERSION} --activate

# Runtime image копирует собранный workspace и node_modules из build stage.
# Это сознательно простой вариант для текущего монорепозитория: сервисы зависят
# от workspace packages. Позже его можно ужесточить через `pnpm deploy` и
# distroless image, когда границы package exports будут окончательно закреплены.
COPY --from=build --chown=node:node /workspace /workspace

USER node

# Shell form нужна, чтобы SERVICE_NAME выбирал конкретный сервис при старте.
CMD ["sh", "-c", "pnpm --filter \"$SERVICE_NAME\" start:prod"]
