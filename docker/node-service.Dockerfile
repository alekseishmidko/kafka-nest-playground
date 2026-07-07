# syntax=docker/dockerfile:1.7

# Этот Dockerfile собирает любой NestJS-сервис из pnpm workspace.
# Один параметризованный Dockerfile нужен потому, что gateway/order/risk/payment/
# notification используют одинаковую Node.js модель сборки. Так политика image
# остаётся единой, а сопровождать пять почти одинаковых файлов не приходится.

# Node patch version важен не только для runtime, но и для bundled npm:
# Trivy сканирует глобальные npm-зависимости внутри base image. В 22.13.1
# bundled npm содержит уязвимый node-tar 7.4.3, а 22.23.1 содержит patched 7.5.11.
ARG NODE_IMAGE=node:22.23.1-bookworm-slim
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

ARG SERVICE_NAME
ENV NODE_ENV=production
ENV SERVICE_NAME=${SERVICE_NAME}

WORKDIR /workspace

# Production container не должен содержать package managers: сервис запускается
# уже собранным JavaScript через `node`, а npm/yarn/pnpm нужны только build stage.
# Это уменьшает attack surface и убирает CVE из bundled npm/yarn/corepack tooling,
# которые Trivy иначе считает частью runtime image.
RUN rm -rf /usr/local/lib/node_modules/npm \
  /usr/local/bin/npm \
  /usr/local/bin/npx \
  /opt/yarn* \
  /usr/local/bin/yarn \
  /usr/local/bin/yarnpkg \
  /root/.cache/node/corepack

# Runtime image копирует собранный workspace и node_modules из build stage.
# Это сознательно простой вариант для текущего монорепозитория: сервисы зависят
# от workspace packages. Позже его можно ужесточить через `pnpm deploy` и
# distroless image, когда границы package exports будут окончательно закреплены.
COPY --from=build --chown=node:node /workspace /workspace

USER node

# Shell form нужна, чтобы SERVICE_NAME выбирал конкретный сервис при старте.
CMD ["sh", "-c", "node \"app/$SERVICE_NAME/dist/main.js\""]
