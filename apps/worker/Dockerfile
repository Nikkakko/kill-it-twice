FROM node:22-alpine AS build
WORKDIR /app
ENV CI=true
RUN corepack enable
COPY . .
RUN pnpm install --frozen-lockfile=false && pnpm --filter @kill-it-twice/worker... build
FROM node:22-alpine
WORKDIR /app
RUN corepack enable
COPY --from=build /app /app
CMD ["node", "apps/worker/dist/main.js"]
