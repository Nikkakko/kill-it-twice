FROM node:22-alpine AS build
WORKDIR /app
ENV CI=true
RUN corepack enable
COPY . .
RUN pnpm install --frozen-lockfile=false && pnpm --filter @kill-it-twice/api... build
FROM node:22-alpine
WORKDIR /app
RUN corepack enable
COPY --from=build /app /app
EXPOSE 3000
CMD ["node", "apps/api/dist/main.js"]
