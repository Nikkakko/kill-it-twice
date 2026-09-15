FROM node:22-alpine AS build
WORKDIR /app
ENV CI=true
RUN corepack enable
COPY . .
RUN pnpm install --frozen-lockfile=false && pnpm --filter @kill-it-twice/web... build
FROM nginx:alpine
COPY --from=build /app/apps/web/dist/web/browser /usr/share/nginx/html
COPY apps/web/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
