FROM node:22-bookworm-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY apps ./apps
COPY packages ./packages
COPY scripts ./scripts
RUN npm run typecheck && npm run build

FROM node:22-bookworm-slim
ENV NODE_ENV=production
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force
COPY --from=build /app/dist ./dist
COPY database ./database
USER node
CMD ["node","dist/server.mjs"]
