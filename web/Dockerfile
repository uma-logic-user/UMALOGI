# ── Next.js フロントエンド Dockerfile ─────────────────────────────────
# better-sqlite3 はネイティブアドオンのため Linux 上でコンパイルが必要。

# ----- Stage 1: 依存インストール（ネイティブビルドツール込み） -----
FROM node:20-slim AS deps
WORKDIR /app

# better-sqlite3 コンパイルに必要なビルドツール
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 make g++ \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json* ./
RUN npm ci

# ----- Stage 2: ビルド -----
FROM node:20-slim AS builder
WORKDIR /app
ENV NEXT_TELEMETRY_DISABLED=1

COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

# ----- Stage 3: 本番実行 -----
FROM node:20-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PORT=3000
ENV HOSTNAME=0.0.0.0

COPY --from=builder /app/.next       ./.next
COPY --from=builder /app/public      ./public
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./

# DB は /data にマウントされる想定（docker-compose.yml 参照）
# process.cwd() = /app → DB_PATH = /app/../data/umalogi.db = /data/umalogi.db

EXPOSE 3000
CMD ["npm", "start"]
