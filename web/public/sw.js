/**
 * UMALOGI Service Worker
 * 戦略:
 *   - 静的アセット (JS/CSS/画像/フォント) → Cache First（高速）
 *   - APIルート (/api/*) → Network First → キャッシュフォールバック
 *   - ナビゲーション (HTML) → Network First → offline.html フォールバック
 */

const CACHE_VERSION = 'umalogi-v1';
const STATIC_CACHE  = `${CACHE_VERSION}-static`;
const DATA_CACHE    = `${CACHE_VERSION}-data`;

const PRECACHE_URLS = [
  '/',
  '/offline.html',
  '/manifest.json',
  '/icon-192.png',
  '/icon-512.png',
  '/apple-touch-icon.png',
];

// ── インストール: 静的アセットをプリキャッシュ ──────────────────────────────
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) => {
      return cache.addAll(PRECACHE_URLS);
    }).then(() => self.skipWaiting())
  );
});

// ── アクティベート: 古いキャッシュを削除 ─────────────────────────────────────
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((k) => k.startsWith('umalogi-') && k !== STATIC_CACHE && k !== DATA_CACHE)
          .map((k) => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

// ── フェッチ: リクエスト戦略の振り分け ───────────────────────────────────────
self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);

  // Chrome 拡張等は無視
  if (!url.protocol.startsWith('http')) return;

  // ── API: Network First ─────────────────────────────────────────────────
  if (url.pathname.startsWith('/api/')) {
    event.respondWith(networkFirstWithCache(request, DATA_CACHE, 10_000));
    return;
  }

  // ── 静的アセット (JS/CSS/画像/フォント): Cache First ───────────────────
  if (
    request.destination === 'script' ||
    request.destination === 'style'  ||
    request.destination === 'image'  ||
    request.destination === 'font'
  ) {
    event.respondWith(cacheFirstWithNetwork(request, STATIC_CACHE));
    return;
  }

  // ── HTML ナビゲーション: Network First → offline.html ─────────────────
  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request)
        .then((res) => {
          const clone = res.clone();
          caches.open(STATIC_CACHE).then((c) => c.put(request, clone));
          return res;
        })
        .catch(() =>
          caches.match(request).then(
            (cached) => cached || caches.match('/offline.html')
          )
        )
    );
    return;
  }

  // ── その他: Network First ──────────────────────────────────────────────
  event.respondWith(networkFirstWithCache(request, DATA_CACHE, 5_000));
});

// ── ヘルパー関数 ─────────────────────────────────────────────────────────────

async function cacheFirstWithNetwork(request, cacheName) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const res = await fetch(request);
    if (res.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, res.clone());
    }
    return res;
  } catch {
    return new Response('Offline', { status: 503 });
  }
}

async function networkFirstWithCache(request, cacheName, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(request, { signal: controller.signal });
    clearTimeout(timer);
    if (res.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, res.clone());
    }
    return res;
  } catch {
    clearTimeout(timer);
    const cached = await caches.match(request);
    return cached || new Response(JSON.stringify({ error: 'offline' }), {
      status: 503,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}

// ── バックグラウンド同期（Push通知の準備） ─────────────────────────────────
self.addEventListener('message', (event) => {
  if (event.data?.type === 'SKIP_WAITING') self.skipWaiting();
});
