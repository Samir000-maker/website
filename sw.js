const CACHE_NAME = 'vibegra-pwa-v1';

self.addEventListener('install', (event) => {
  self.skipWaiting();
  event.waitUntil(caches.open(CACHE_NAME));
});

self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    try {
      const keys = await caches.keys();
      await Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)));
    } finally {
      await self.clients.claim();
    }
  })());
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;

  // Avoid caching API endpoints and sockets; keep SW minimal and non-breaking.
  if (url.pathname.startsWith('/api/')) return;

  event.respondWith((async () => {
    const cache = await caches.open(CACHE_NAME);

    // Network-first for HTML navigations so updates deploy quickly.
    if (req.mode === 'navigate' || (req.destination === 'document')) {
      try {
        const fresh = await fetch(req);
        cache.put(req, fresh.clone());
        return fresh;
      } catch {
        const cached = await cache.match(req);
        if (cached) return cached;
        return fetch(req);
      }
    }

    // Cache-first for static assets.
    const cached = await cache.match(req);
    if (cached) return cached;

    const fresh = await fetch(req);
    // Cache same-origin assets with ok responses.
    if (fresh && fresh.ok) {
      cache.put(req, fresh.clone());
    }
    return fresh;
  })());
});
