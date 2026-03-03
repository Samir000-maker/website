const CACHE_NAME = 'vibegra-pwa-v3';

self.addEventListener('install', (event) => {
  self.skipWaiting();
  event.waitUntil(caches.open(CACHE_NAME));
});

function resolveRootConfig() {
  const root = (typeof self !== 'undefined') ? self : {};
  return {
    firebaseConfig: root.__VIBE_FIREBASE_CONFIG__ || null,
    vapidKey: root.__VIBE_FCM_VAPID_KEY__ || null
  };
}

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

// ===============================
// Firebase Cloud Messaging (Web Push)
// ===============================

// Note: Firebase scripts must be imported from within the service worker.
// We use compat to match the client.
try {
  importScripts('https://www.gstatic.com/firebasejs/10.7.1/firebase-app-compat.js');
  importScripts('https://www.gstatic.com/firebasejs/10.7.1/firebase-messaging-compat.js');
  importScripts('/env-config.js');

  const { firebaseConfig } = resolveRootConfig();
  if (firebaseConfig && typeof firebase !== 'undefined') {
    if (!firebase.apps.length) {
      firebase.initializeApp(firebaseConfig);
    }

    const messaging = firebase.messaging();

    messaging.onBackgroundMessage((payload) => {
      const title = payload?.data?.title || payload?.notification?.title || 'Notification';
      const body = payload?.data?.body || payload?.notification?.body || '';
      const url = payload?.data?.url || '/';

      self.registration.showNotification(title, {
        body,
        data: { url },
        icon: '/favicon.ico'
      });
    });
  }
} catch {
  // Messaging is optional; SW should still function for caching.
}

self.addEventListener('notificationclick', (event) => {
  event.notification?.close();
  const targetUrl = event?.notification?.data?.url || '/';

  event.waitUntil((async () => {
    const allClients = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const client of allClients) {
      try {
        if (client.url && client.url.includes(targetUrl)) {
          await client.focus();
          return;
        }
      } catch { }
    }
    await self.clients.openWindow(targetUrl);
  })());
});

// Fallback for cases where Firebase Messaging doesn't invoke onBackgroundMessage.
// If a push arrives and contains displayable data, show an OS notification.
self.addEventListener('push', (event) => {
  try {
    if (!event?.data) return;
    const raw = event.data.json();

    const title = raw?.data?.title || raw?.notification?.title || 'Notification';
    const body = raw?.data?.body || raw?.notification?.body || '';
    const url = raw?.data?.url || '/';

    event.waitUntil(
      self.registration.showNotification(title, {
        body,
        data: { url },
        icon: '/favicon.ico'
      })
    );
  } catch {
    // ignore
  }
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;

  // Avoid caching API endpoints and sockets; keep SW minimal and non-breaking.
  if (url.pathname.startsWith('/api/')) return;

  // Avoid caching frequently updated boot/config scripts.
  if (url.pathname === '/social-club.js' || url.pathname === '/env-config.js' || url.pathname === '/app.js') return;

  event.respondWith((async () => {
    const cache = await caches.open(CACHE_NAME);

    const canCacheResponse = (request, response) => {
      try {
        if (!request || !response) return false;
        if ((request.method || 'GET') !== 'GET') return false;
        // Range requests often return 206 Partial Content, which Cache.put does not support.
        if (request.headers && request.headers.has('range')) return false;
        // Only cache full successful responses.
        if (response.status !== 200) return false;
        // Avoid caching opaque responses.
        if (response.type === 'opaque') return false;
        return true;
      } catch {
        return false;
      }
    };

    // Network-first for HTML navigations so updates deploy quickly.
    if (req.mode === 'navigate' || (req.destination === 'document')) {
      try {
        const fresh = await fetch(req);
        if (canCacheResponse(req, fresh)) {
          cache.put(req, fresh.clone());
        }
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
    if (fresh && canCacheResponse(req, fresh)) {
      cache.put(req, fresh.clone());
    }
    return fresh;
  })());
});
