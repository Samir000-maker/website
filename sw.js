const CACHE_NAME = 'vibegra-pwa-v' + Date.now(); // Always fresh cache name
const URLS_TO_CACHE = [
  '/',
  '/index.html',
  '/manifest.webmanifest',
  '/public/favicon.png'
];

// Install - cache essential files
self.addEventListener('install', (event) => {
  console.log('Service Worker installing...');
  
  // Skip waiting to activate immediately
  self.skipWaiting();
  
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      console.log('Caching app shell');
      return cache.addAll(URLS_TO_CACHE);
    })
  );
});

// Activate - clean up old caches
self.addEventListener('activate', (event) => {
  console.log('Service Worker activating...');
  
  event.waitUntil(
    (async () => {
      try {
        // Delete all old caches
        const cacheNames = await caches.keys();
        await Promise.all(
          cacheNames
            .filter((name) => name !== CACHE_NAME)
            .map((name) => {
              console.log('Deleting old cache:', name);
              return caches.delete(name);
            })
        );
        
        // Take control of all clients immediately
        await self.clients.claim();
        
        console.log('Service Worker activated');
      } catch (err) {
        console.error('Service Worker activation error:', err);
      }
    })()
  );
});

// Fetch - network first, then cache
self.addEventListener('fetch', (event) => {
  const { request } = event;
  
  // Skip non-GET requests
  if (request.method !== 'GET') return;
  
  // Skip cross-origin requests
  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;
  
  // Skip API calls
  if (url.pathname.startsWith('/api/')) return;

  event.respondWith(
    (async () => {
      try {
        // Try network first (always get fresh content)
        const networkResponse = await fetch(request, {
          cache: 'no-cache'
        });
        
        // Cache successful responses
        if (networkResponse && networkResponse.ok) {
          const cache = await caches.open(CACHE_NAME);
          cache.put(request, networkResponse.clone());
        }
        
        return networkResponse;
      } catch (err) {
        // Network failed, try cache
        const cachedResponse = await caches.match(request);
        if (cachedResponse) {
          return cachedResponse;
        }
        
        // If no cache, return error
        throw err;
      }
    })()
  );
});

// Message handler for manual cache clearing
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
  
  if (event.data && event.data.type === 'CLEAR_CACHE') {
    event.waitUntil(
      caches.keys().then((cacheNames) => {
        return Promise.all(
          cacheNames.map((cacheName) => caches.delete(cacheName))
        );
      })
    );
  }
});
