// Minimal Service Worker - NO CACHING
// Just enables PWA installation without any caching logic

self.addEventListener('install', (event) => {
  console.log('Service Worker installing - no cache');
  // Skip waiting to activate immediately
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  console.log('Service Worker activating - no cache');
  // Take control of all clients immediately
  event.waitUntil(self.clients.claim());
});

// No fetch handler - let browser handle all requests normally
// This means NO CACHING at all - everything goes to network

self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});
