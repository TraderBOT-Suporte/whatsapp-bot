// Service Worker — corre em background, mesmo com a app/aba fechada.
// v2.2 — compatível com iOS 16.4+ (data achatada + navigate no click).
//        Badge removido (usa o ícone da app via manifest.json).

const CACHE_NAME = 'painel-sinais-v4';
const APP_SHELL = ['/', '/index.html', '/manifest.json'];

self.addEventListener('install', (event) => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL)).catch(() => {})
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('push', (event) => {
  let data = { title: '🔔 Novo sinal', body: 'Verifique o painel.' };
  try {
    if (event.data) data = event.data.json();
  } catch (e) {
    if (event.data) data.body = event.data.text();
  }

  const options = {
    icon: '/icon-192.png',
    badge: '/badge-72.png',
    tag: String(data.tag || 'sinal'),
    // badge removido: Android usa o ícone da app do manifest.json
    tag: String(data.tag || 'sinal'),
    data: {
      url: String((data.data && data.data.url) || '/'),
      symbol: String((data.data && data.data.symbol) || ''),
      mode: String((data.data && data.data.mode) || ''),
      tipo: String((data.data && data.data.tipo) || '')
    }
  };

  event.waitUntil(
    self.registration.showNotification(String(data.title || '🔔 Novo sinal'), options)
  );
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.url) || '/';

  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientsArr) => {
      const existing = clientsArr.find((c) => c.url.includes(self.location.origin));
      if (existing) {
        return existing.focus().then((c) => {
          if (c && typeof c.navigate === 'function') return c.navigate(targetUrl);
        });
      }
      return self.clients.openWindow(targetUrl);
    })
  );
});
