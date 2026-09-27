// Service Worker — corre em background, mesmo com a app/aba fechada.
// v2.7 — abre direto no Painel de Sinais ao clicar + compatível com iOS 16.4+
//        Badge ativo (silhueta branca do logo).
//        + Notifica janelas abertas quando chega push (auto-refresh do painel).

const CACHE_NAME = 'painel-sinais-v12';
const APP_SHELL = ['/', '/index.html', '/manifest.json'];

// ============ INSTALL ============
self.addEventListener('install', (event) => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL)).catch(() => {})
  );
});

// ============ ACTIVATE ============
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

// ============ PUSH ============
// Recebe o push do servidor, mostra a notificação E avisa todas as janelas abertas
// (para o painel de sinais se atualizar automaticamente).
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
    renotify: true,
    data: {
      url: String((data.data && data.data.url) || '/'),
      symbol: String((data.data && data.data.symbol) || ''),
      mode: String((data.data && data.data.mode) || ''),
      tipo: String((data.data && data.data.tipo) || '')
    }
  };

  const titulo = String(data.title || '🔔 Novo sinal');

  event.waitUntil(
    Promise.all([
      // 1) Mostra a notificação no sistema
      self.registration.showNotification(titulo, options),

      // 2) Avisa TODAS as janelas abertas desta app que chegou um sinal novo
      self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientsArr) => {
        clientsArr.forEach((c) => {
          try {
            c.postMessage({
              action: 'newSignal',
              payload: {
                symbol: options.data.symbol,
                mode: options.data.mode,
                tipo: options.data.tipo,
                title: titulo,
                body: String(data.body || '')
              }
            });
          } catch (err) {
            // ignora janelas que já não respondem
          }
        });
      })
    ])
  );
});

// ============ NOTIFICATION CLICK ============
// Quando o utilizador toca na notificação:
// - Se já existe uma janela da app aberta → foca-a e envia mensagem para abrir o painel
// - Caso contrário → abre uma nova janela já no painel de sinais
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.url) || '/?open=signals';

  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientsArr) => {
      const existing = clientsArr.find((c) => c.url.includes(self.location.origin));

      if (existing) {
        // Tenta enviar mensagem primeiro (sem recarregar a página)
        try {
          existing.postMessage({ action: 'openSignals' });
        } catch (e) {}
        // Foca a janela existente
        return existing.focus().catch(() => {
          if (self.clients.openWindow) return self.clients.openWindow(targetUrl);
        });
      }

      // Se não há janela aberta, abre nova com ?open=signals
      if (self.clients.openWindow) return self.clients.openWindow(targetUrl);
    })
  );
});

// ============ PUSH SUBSCRIPTION CHANGE ============
// Se o browser renovar a subscrição, avisa as janelas para re-registarem no servidor.
self.addEventListener('pushsubscriptionchange', (event) => {
  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientsArr) => {
      clientsArr.forEach((c) => {
        try { c.postMessage({ action: 'pushSubscriptionChanged' }); } catch (e) {}
      });
    })
  );
});
