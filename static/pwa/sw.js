// sw.js — cache-first for static, network-first for analysis pages
const CACHE = 'luna-v1';
const STATIC_ASSETS = [
  '/', '/static/css/control_panel.css', '/static/css/control_panel_mobile.css',
  '/static/img/dexeon-logo.svg', '/static/img/icon-192.png', '/static/img/icon-512.png',
  'https://cdn.plot.ly/plotly-2.35.2.min.js'
];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(STATIC_ASSETS)));
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
});

self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);

  // Cache-first for static
  const isStatic = STATIC_ASSETS.some(s => url.href.includes(s));
  if (isStatic || url.pathname.startsWith('/static/')) {
    e.respondWith(
      caches.match(e.request).then(res => res || fetch(e.request).then(r => {
        const copy = r.clone();
        caches.open(CACHE).then(c => c.put(e.request, copy));
        return r;
      }))
    );
    return;
  }

  // Network-first for app pages (so new analysis shows up)
  if (url.pathname.startsWith('/analyze') || url.pathname.startsWith('/chart_expand')) {
    e.respondWith(
      fetch(e.request).then(r => {
        const copy = r.clone();
        caches.open(CACHE).then(c => c.put(e.request, copy));
        return r;
      }).catch(() => caches.match(e.request))
    );
    return;
  }
});
