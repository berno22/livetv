// Bumped to v3 for the Drive-mode toggle. This worker also caches navigations,
// so without the bump the car can keep serving the previous index.html and the
// toggle simply will not be there - which looks exactly like the change failing.
// v4: InMotion mode becomes the default, is renamed from Drive, CHANNELS
// stops playback, and the stage gains a loading state. This worker caches
// navigations, so without the bump a returning visitor keeps the previous
// page and every one of those changes looks like it did not deploy.
// v7: Mute and Wide added to the header, the loading indicator enlarged, and
// InMotion audio locked to the picture. Same reason as above.
// v8: the now/next guide banner. The guide is fetched from the provider and not
// cached here, but index.html changed and this worker caches navigations, so
// without the bump a returning visitor keeps the previous page and the banner
// is simply not there - which looks exactly like a broken deploy.
// v9: same reason for the mute button going green/red and its icon growing.
const CACHE_NAME = 'starlite-logos-v9';

self.addEventListener('install', event => {
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('message', event => {
  if (event.data && event.data.type === 'CACHE_LOGOS') {
    const urls = event.data.urls;
    event.waitUntil(
      caches.open(CACHE_NAME).then(cache =>
        Promise.all(urls.map(url =>
          fetch(url)
            .then(res => { if (res.ok) return cache.put(url, res); })
            .catch(() => {})
        ))
      )
    );
  }
});

self.addEventListener('fetch', event => {
  const url = event.request.url;
  if (url.includes('/logos/')) {
    event.respondWith(
      caches.match(event.request).then(cached => {
        if (cached) return cached;
        return fetch(event.request).then(res => {
          if (res.ok) {
            const clone = res.clone();
            caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
          }
          return res;
        }).catch(() => caches.match(event.request));
      })
    );
    return;
  }
  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request)
        .then(res => {
          const clone = res.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
          return res;
        })
        .catch(() => caches.match(event.request))
    );
  }
});
