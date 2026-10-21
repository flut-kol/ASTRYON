import { defineConfig } from 'vite';
// Dev reverse proxy: the browser only talks to this origin, so Go services need no CORS.
// Targets default to the demo stack ports; override with env vars.
const t = (name: string, def: string) => process.env[name] ?? def;
const strip = (prefix: string) => (p: string) => p.replace(new RegExp(`^${prefix}`), '');
// DEV ONLY: demo-up.sh exports ADMIN_UI_DEV_TOKEN; the proxy adds it server-side as a bearer token when the request has none
// (the manual token box still overrides). Read at server start, no VITE_ prefix, so never bundled into client JS.
const devToken = process.env.ADMIN_UI_DEV_TOKEN;
const px = (prefix: string, target: string, auth = true) => ({
  target, changeOrigin: true, rewrite: strip(prefix),
  configure: (proxy: any) => {
    if (!devToken || !auth) return;
    proxy.on('proxyReq', (req: any) => { if (!req.getHeader('authorization')) req.setHeader('authorization', `Bearer ${devToken}`); });
  },
});
// BFF mode (VITE_AUTH_MODE=bff): the browser holds only the session cookie; /auth and /api/<service> go to services/admin-bff,
// which adds the bearer. No dev token is injected. Cookies are host-scoped, so the BFF cookie set on 127.0.0.1:18090 reaches :15173 too.
const bff = process.env.VITE_AUTH_MODE === 'bff';
const bffTarget = t('PROXY_BFF', 'http://127.0.0.1:18090');
const bffProxy = {
  '/auth': { target: bffTarget, changeOrigin: false },
  '/api/s3': px('/api/s3', t('PROXY_S3', 'http://127.0.0.1:19200'), false),
  '/api': { target: bffTarget, changeOrigin: false },
};
export default defineConfig({
  server: {
    proxy: bff ? bffProxy : {
      '/api/gateway': px('/api/gateway', t('PROXY_GATEWAY', 'http://127.0.0.1:18080')),
      '/api/files': px('/api/files', t('PROXY_FILES', 'http://127.0.0.1:18085')),
      '/api/query': px('/api/query', t('PROXY_QUERY', 'http://127.0.0.1:18082')),
      '/api/connectors': px('/api/connectors', t('PROXY_CONNECTORS', 'http://127.0.0.1:18088')),
      '/api/exports': px('/api/exports', t('PROXY_EXPORTS', 'http://127.0.0.1:18083')),
      '/api/quarantine': px('/api/quarantine', t('PROXY_QUARANTINE', 'http://127.0.0.1:18086')),
      '/api/replays': px('/api/replays', t('PROXY_REPLAYS', 'http://127.0.0.1:18087')),
      // Pre-signed part URLs point at S3 directly; the page rewrites them to /api/s3 (dev only, avoids S3 CORS).
      '/api/s3': px('/api/s3', t('PROXY_S3', 'http://127.0.0.1:19200'), false),
    },
  },
  test: { environment: 'node', include: ['src/**/*.test.ts'] }, // e2e/ is Playwright's
});
