import { defineConfig } from 'vite';
// Dev reverse proxy: the browser only talks to this origin, so Go services need no CORS.
// Targets default to the demo stack ports; override with env vars.
const t = (name: string, def: string) => process.env[name] ?? def;
const strip = (prefix: string) => (p: string) => p.replace(new RegExp(`^${prefix}`), '');
const px = (prefix: string, target: string) => ({ target, changeOrigin: true, rewrite: strip(prefix) });
export default defineConfig({
  server: {
    proxy: {
      '/api/gateway': px('/api/gateway', t('PROXY_GATEWAY', 'http://127.0.0.1:18080')),
      '/api/files': px('/api/files', t('PROXY_FILES', 'http://127.0.0.1:18085')),
      '/api/query': px('/api/query', t('PROXY_QUERY', 'http://127.0.0.1:18082')),
      '/api/connectors': px('/api/connectors', t('PROXY_CONNECTORS', 'http://127.0.0.1:8089')),
      // Pre-signed part URLs point at S3 directly; the page rewrites them to /api/s3 (dev only, avoids S3 CORS).
      '/api/s3': px('/api/s3', t('PROXY_S3', 'http://127.0.0.1:19200')),
    },
  },
  test: { environment: 'node' },
});
