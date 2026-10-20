import { defineConfig } from 'vite';
// Dev: /v1 and /healthz proxied to VITE_API_PROXY (default: mock server).
const target = process.env.VITE_API_PROXY ?? 'http://localhost:8099';
export default defineConfig({
  server: { proxy: { '/v1': target, '/healthz': target } },
  test: { environment: 'node' },
});
