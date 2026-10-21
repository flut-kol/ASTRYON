import { defineConfig } from '@playwright/test';
// Smoke suite against the Vite dev server in BFF mode with a mocked backend (page.route): no live AAS, services or tokens.
// Own port so it never touches the dev server on :15173.
const PORT = Number(process.env.E2E_PORT ?? 15174);
export default defineConfig({
  testDir: './e2e',
  timeout: 20_000,
  reporter: [['list']],
  use: { baseURL: `http://127.0.0.1:${PORT}`, trace: 'retain-on-failure' },
  webServer: {
    command: `npx vite --host 127.0.0.1 --port ${PORT} --strictPort`,
    url: `http://127.0.0.1:${PORT}/`,
    reuseExistingServer: false,
    env: { VITE_AUTH_MODE: 'bff' },
    timeout: 60_000,
  },
});
