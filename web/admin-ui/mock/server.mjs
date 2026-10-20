// Dev mock of the Query API. Requires any Bearer token; send "Bearer expired" to see a 401 error with trace_id.
import http from 'node:http';
import { readFileSync } from 'node:fs';
const fx = n => readFileSync(new URL(`./fixtures/${n}.json`, import.meta.url), 'utf8');
const routes = { '/v1/metrics/summary': 'metrics', '/v1/quality/summary': 'quality', '/v1/sources': 'sources', '/v1/jobs': 'jobs' };
http.createServer((req, res) => {
  const path = new URL(req.url, 'http://x').pathname;
  const send = (code, body) => { res.writeHead(code, { 'content-type': 'application/json', 'x-request-id': 'mock-req' }); res.end(body); };
  if (path === '/healthz') return send(200, fx('health')); // real API returns 503 + Health body when a dependency is down
  const auth = req.headers.authorization ?? '';
  if (!auth.startsWith('Bearer ') || auth === 'Bearer expired')
    return send(401, JSON.stringify({ code: 'AUTH_REQUIRED', message: 'invalid or missing token', details: null, trace_id: 'mock-trace-0001' }));
  if (routes[path]) return send(200, fx(routes[path]));
  send(404, JSON.stringify({ code: 'not_found', message: 'no such route', details: null, trace_id: 'mock-trace-404' }));
}).listen(8099, () => console.log('mock query-api on :8099'));
