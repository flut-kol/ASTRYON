// Auth is external (AAS). The bearer token is kept in memory and sessionStorage only (never in source/localStorage).
// Dev: paste a static bearer token. Later: AAS access token (optionally returned in `#token=` by VITE_LOGIN_URL flow).
const KEY = 'astryon_admin_token';
let mem: string | null = null;
export function takeTokenFromHash(hash: string): string | null {
  const m = /(?:^#|&)token=([^&]+)/.exec(hash);
  return m ? decodeURIComponent(m[1]) : null;
}
export function getToken(): string | null {
  if (mem) return mem;
  try { mem = sessionStorage.getItem(KEY); } catch { /* storage blocked */ }
  return mem;
}
export function setToken(t: string | null) {
  mem = t && t.trim() ? t.trim() : null;
  try { mem ? sessionStorage.setItem(KEY, mem) : sessionStorage.removeItem(KEY); } catch { /* ignore */ }
}
export const loginUrl: string | undefined = import.meta.env?.VITE_LOGIN_URL;

// Non-secret UI context (source filter), sessionStorage only.
const SRC = 'astryon_admin_source';
export function getSource(): string {
  try { return sessionStorage.getItem(SRC) ?? import.meta.env?.VITE_DEFAULT_SOURCE_ID ?? ''; } catch { return ''; }
}
export function setSource(s: string) { try { sessionStorage.setItem(SRC, s.trim()); } catch { /* ignore */ } }
