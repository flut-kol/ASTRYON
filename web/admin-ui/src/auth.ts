// Auth is external (AAS). Token is kept in sessionStorage only; nothing is persisted in the repo.
// VITE_LOGIN_URL: if set, "Sign in" redirects there; the AAS flow is expected to return to
// `#token=<access_token>` (fragment). Otherwise a dev paste-token field is shown.
const KEY = 'astryon_admin_token';
export function takeTokenFromHash(hash: string): string | null {
  const m = /(?:^#|&)token=([^&]+)/.exec(hash);
  return m ? decodeURIComponent(m[1]) : null;
}
export function getToken(): string | null { try { return sessionStorage.getItem(KEY); } catch { return null; } }
export function setToken(t: string | null) {
  try { t ? sessionStorage.setItem(KEY, t.trim()) : sessionStorage.removeItem(KEY); } catch { /* ignore */ }
}
export const loginUrl: string | undefined = import.meta.env.VITE_LOGIN_URL;
