// Theme preference: 'system' follows prefers-color-scheme (no attribute), 'light'/'dark' force it via <html data-theme>.
export type Theme = 'system' | 'light' | 'dark';
export const THEMES: Theme[] = ['system', 'light', 'dark'];
const KEY = 'astryon_admin_theme';

let theme: Theme = initial();
function initial(): Theme {
  try {
    const s = localStorage.getItem(KEY);
    if (s === 'light' || s === 'dark' || s === 'system') return s;
  } catch { /* storage blocked */ }
  return 'system';
}
export const getTheme = (): Theme => theme;
export function applyTheme() {
  if (theme === 'system') document.documentElement.removeAttribute('data-theme');
  else document.documentElement.setAttribute('data-theme', theme);
}
export function setTheme(t: Theme) {
  theme = t;
  try { localStorage.setItem(KEY, t); } catch { /* ignore */ }
  applyTheme();
}
