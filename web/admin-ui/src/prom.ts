export interface Sample { name: string; labels: Record<string, string>; value: number }
/** Minimal Prometheus text parser (counters/gauges). Unparseable lines are skipped. */
export function parseProm(text: string): Sample[] {
  const out: Sample[] = [];
  for (const line of text.split('\n')) {
    if (!line || line.startsWith('#')) continue;
    const m = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)/.exec(line);
    if (!m) continue;
    const value = Number(m[3]);
    if (!Number.isFinite(value)) continue;
    const labels: Record<string, string> = {};
    for (const l of (m[2] ?? '').matchAll(/([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/g)) labels[l[1]] = l[2];
    out.push({ name: m[1], labels, value });
  }
  return out;
}
