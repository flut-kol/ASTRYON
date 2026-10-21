// Safe DOM builder: server strings only ever reach the DOM via textContent / text nodes (never innerHTML).
type Child = Node | string | number | boolean | null | undefined;
export function h<K extends keyof HTMLElementTagNameMap>(
  tag: K, attrs: Record<string, string | boolean | ((e: Event) => void)> = {}, ...kids: (Child | Child[])[]
): HTMLElementTagNameMap[K] {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (typeof v === 'function') el.addEventListener(k.replace(/^on/, ''), v);
    else if (typeof v === 'boolean') { if (v) el.setAttribute(k, ''); }
    else if (k === 'class') el.className = v;
    else el.setAttribute(k, v);
  }
  for (const k of kids.flat()) {
    if (k == null || k === false) continue;
    el.append(k instanceof Node ? k : String(k));
  }
  return el;
}
export function clear(el: Element) { while (el.firstChild) el.removeChild(el.firstChild); }

const NS = 'http://www.w3.org/2000/svg';
/** Decorative stroke icon from static path data (24x24 grid); hidden from assistive tech, built without innerHTML. */
export function svg(paths: string[], size = 24): SVGSVGElement {
  const el = document.createElementNS(NS, 'svg');
  for (const [k, v] of Object.entries({ viewBox: '0 0 24 24', width: String(size), height: String(size), fill: 'none', stroke: 'currentColor', 'stroke-width': '1.8', 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'aria-hidden': 'true', focusable: 'false' })) el.setAttribute(k, v);
  for (const d of paths) { const p = document.createElementNS(NS, 'path'); p.setAttribute('d', d); el.append(p); }
  return el;
}
