// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { errorText, fmtBytes, fmtTime, human, humanize, timeEl } from './labels';
import { setLang } from './i18n';

beforeEach(() => { localStorage.clear(); setLang('en'); });
afterEach(() => { setLang('en'); });

describe('human', () => {
  it('maps known enum values per language', () => {
    expect(human('status', 'SUCCEEDED')).toBe('Succeeded');
    expect(human('sourceType', 'credentials_bundle')).toBe('Credentials bundle');
    expect(human('label', 'contains_pii')).toBe('Contains personal data');
    setLang('ua');
    expect(human('status', 'SUCCEEDED')).toBe('Успішно');
    expect(human('sensitivity', 'restricted')).toBe('Обмежена');
    expect(human('reason', 'ERR_EVENT_TIME_FUTURE')).toBe('Час події в майбутньому');
  });
  it('humanizes unknown values and keeps empty empty', () => {
    expect(human('status', 'SOME_NEW_STATE')).toBe('Some new state');
    expect(human('reason', 'ERR_FOO_BAR')).toBe('Foo bar');
    expect(human('flag', undefined)).toBe('');
    expect(human('flag', null)).toBe('');
    expect(human('flag', '')).toBe('');
  });
});

describe('humanize', () => {
  it('strips ERR_, replaces separators and capitalizes', () => {
    expect(humanize('ERR_PAYLOAD_TOO_LARGE')).toBe('Payload too large');
    expect(humanize('some.dotted_code')).toBe('Some dotted code');
    expect(humanize('___')).toBe('___');
  });
});

describe('errorText', () => {
  it('prefers the error code, in both languages', () => {
    expect(errorText('forbidden', 403)).toBe('You do not have permission for this action.');
    setLang('ua');
    expect(errorText('forbidden', 403)).toBe('У вас немає прав для цієї дії.');
  });
  it('accepts upper-case codes', () => {
    expect(errorText('FORBIDDEN', 0)).toBe('You do not have permission for this action.');
    expect(errorText('INVALID_CURSOR', 400)).toBe('The page cursor is invalid or expired.');
  });
  it('falls back to the HTTP status, then undefined', () => {
    expect(errorText('http_503', 503)).toBe('The service is temporarily unavailable.');
    expect(errorText('weird', 429)).toBe('Too many requests. Try again in a moment.');
    setLang('ua');
    expect(errorText('http_502', 502)).toBe('Внутрішній сервіс повернув помилку.');
    expect(errorText('weird', 418)).toBeUndefined();
    expect(errorText('weird', 0)).toBeUndefined();
  });
});

describe('time', () => {
  const iso = '2026-10-21T10:00:00Z';
  it('formats valid times per locale and passes invalid/empty through', () => {
    const en = fmtTime(iso);
    expect(en).toMatch(/2026/);
    setLang('ua');
    const ua = fmtTime(iso);
    expect(ua).toMatch(/2026/);
    expect(ua).not.toBe(en);
    expect(fmtTime('not a date')).toBe('not a date');
    expect(fmtTime('')).toBe('');
    expect(fmtTime(undefined)).toBe('');
  });
  it('timeEl keeps the raw value in datetime/title', () => {
    const el = timeEl(iso) as HTMLTimeElement;
    expect(el.tagName).toBe('TIME');
    expect(el.dateTime).toBe(iso);
    expect(el.title).toBe(iso);
    expect(el.textContent).toBe(fmtTime(iso));
    expect(timeEl('garbage')).toBe('garbage');
    expect(timeEl('')).toBe('');
    expect(timeEl(null)).toBe('');
  });
});

describe('fmtBytes', () => {
  it('scales units', () => {
    expect(fmtBytes(0)).toBe('0 B');
    expect(fmtBytes(512)).toBe('512 B');
    expect(fmtBytes(2048)).toBe('2.0 KiB');
    expect(fmtBytes(5 * 1024 * 1024)).toBe('5.0 MiB');
    expect(fmtBytes(300 * 1024 * 1024)).toBe('300 MiB');
    expect(fmtBytes(Number.NaN)).toBe('');
  });
  it('works in Ukrainian (units fall back to the source text when untranslated)', () => {
    setLang('ua');
    expect(fmtBytes(2048)).toMatch(/^2\.0 \S+$/);
  });
});
