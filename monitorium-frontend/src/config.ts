// ─────────────────────────────────────────────────────────────────────────────
// MONITORIUM FRONTEND CONFIG
// Edit this file to add/remove tickers, sources, countries, topics.
// Everything in the UI derives from here — no other files need to change.
// ─────────────────────────────────────────────────────────────────────────────

// ── Tickers ──────────────────────────────────────────────────────────────────
export const TICKERS = {
  kz: [
    { symbol: 'KSPI',  name: 'Kaspi.kz',         sector: 'Fintech' },
    { symbol: 'HSBK',  name: 'Halyk Bank',        sector: 'Banking' },
    { symbol: 'KMGZ',  name: 'KazMunayGas',        sector: 'Oil & Gas' },
    { symbol: 'KZAP',  name: 'Kazatomprom',        sector: 'Uranium' },
    { symbol: 'KZTK',  name: 'Kazakhtelecom',      sector: 'Telecom' },
    { symbol: 'KEGC',  name: 'KEGOC',             sector: 'Energy' },
    { symbol: 'KCEL',  name: 'Kcell',             sector: 'Telecom' },
    { symbol: 'BRBK',  name: 'Bereke Bank',        sector: 'Banking' },
  ],
  global: [
    { symbol: 'AAPL',    name: 'Apple',           sector: 'Tech' },
    { symbol: 'MSFT',    name: 'Microsoft',       sector: 'Tech' },
    { symbol: 'JPM',     name: 'JPMorgan',        sector: 'Banking' },
    { symbol: 'BZ=F',    name: 'Brent Crude',     sector: 'Commodity' },
    { symbol: 'GC=F',    name: 'Gold',            sector: 'Commodity' },
    { symbol: 'SI=F',    name: 'Silver',          sector: 'Commodity' },
    { symbol: 'UX1!',    name: 'Uranium',         sector: 'Commodity' },
  ],
} as const

export type TickerSymbol = (typeof TICKERS.kz | typeof TICKERS.global)[number]['symbol']

// Flat list helpers
export const ALL_TICKERS = [...TICKERS.kz, ...TICKERS.global]
export const getTicker = (symbol: string) =>
  ALL_TICKERS.find(t => t.symbol === symbol.toUpperCase())

// ── News sources ──────────────────────────────────────────────────────────────
export const NEWS_SOURCES = [
  { id: 'kursiv',    label: 'Kursiv',     color: '#3b82f6', lang: 'en/ru' },
  { id: 'kapital',   label: 'Kapital',    color: '#8b5cf6', lang: 'ru' },
  { id: 'kase_news', label: 'KASE',       color: '#f59e0b', lang: 'ru' },
  { id: 'adilet',    label: 'Adilet',     color: '#10b981', lang: 'ru' },
  { id: 'news',      label: 'Global',     color: '#6b7280', lang: 'en' },
] as const

export type SourceId = (typeof NEWS_SOURCES)[number]['id']
export const getSource = (id: string) => NEWS_SOURCES.find(s => s.id === id)

// ── Countries ─────────────────────────────────────────────────────────────────
export const COUNTRIES = [
  { code: 'KZ', name: 'Kazakhstan',    flag: '🇰🇿', region: 'Central Asia' },
  { code: 'RU', name: 'Russia',        flag: '🇷🇺', region: 'Central Asia' },
  { code: 'CN', name: 'China',         flag: '🇨🇳', region: 'Central Asia' },
  { code: 'AZ', name: 'Azerbaijan',    flag: '🇦🇿', region: 'Central Asia' },
  { code: 'UZ', name: 'Uzbekistan',    flag: '🇺🇿', region: 'Central Asia' },
  { code: 'US', name: 'United States', flag: '🇺🇸', region: 'Americas' },
  { code: 'GB', name: 'United Kingdom',flag: '🇬🇧', region: 'Europe' },
  { code: 'DE', name: 'Germany',       flag: '🇩🇪', region: 'Europe' },
  { code: 'FR', name: 'France',        flag: '🇫🇷', region: 'Europe' },
  { code: 'NO', name: 'Norway',        flag: '🇳🇴', region: 'Europe' },
  { code: 'TR', name: 'Turkey',        flag: '🇹🇷', region: 'Europe' },
  { code: 'SA', name: 'Saudi Arabia',  flag: '🇸🇦', region: 'Middle East' },
  { code: 'IN', name: 'India',         flag: '🇮🇳', region: 'Asia' },
  { code: 'JP', name: 'Japan',         flag: '🇯🇵', region: 'Asia' },
  { code: 'KR', name: 'South Korea',   flag: '🇰🇷', region: 'Asia' },
  { code: 'NG', name: 'Nigeria',       flag: '🇳🇬', region: 'Africa' },
] as const

export type CountryCode = (typeof COUNTRIES)[number]['code']
export const getCountry = (code: string) => COUNTRIES.find(c => c.code === code)

// ── Macro indicators ──────────────────────────────────────────────────────────
export const MACRO_INDICATORS = [
  { id: 'gdp_growth',    labelKey: 'macro.gdp_growth',    unit: '%',  format: 'decimal' },
  { id: 'inflation_cpi', labelKey: 'macro.inflation_cpi', unit: '%',  format: 'decimal' },
  { id: 'unemployment',  labelKey: 'macro.unemployment',  unit: '%',  format: 'decimal' },
  { id: 'interest_rate', labelKey: 'macro.interest_rate', unit: '%',  format: 'decimal' },
] as const

export type IndicatorId = (typeof MACRO_INDICATORS)[number]['id']

// ── News topics ───────────────────────────────────────────────────────────────
export const NEWS_TOPICS = [
  { id: 'oil_price',     label: 'Oil Price' },
  { id: 'currency',      label: 'Currency / FX' },
  { id: 'privatization', label: 'Privatization' },
  { id: 'geopolitics',   label: 'Geopolitics' },
  { id: 'debt',          label: 'Debt / Bonds' },
  { id: 'banking',       label: 'Banking' },
  { id: 'mining',        label: 'Mining' },
  { id: 'sanctions',     label: 'Sanctions' },
  { id: 'earnings',      label: 'Earnings' },
  { id: 'dividend',      label: 'Dividends' },
] as const

export type TopicId = (typeof NEWS_TOPICS)[number]['id']
