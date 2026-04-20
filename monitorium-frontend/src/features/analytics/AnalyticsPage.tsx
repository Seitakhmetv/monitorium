import { useState, useMemo } from 'react'
import { useQuery, useQueries } from '@tanstack/react-query'
import {
  LineChart, Line, BarChart, Bar, ComposedChart,
  ScatterChart, Scatter, ZAxis,
  XAxis, YAxis, Tooltip, ResponsiveContainer, Legend,
  CartesianGrid, ReferenceLine, Cell,
} from 'recharts'
import {
  TrendingUp, BarChart2, Grid3x3, CircleDot, BarChartHorizontal,
  Plus, X, Settings, GripVertical, Activity,
} from 'lucide-react'
import { getStatements, getFinTickers } from '../../api/fundamentals'
import type { StatementYear } from '../../api/fundamentals'
import { Spinner } from '../../components/ui'

/* ─── types ───────────────────────────────────────────────── */

type ChartType = 'margin' | 'fcf' | 'leverage' | 'bubble' | 'cagr' | 'custom'

const METRICS = [
  { key: 'revenue',              label: 'Revenue'        },
  { key: 'gross_profit',         label: 'Gross Profit'   },
  { key: 'operating_profit',     label: 'Op. Profit'     },
  { key: 'ebitda',               label: 'EBITDA'         },
  { key: 'net_income',           label: 'Net Income'     },
  { key: 'free_cash_flow',       label: 'Free Cash Flow' },
  { key: 'operating_cash_flow',  label: 'Op. Cash Flow'  },
  { key: 'total_assets',         label: 'Total Assets'   },
  { key: 'total_debt',           label: 'Total Debt'     },
  { key: 'total_equity',         label: 'Total Equity'   },
  { key: 'cash_and_equivalents', label: 'Cash'           },
  { key: 'capex',                label: 'CapEx'          },
  { key: 'eps',                  label: 'EPS'            },
  { key: 'net_interest_income',  label: 'Net Int. Inc.'  },
] as const

type MetricKey = typeof METRICS[number]['key']

interface Block {
  id:         string
  type:       ChartType
  tickers:    string[]
  size:       'half' | 'full'
  metric:     MetricKey
  chartStyle: 'line' | 'bar'
}

type DataEntry = { ticker: string; years: StatementYear[] }

/* ─── block helpers ───────────────────────────────────────── */

function mkBlock(): Block {
  return {
    id:         Math.random().toString(36).slice(2, 10),
    type:       'margin',
    tickers:    [],
    size:       'half',
    metric:     'revenue',
    chartStyle: 'line',
  }
}

const LS_KEY = 'analytics-blocks-v2'

function loadBlocks(): Block[] {
  try {
    const s = localStorage.getItem(LS_KEY)
    if (s) return JSON.parse(s) as Block[]
  } catch { /* ignore */ }
  return []
}

function saveBlocks(blocks: Block[]) {
  try { localStorage.setItem(LS_KEY, JSON.stringify(blocks)) } catch { /* ignore */ }
}

/* ─── data helpers ────────────────────────────────────────── */

function toBn(val: number | null | undefined, units: string): number | null {
  if (val == null) return null
  const u = units.toLowerCase()
  if (u.includes('thousand')) return val / 1_000_000
  if (u.includes('million'))  return val / 1_000
  return val
}

const BANK_TICKERS = new Set(['HSBK', 'KSPI', 'ASBN', 'CCBN', 'AKZM'])

function ebitda(y: StatementYear, ticker: string): number | null {
  if (y.ebitda != null) return y.ebitda
  if (BANK_TICKERS.has(ticker)) return null
  return y.operating_profit
}

function rev(y: StatementYear): number | null {
  return y.net_interest_income ?? y.revenue
}

function pct(num: number | null | undefined, denom: number | null | undefined): number | null {
  if (!num || !denom) return null
  return parseFloat(((num / denom) * 100).toFixed(1))
}

function fmtPct(n: number | null): string {
  return n == null ? '—' : `${n.toFixed(1)}%`
}

function fmt1(n: number | null): string {
  return n == null ? '—' : n.toFixed(1)
}

function cagrPct(first: number, last: number, years: number): number | null {
  if (years <= 0 || first <= 0 || last <= 0) return null
  return parseFloat(((Math.pow(last / first, 1 / years) - 1) * 100).toFixed(1))
}

function leverageColor(ratio: number | null): string {
  if (ratio == null) return '#1f2937'
  if (ratio < 0)  return '#7c3aed'
  if (ratio < 1)  return '#15803d'
  if (ratio < 2)  return '#65a30d'
  if (ratio < 3)  return '#ca8a04'
  if (ratio < 4)  return '#ea580c'
  return '#dc2626'
}

const EPS_METRICS = new Set<MetricKey>(['eps'])

function metricBn(y: StatementYear, metric: MetricKey): number | null {
  const raw = y[metric as keyof StatementYear]
  if (raw == null || typeof raw !== 'number') return null
  if (EPS_METRICS.has(metric)) return raw
  return toBn(raw, y.units ?? 'millions')
}

/* ─── colors ──────────────────────────────────────────────── */

const TICKER_COLORS: Record<string, string> = {
  AIRA: '#38bdf8', AKZM: '#fb923c', ASBN: '#a78bfa', BAST: '#f472b6',
  BSUL: '#34d399', CCBN: '#fbbf24', HSBK: '#60a5fa', KCEL: '#4ade80',
  KEGC: '#f87171', KMGD: '#c084fc', KMGZ: '#22d3ee', KSPI: '#facc15',
  KZAP: '#86efac', KZTK: '#fca5a5', KZTO: '#93c5fd', RAHT: '#d9f99d',
}
const COLOR_CYCLE = [
  '#38bdf8','#fb923c','#a78bfa','#f472b6',
  '#34d399','#fbbf24','#60a5fa','#4ade80',
  '#f87171','#c084fc','#22d3ee','#facc15',
]
function tColor(ticker: string, idx: number): string {
  return TICKER_COLORS[ticker] ?? COLOR_CYCLE[idx % COLOR_CYCLE.length]
}

/* ─── shared tooltip / axis ───────────────────────────────── */

const TT: React.CSSProperties = {
  background:   'var(--color-surface)',
  border:       '1px solid var(--color-border)',
  borderRadius: 6,
  fontSize:     12,
  color:        'var(--color-text)',
}
const AX = { fontSize: 11, fill: 'var(--color-muted)' }

/* ─── chart: margin trends ────────────────────────────────── */

function MarginChart({ tickers, allData }: { tickers: string[]; allData: DataEntry[] }) {
  const target = tickers.length
    ? allData.filter(d => tickers.includes(d.ticker))
    : allData.slice(0, 1)
  if (!target.length) return <Spinner />

  if (target.length > 1) {
    const allYears = [...new Set(target.flatMap(d => d.years.map(y => y.fiscal_year)))].sort((a,b) => a-b)
    const data = allYears.map(yr => {
      const pt: Record<string, number | null | number> = { year: yr }
      target.forEach(({ ticker, years }) => {
        const y = years.find(yy => yy.fiscal_year === yr)
        pt[ticker] = y ? pct(y.net_income, rev(y)) : null
      })
      return pt
    })
    return (
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
          <XAxis dataKey="year" tick={AX} axisLine={false} tickLine={false} />
          <YAxis tickFormatter={v => `${v}%`} tick={AX} axisLine={false} tickLine={false} width={40} />
          <ReferenceLine y={0} stroke="var(--color-border)" />
          <Tooltip contentStyle={TT} formatter={(v: unknown) => [`${Number(v).toFixed(1)}%`]} />
          <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 11, color: 'var(--color-muted)' }} />
          {target.map(({ ticker }, idx) => (
            <Line key={ticker} type="monotone" dataKey={ticker}
              name={`${ticker} net`} stroke={tColor(ticker, idx)}
              strokeWidth={2} dot={false} connectNulls />
          ))}
        </LineChart>
      </ResponsiveContainer>
    )
  }

  const { ticker, years } = target[0]
  const isBank      = BANK_TICKERS.has(ticker)
  const hasFallback = !isBank && years.some(y => y.ebitda == null)
  const data = [...years].sort((a,b) => a.fiscal_year - b.fiscal_year).map(y => ({
    year:     y.fiscal_year,
    ebitda_m: pct(ebitda(y, ticker), rev(y)),
    op_m:     pct(y.operating_profit, rev(y)),
    net_m:    pct(y.net_income, rev(y)),
  }))

  return (
    <ResponsiveContainer width="100%" height={220}>
      <LineChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
        <XAxis dataKey="year" tick={AX} axisLine={false} tickLine={false} />
        <YAxis tickFormatter={v => `${v}%`} tick={AX} axisLine={false} tickLine={false} width={40} />
        <ReferenceLine y={0} stroke="var(--color-border)" />
        <Tooltip contentStyle={TT} formatter={(v: unknown) => [`${Number(v).toFixed(1)}%`]} />
        <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 11, color: 'var(--color-muted)' }} />
        {!isBank && (
          <Line type="monotone" dataKey="ebitda_m"
            name={hasFallback ? 'EBIT margin*' : 'EBITDA margin'}
            stroke="#fbbf24" strokeWidth={2} dot={false} connectNulls />
        )}
        <Line type="monotone" dataKey="op_m"  name="Op. margin"  stroke="#60a5fa" strokeWidth={2} dot={false} connectNulls />
        <Line type="monotone" dataKey="net_m" name="Net margin"  stroke="#4ade80" strokeWidth={2} dot={false} connectNulls />
      </LineChart>
    </ResponsiveContainer>
  )
}

/* ─── chart: FCF vs net income ────────────────────────────── */

function FcfChart({ tickers, allData }: { tickers: string[]; allData: DataEntry[] }) {
  const tk    = tickers[0] ?? allData[0]?.ticker
  const years = allData.find(d => d.ticker === tk)?.years ?? []
  const data  = [...years].sort((a,b) => a.fiscal_year - b.fiscal_year).map(y => {
    const u = y.units ?? 'millions'
    return {
      year:       y.fiscal_year,
      net_income: toBn(y.net_income, u),
      fcf: toBn(
        y.free_cash_flow ?? (
          y.operating_cash_flow != null && y.capex != null
            ? y.operating_cash_flow + y.capex
            : null
        ), u),
    }
  })
  if (!data.length) return <Spinner />
  return (
    <ResponsiveContainer width="100%" height={220}>
      <ComposedChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
        <XAxis dataKey="year" tick={AX} axisLine={false} tickLine={false} />
        <YAxis tickFormatter={v => `${Number(v).toFixed(0)}B`} tick={AX} axisLine={false} tickLine={false} width={44} />
        <ReferenceLine y={0} stroke="var(--color-border)" />
        <Tooltip contentStyle={TT} formatter={(v: unknown) => [`${Number(v).toFixed(1)}B KZT`]} />
        <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 11, color: 'var(--color-muted)' }} />
        <Bar  dataKey="net_income" name="Net Income"    fill="#4ade80" fillOpacity={0.7} radius={[3,3,0,0]} />
        <Line type="monotone" dataKey="fcf" name="Free Cash Flow"
          stroke="#f87171" strokeWidth={2} dot={{ r: 3, fill: '#f87171' }} connectNulls />
      </ComposedChart>
    </ResponsiveContainer>
  )
}

/* ─── chart: leverage heatmap ─────────────────────────────── */

function LeverageHeatmap({ allData }: { allData: DataEntry[] }) {
  const allYears = useMemo(() => {
    const s = new Set<number>()
    allData.forEach(d => d.years.forEach(y => s.add(y.fiscal_year)))
    return [...s].sort((a,b) => a-b)
  }, [allData])

  const matrix = useMemo(() => {
    const m: Record<string, Record<number, number | null>> = {}
    allData.forEach(({ ticker, years }) => {
      m[ticker] = {}
      years.forEach(y => {
        const eb = ebitda(y, ticker)
        m[ticker][y.fiscal_year] = eb && y.total_debt != null
          ? parseFloat((y.total_debt / eb).toFixed(2))
          : null
      })
    })
    return m
  }, [allData])

  const tickers = allData.map(d => d.ticker).sort()

  return (
    <div className="overflow-x-auto">
      <table className="text-xs border-collapse" style={{ color: 'var(--color-text)' }}>
        <thead>
          <tr>
            <th className="text-left pr-3 py-1 font-medium sticky left-0"
              style={{ color: 'var(--color-muted)', background: 'var(--color-surface)', minWidth: 52 }}>
              Ticker
            </th>
            {allYears.map(y => (
              <th key={y} className="px-1 py-1 font-medium text-center"
                style={{ color: 'var(--color-muted)', minWidth: 38 }}>{y}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {tickers.map(ticker => (
            <tr key={ticker}>
              <td className="pr-3 py-0.5 font-semibold sticky left-0"
                style={{ color: 'var(--color-heading)', background: 'var(--color-surface)' }}>
                {ticker}
              </td>
              {allYears.map(y => {
                const ratio = matrix[ticker]?.[y] ?? null
                return (
                  <td key={y} className="px-0.5 py-0.5">
                    <div className="rounded text-center tabular font-mono"
                      style={{
                        background: leverageColor(ratio),
                        color:      ratio == null ? 'transparent' : '#fff',
                        fontSize:   10,
                        padding:    '2px 4px',
                        minWidth:   32,
                        opacity:    ratio == null ? 0.15 : 1,
                      }}
                      title={ratio != null ? `${ticker} FY${y}: ${ratio}x` : 'No data'}>
                      {ratio != null ? `${ratio}x` : '·'}
                    </div>
                  </td>
                )
              })}
            </tr>
          ))}
        </tbody>
      </table>
      <div className="flex flex-wrap gap-3 mt-3">
        {[['#15803d','< 1x'],['#65a30d','1–2x'],['#ca8a04','2–3x'],['#ea580c','3–4x'],['#dc2626','> 4x'],['#7c3aed','Neg.']].map(([c, l]) => (
          <div key={l} className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded-sm" style={{ background: c }} />
            <span className="text-xs" style={{ color: 'var(--color-muted)' }}>{l}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

/* ─── chart: bubble (EBITDA margin vs revenue growth) ──────── */

function BubbleChart({ allData }: { allData: DataEntry[] }) {
  const points = useMemo(() => allData.flatMap(({ ticker, years }) => {
    const sorted = [...years].sort((a,b) => a.fiscal_year - b.fiscal_year)
    const last = sorted.at(-1), prev = sorted.at(-2)
    if (!last) return []
    const u = last.units ?? 'millions'
    const revenue  = rev(last), prevRev = prev ? rev(prev) : null
    const ebitdaM  = pct(ebitda(last, ticker), revenue)
    const revGrw   = revenue && prevRev ? pct(revenue - prevRev, prevRev) : null
    const assetsBn = toBn(last.total_assets, u) ?? 1
    if (ebitdaM == null || revGrw == null) return []
    return [{ ticker, ebitda_margin: ebitdaM, revenue_growth: revGrw, assets_bn: assetsBn, year: last.fiscal_year }]
  }), [allData])

  if (!points.length) return <Spinner />

  return (
    <ResponsiveContainer width="100%" height={300}>
      <ScatterChart margin={{ top: 16, right: 24, left: 0, bottom: 24 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
        <XAxis type="number" dataKey="revenue_growth" name="Revenue Growth"
          tickFormatter={v => `${v}%`} tick={AX} axisLine={false} tickLine={false}
          label={{ value: 'Revenue YoY %', position: 'insideBottom', offset: -16, style: { fontSize: 11, fill: 'var(--color-muted)' } }} />
        <YAxis type="number" dataKey="ebitda_margin" name="EBITDA Margin"
          tickFormatter={v => `${v}%`} tick={AX} axisLine={false} tickLine={false} width={42}
          label={{ value: 'EBITDA Margin %', angle: -90, position: 'insideLeft', style: { fontSize: 11, fill: 'var(--color-muted)' } }} />
        <ZAxis type="number" dataKey="assets_bn" range={[200, 2400]} />
        <ReferenceLine x={0} stroke="var(--color-border)" strokeDasharray="4 4" />
        <ReferenceLine y={0} stroke="var(--color-border)" strokeDasharray="4 4" />
        <Tooltip contentStyle={TT} content={({ payload }) => {
          if (!payload?.length) return null
          const d = payload[0].payload
          return (
            <div className="px-3 py-2" style={TT}>
              <div className="font-bold mb-1" style={{ color: TICKER_COLORS[d.ticker] ?? '#fff' }}>{d.ticker} FY{d.year}</div>
              <div>EBITDA margin: {fmtPct(d.ebitda_margin)}</div>
              <div>Revenue growth: {fmtPct(d.revenue_growth)}</div>
              <div>Assets: {fmt1(d.assets_bn)}B KZT</div>
            </div>
          )
        }} />
        <Scatter data={points} label={{ dataKey: 'ticker', position: 'top', style: { fontSize: 10, fill: 'var(--color-muted)' } }}>
          {points.map((p, i) => <Cell key={p.ticker} fill={tColor(p.ticker, i)} fillOpacity={0.85} />)}
        </Scatter>
      </ScatterChart>
    </ResponsiveContainer>
  )
}

/* ─── chart: revenue CAGR bar ─────────────────────────────── */

function CagrBar({ allData }: { allData: DataEntry[] }) {
  const data = useMemo(() => allData.flatMap(({ ticker, years }) => {
    const sorted = [...years].sort((a,b) => a.fiscal_year - b.fiscal_year)
    if (sorted.length < 2) return []
    const first = rev(sorted[0]), last = rev(sorted.at(-1)!)
    const n = sorted.at(-1)!.fiscal_year - sorted[0].fiscal_year
    const c = cagrPct(first ?? 0, last ?? 0, n)
    if (c == null) return []
    return [{ ticker, cagr: c, from: sorted[0].fiscal_year, to: sorted.at(-1)!.fiscal_year, years: n }]
  }).sort((a,b) => b.cagr - a.cagr), [allData])

  if (!data.length) return <Spinner />

  return (
    <ResponsiveContainer width="100%" height={Math.max(200, data.length * 30 + 40)}>
      <BarChart data={data} layout="vertical" margin={{ top: 4, right: 48, left: 4, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" horizontal={false} />
        <XAxis type="number" tickFormatter={v => `${v}%`} tick={AX} axisLine={false} tickLine={false} />
        <YAxis type="category" dataKey="ticker"
          tick={{ ...AX, fill: 'var(--color-heading)', fontWeight: 600 }}
          axisLine={false} tickLine={false} width={40} />
        <ReferenceLine x={0} stroke="var(--color-border)" />
        <Tooltip contentStyle={TT} content={({ payload }) => {
          if (!payload?.length) return null
          const d = payload[0].payload
          return (
            <div className="px-3 py-2" style={TT}>
              <div className="font-bold mb-1" style={{ color: tColor(d.ticker, 0) }}>{d.ticker}</div>
              <div>Revenue CAGR: {fmtPct(d.cagr)}</div>
              <div style={{ color: 'var(--color-muted)' }}>FY{d.from}–FY{d.to} ({d.years}y)</div>
            </div>
          )
        }} />
        <Bar dataKey="cagr" name="Revenue CAGR" radius={[0,4,4,0]}
          label={{ position: 'right', formatter: (v: unknown) => `${Number(v).toFixed(1)}%`, style: { fontSize: 10, fill: 'var(--color-muted)' } }}>
          {data.map((d, i) => <Cell key={d.ticker} fill={d.cagr >= 0 ? tColor(d.ticker, i) : '#f87171'} fillOpacity={0.85} />)}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

/* ─── chart: custom metric ────────────────────────────────── */

function CustomMetricChart({
  metric, chartStyle, tickers, allData,
}: {
  metric: MetricKey; chartStyle: 'line' | 'bar'; tickers: string[]; allData: DataEntry[]
}) {
  const target = tickers.length ? allData.filter(d => tickers.includes(d.ticker)) : []
  if (!target.length) {
    return (
      <div className="h-[180px] flex flex-col items-center justify-center gap-2" style={{ color: 'var(--color-muted)' }}>
        <Settings size={20} style={{ opacity: 0.3 }} />
        <p className="text-xs">Select tickers in ⚙ config</p>
      </div>
    )
  }

  const isEps   = EPS_METRICS.has(metric)
  const allYrs  = [...new Set(target.flatMap(d => d.years.map(y => y.fiscal_year)))].sort((a,b) => a-b)
  const data    = allYrs.map(yr => {
    const pt: Record<string, number | null | number> = { year: yr }
    target.forEach(({ ticker, years }) => {
      const y = years.find(yy => yy.fiscal_year === yr)
      pt[ticker] = y ? metricBn(y, metric) : null
    })
    return pt
  })

  const fmtV = (v: unknown) => isEps ? `${Number(v).toFixed(2)}` : `${Number(v).toFixed(1)}B KZT`
  const fmtY = (v: number)  => isEps ? v.toFixed(2) : `${v.toFixed(0)}B`

  if (chartStyle === 'bar') {
    return (
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
          <XAxis dataKey="year" tick={AX} axisLine={false} tickLine={false} />
          <YAxis tickFormatter={fmtY} tick={AX} axisLine={false} tickLine={false} width={44} />
          <ReferenceLine y={0} stroke="var(--color-border)" />
          <Tooltip contentStyle={TT} formatter={(v: unknown) => [fmtV(v)]} />
          <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 11, color: 'var(--color-muted)' }} />
          {target.map(({ ticker }, idx) => (
            <Bar key={ticker} dataKey={ticker} name={ticker}
              fill={tColor(ticker, idx)} fillOpacity={0.8} radius={[3,3,0,0]} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    )
  }

  return (
    <ResponsiveContainer width="100%" height={220}>
      <LineChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
        <XAxis dataKey="year" tick={AX} axisLine={false} tickLine={false} />
        <YAxis tickFormatter={fmtY} tick={AX} axisLine={false} tickLine={false} width={44} />
        <ReferenceLine y={0} stroke="var(--color-border)" />
        <Tooltip contentStyle={TT} formatter={(v: unknown) => [fmtV(v)]} />
        <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 11, color: 'var(--color-muted)' }} />
        {target.map(({ ticker }, idx) => (
          <Line key={ticker} type="monotone" dataKey={ticker} name={ticker}
            stroke={tColor(ticker, idx)} strokeWidth={2} dot={false} connectNulls />
        ))}
      </LineChart>
    </ResponsiveContainer>
  )
}

/* ─── page ────────────────────────────────────────────────── */

const CHART_TYPES = [
  { type: 'margin'   as ChartType, label: 'Margins',  Icon: TrendingUp         },
  { type: 'fcf'      as ChartType, label: 'Earnings', Icon: BarChart2          },
  { type: 'leverage' as ChartType, label: 'Leverage', Icon: Grid3x3            },
  { type: 'bubble'   as ChartType, label: 'Bubble',   Icon: CircleDot          },
  { type: 'cagr'     as ChartType, label: 'CAGR',     Icon: BarChartHorizontal },
  { type: 'custom'   as ChartType, label: 'Custom',   Icon: Activity           },
]

const TYPE_LABELS: Record<ChartType, string> = {
  margin:   'Margin Trends',
  fcf:      'Earnings Quality',
  leverage: 'Leverage Heatmap',
  bubble:   'Growth vs Profit',
  cagr:     'Revenue CAGR',
  custom:   'Custom',
}

export default function AnalyticsPage() {
  const [blocks,     setBlocks]     = useState<Block[]>(loadBlocks)
  const [openConfig, setOpenConfig] = useState<string | null>(null)
  const [dragIdx,    setDragIdx]    = useState<number | null>(null)
  const [dragOver,   setDragOver]   = useState<string | null>(null)

  /* data */
  const { data: tickers } = useQuery({
    queryKey: ['fin-tickers'],
    queryFn:  getFinTickers,
    staleTime: 3600_000,
  })
  const availableTickers = tickers ?? []

  const allStatements = useQueries({
    queries: availableTickers.map(t => ({
      queryKey: ['statements', t],
      queryFn:  () => getStatements(t),
      staleTime: 3600_000,
    })),
  })

  const allLoaded = allStatements.every(q => !q.isLoading)
  const allData   = useMemo(
    () => allStatements.filter(q => q.data).map(q => ({ ticker: q.data!.ticker, years: q.data!.years })),
    [allStatements],
  )

  /* mutations */
  function mutate(updated: Block[]) {
    setBlocks(updated)
    saveBlocks(updated)
  }

  function addBlock() {
    const b = mkBlock()
    mutate([...blocks, b])
    setOpenConfig(b.id)
  }

  function removeBlock(id: string) {
    mutate(blocks.filter(b => b.id !== id))
    if (openConfig === id) setOpenConfig(null)
  }

  function updateBlock(id: string, patch: Partial<Block>) {
    mutate(blocks.map(b => b.id === id ? { ...b, ...patch } : b))
  }

  function handleDrop(targetIdx: number) {
    if (dragIdx === null || dragIdx === targetIdx) return
    const next = [...blocks]
    const [moved] = next.splice(dragIdx, 1)
    next.splice(targetIdx, 0, moved)
    mutate(next)
    setDragIdx(null)
    setDragOver(null)
  }

  /* block title */
  function blockTitle(block: Block): string {
    const base = block.type === 'custom'
      ? (METRICS.find(m => m.key === block.metric)?.label ?? 'Custom')
      : TYPE_LABELS[block.type]

    if (block.tickers.length === 1) return `${base} · ${block.tickers[0]}`
    if (block.tickers.length > 1) {
      const shown = block.tickers.slice(0, 2).join(' vs ')
      const more  = block.tickers.length > 2 ? ` +${block.tickers.length - 2}` : ''
      return `${base} · ${shown}${more}`
    }
    if (['margin', 'fcf'].includes(block.type) && allData.length) {
      return `${base} · ${allData[0].ticker}`
    }
    return base
  }

  /* render chart for a block */
  function renderChart(block: Block) {
    const filtered = block.tickers.length
      ? allData.filter(d => block.tickers.includes(d.ticker))
      : allData
    if (!allLoaded && ['leverage', 'bubble', 'cagr'].includes(block.type)) return <Spinner />

    switch (block.type) {
      case 'margin':   return <MarginChart tickers={block.tickers} allData={allData} />
      case 'fcf':      return <FcfChart tickers={block.tickers} allData={allData} />
      case 'leverage': return <LeverageHeatmap allData={filtered} />
      case 'bubble':   return <BubbleChart allData={filtered} />
      case 'cagr':     return <CagrBar allData={filtered} />
      case 'custom':   return (
        <CustomMetricChart
          metric={block.metric}
          chartStyle={block.chartStyle}
          tickers={block.tickers}
          allData={allData}
        />
      )
    }
  }

  return (
    <div className="max-w-7xl mx-auto px-4 py-6">

      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-xl font-bold" style={{ color: 'var(--color-heading)' }}>Analytics</h1>
          <p className="text-sm mt-0.5" style={{ color: 'var(--color-muted)' }}>
            Build your own view — compare stocks, arrange charts
          </p>
        </div>
        {blocks.length > 0 && (
          <button
            onClick={addBlock}
            className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-semibold transition-all hover:brightness-110"
            style={{ background: 'var(--color-accent)', color: '#000' }}
          >
            <Plus size={15} />
            Add Chart
          </button>
        )}
      </div>

      {/* Empty state */}
      {blocks.length === 0 && (
        <div className="flex flex-col items-center justify-center py-28 gap-5">
          <button
            onClick={addBlock}
            className="w-28 h-28 rounded-3xl border-2 border-dashed flex flex-col items-center justify-center gap-2 transition-all hover:scale-105 hover:border-amber-400"
            style={{ borderColor: 'var(--color-border)' }}
          >
            <Plus size={40} style={{ color: 'var(--color-muted)' }} />
          </button>
          <div className="text-center">
            <p className="font-semibold text-base" style={{ color: 'var(--color-heading)' }}>
              Add your first chart
            </p>
            <p className="text-sm mt-1" style={{ color: 'var(--color-muted)' }}>
              Compare stocks, overlay metrics, arrange freely
            </p>
          </div>
        </div>
      )}

      {/* Dashboard grid */}
      {blocks.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {blocks.map((block, i) => (
            <div
              key={block.id}
              className={block.size === 'full' ? 'lg:col-span-2' : ''}
              draggable
              onDragStart={e => { e.dataTransfer.effectAllowed = 'move'; setDragIdx(i) }}
              onDragOver={e => { e.preventDefault(); setDragOver(block.id) }}
              onDrop={() => handleDrop(i)}
              onDragEnd={() => { setDragIdx(null); setDragOver(null) }}
            >
              {/* Block card */}
              <div
                className="rounded-xl border p-4 transition-all"
                style={{
                  background:   'var(--color-surface)',
                  borderColor:  dragOver === block.id && dragIdx !== i
                    ? 'var(--color-accent)'
                    : 'var(--color-border)',
                  opacity: dragIdx === i ? 0.45 : 1,
                }}
              >
                {/* Block header */}
                <div className="flex items-center gap-2 mb-3">
                  <GripVertical size={14} className="cursor-grab shrink-0"
                    style={{ color: 'var(--color-muted)' }} />
                  <p className="text-xs font-semibold uppercase tracking-wider flex-1 truncate"
                    style={{ color: 'var(--color-heading)' }}>
                    {blockTitle(block)}
                  </p>
                  <button
                    onClick={() => setOpenConfig(openConfig === block.id ? null : block.id)}
                    title="Configure"
                    style={{ color: openConfig === block.id ? 'var(--color-accent)' : 'var(--color-muted)' }}
                  >
                    <Settings size={13} />
                  </button>
                  <button onClick={() => removeBlock(block.id)} title="Remove"
                    style={{ color: 'var(--color-muted)' }}>
                    <X size={13} />
                  </button>
                </div>

                {/* Chart content */}
                {renderChart(block)}

                {/* Config panel */}
                {openConfig === block.id && (
                  <div className="border-t mt-4 pt-4 space-y-4"
                    style={{ borderColor: 'var(--color-border)' }}>

                    {/* Chart type */}
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-wider mb-2"
                        style={{ color: 'var(--color-muted)', fontSize: 10 }}>Chart Type</p>
                      <div className="flex flex-wrap gap-1.5">
                        {CHART_TYPES.map(({ type, label, Icon }) => (
                          <button key={type}
                            onClick={() => updateBlock(block.id, { type })}
                            className="flex items-center gap-1.5 px-2.5 py-1.5 rounded transition-all"
                            style={{
                              background:  block.type === type ? 'var(--color-accent)' : 'var(--color-border)',
                              color:       block.type === type ? '#000' : 'var(--color-text)',
                              fontSize:    11,
                            }}
                          >
                            <Icon size={11} />
                            {label}
                          </button>
                        ))}
                      </div>
                    </div>

                    {/* Metric + style (custom only) */}
                    {block.type === 'custom' && (
                      <div>
                        <p className="text-xs font-semibold uppercase tracking-wider mb-2"
                          style={{ color: 'var(--color-muted)', fontSize: 10 }}>Metric</p>
                        <div className="flex flex-wrap gap-1 mb-2.5">
                          {METRICS.map(m => (
                            <button key={m.key}
                              onClick={() => updateBlock(block.id, { metric: m.key })}
                              className="px-2 py-0.5 rounded transition-all"
                              style={{
                                background: block.metric === m.key ? 'var(--color-accent)' : 'var(--color-border)',
                                color:      block.metric === m.key ? '#000' : 'var(--color-text)',
                                fontSize:   10,
                              }}
                            >
                              {m.label}
                            </button>
                          ))}
                        </div>
                        <div className="flex gap-1.5">
                          {(['line', 'bar'] as const).map(s => (
                            <button key={s}
                              onClick={() => updateBlock(block.id, { chartStyle: s })}
                              className="px-3 py-1 rounded capitalize text-xs transition-all"
                              style={{
                                background: block.chartStyle === s ? 'var(--color-accent)' : 'var(--color-border)',
                                color:      block.chartStyle === s ? '#000' : 'var(--color-text)',
                              }}
                            >
                              {s}
                            </button>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Ticker selector */}
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-wider mb-2"
                        style={{ color: 'var(--color-muted)', fontSize: 10 }}>
                        {['leverage','bubble','cagr'].includes(block.type)
                          ? 'Filter Tickers — empty = all'
                          : 'Compare Tickers'}
                      </p>
                      <div className="flex flex-wrap gap-1">
                        {availableTickers.map(t => {
                          const on = block.tickers.includes(t)
                          return (
                            <button key={t}
                              onClick={() => {
                                const next = on
                                  ? block.tickers.filter(x => x !== t)
                                  : [...block.tickers, t]
                                updateBlock(block.id, { tickers: next })
                              }}
                              className="px-2 py-0.5 rounded font-mono transition-all"
                              style={{
                                background: on ? 'var(--color-accent)' : 'var(--color-border)',
                                color:      on ? '#000' : 'var(--color-muted)',
                                fontSize:   10,
                              }}
                            >
                              {t}
                            </button>
                          )
                        })}
                      </div>
                    </div>

                    {/* Size */}
                    <div className="flex items-center gap-3">
                      <p className="text-xs font-semibold uppercase tracking-wider"
                        style={{ color: 'var(--color-muted)', fontSize: 10 }}>Size</p>
                      {(['half', 'full'] as const).map(s => (
                        <button key={s}
                          onClick={() => updateBlock(block.id, { size: s })}
                          className="px-3 py-1 rounded capitalize text-xs transition-all"
                          style={{
                            background: block.size === s ? 'var(--color-accent)' : 'var(--color-border)',
                            color:      block.size === s ? '#000' : 'var(--color-text)',
                          }}
                        >
                          {s}
                        </button>
                      ))}
                    </div>

                  </div>
                )}
              </div>
            </div>
          ))}

          {/* Inline add button at the end of grid */}
          <button
            onClick={addBlock}
            className="rounded-xl border-2 border-dashed flex items-center justify-center gap-2 py-8 transition-all hover:border-amber-400 hover:text-amber-400"
            style={{ borderColor: 'var(--color-border)', color: 'var(--color-muted)' }}
          >
            <Plus size={18} />
            <span className="text-sm font-medium">Add Chart</span>
          </button>
        </div>
      )}

    </div>
  )
}
