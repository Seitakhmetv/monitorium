import { useState, useEffect, useMemo, useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { ResponsiveSankey } from '@nivo/sankey'
import { getStatements, getFlows, getFlowsYears, getFinTickers } from '../../api/fundamentals'
import type { StatementYear, FlowData } from '../../api/fundamentals'
import { Spinner } from '../../components/ui'

/* ─── formatting ──────────────────────────────────────── */

function fmtVal(val: number | null | undefined, units: string): string {
  if (val == null) return '—'
  const u = units.toLowerCase()
  const divisor = u.includes('thousand') || u === 'тыс' || u === 'тысяч'
    ? 1_000_000
    : u.includes('million') || u === 'млн'
    ? 1_000
    : u.includes('billion') || u === 'млрд'
    ? 1
    : 1
  const b = Math.abs(val) / divisor
  const sign = val < 0 ? '-' : ''
  if (b >= 1_000) return `${sign}${(b / 1_000).toFixed(1)}T`
  if (b >= 1)     return `${sign}${b.toFixed(1)}B`
  if (b >= 0.001) return `${sign}${(b * 1_000).toFixed(0)}M`
  return `${sign}${(b * 1_000_000).toFixed(0)}K`
}

/* ─── node classification ─────────────────────────────── */

type NodeKind = 'income' | 'cost' | 'profit' | 'subtotal' | 'other'

function classifyNode(id: string): NodeKind {
  const l = id.toLowerCase()
  if (/прочие расходы|other costs?|other exp/.test(l)) return 'other'
  if (/расход|затрат|себестоим|убыток|страхован|амортизац|износ|аренд|персонал|топливо|ремонт|аэропорт|инженерно|обслужив|реализаци|информацион|консультац|имуществ|обесценени|курсов|cost|expense|deprec|amort/.test(l)) return 'cost'
  if (/прибыль|profit|ebitda|маржа/.test(l)) return 'profit'
  if (/выручка|доход|поступлен|revenue|income|proceeds/.test(l)) return 'income'
  return 'subtotal'
}

const KIND_COLOR: Record<NodeKind, string> = {
  income:   '#4ade80',
  cost:     '#f87171',
  profit:   '#fbbf24',
  subtotal: '#818cf8',
  other:    '#94a3b8',
}

function nodeColor(id: string): string {
  return KIND_COLOR[classifyNode(id)]
}

/* ─── flow preprocessing ──────────────────────────────── */

interface ProcessedFlows {
  nodes: { id: string; color: string }[]
  links: { source: string; target: string; value: number }[]
  totalRevenue: number
  netIncome: number | null
  totalCosts: number
}

function preprocessFlows(raw: FlowData): ProcessedFlows {
  const { nodes: rawNodes, links: rawLinks } = raw

  const hasOutgoing = new Set(rawLinks.map(l => l.source))
  const hasIncoming = new Set(rawLinks.map(l => l.target))

  const isSink   = (id: string) => !hasOutgoing.has(id)
  const isSource = (id: string) => !hasIncoming.has(id)

  // Total revenue = sum of all pure-source node outflows
  const totalRevenue = rawLinks
    .filter(l => isSource(l.source))
    .reduce((s, l) => s + l.value, 0)

  // Group outgoing links per emitter
  const bySource: Record<string, typeof rawLinks> = {}
  rawLinks.forEach(l => {
    if (!bySource[l.source]) bySource[l.source] = []
    bySource[l.source].push(l)
  })

  const resultLinks: { source: string; target: string; value: number }[] = []

  Object.entries(bySource).forEach(([source, outLinks]) => {
    const totalOut = outLinks.reduce((s, l) => s + l.value, 0)
    // 6% threshold of this node's total outflow
    const threshold = totalOut * 0.06

    // Always keep links going to non-sink nodes (profit chain)
    const toNonSink = outLinks.filter(l => !isSink(l.target))
    resultLinks.push(...toNonSink)

    // For sink-bound links: keep large, group small
    const toSink      = outLinks.filter(l => isSink(l.target))
    const largeSink   = toSink.filter(l => l.value >= threshold)
    const smallSink   = toSink.filter(l => l.value < threshold)

    resultLinks.push(...largeSink)

    if (smallSink.length >= 2) {
      const groupValue = smallSink.reduce((s, l) => s + l.value, 0)
      resultLinks.push({ source, target: 'Прочие расходы', value: groupValue })
    } else {
      resultLinks.push(...smallSink)
    }
  })

  // Net income: look for profit-related sink node
  const profitSinkId = rawNodes.find(n =>
    /прибыль за год|чистая прибыл|profit for the year|net income|net profit/i.test(n.id)
  )?.id
  const netIncome = profitSinkId
    ? resultLinks.filter(l => l.target === profitSinkId).reduce((s, l) => s + l.value, 0)
    : null

  // Rebuild node list
  const usedIds = new Set<string>()
  resultLinks.forEach(l => { usedIds.add(l.source); usedIds.add(l.target) })

  const sinkIds = Array.from(usedIds).filter(id => !resultLinks.some(l => l.source === id))
  const totalCosts = sinkIds
    .filter(id => classifyNode(id) === 'cost' || classifyNode(id) === 'other')
    .reduce((s, id) =>
      s + resultLinks.filter(l => l.target === id).reduce((ss, l) => ss + l.value, 0)
    , 0)

  const nodes = Array.from(usedIds).map(id => ({ id, color: nodeColor(id) }))

  return { nodes, links: resultLinks, totalRevenue, netIncome, totalCosts }
}

/* ─── summary cards ───────────────────────────────────── */

function SummaryCards({ processed, currency, units }: {
  processed: ProcessedFlows
  currency: string
  units: string
}) {
  const { totalRevenue, netIncome, totalCosts } = processed
  const margin = netIncome != null && totalRevenue ? (netIncome / totalRevenue) * 100 : null
  const netColor = netIncome == null ? 'var(--color-muted)' : netIncome >= 0 ? '#4ade80' : '#f87171'

  return (
    <div className="grid grid-cols-3 gap-3 mb-5">
      {[
        { label: 'Total Revenue',  value: totalRevenue, color: '#4ade80', sub: null },
        { label: 'Total Costs',    value: totalCosts,   color: '#f87171', sub: totalRevenue ? `${((totalCosts / totalRevenue) * 100).toFixed(0)}% of revenue` : null },
        { label: 'Net Income',     value: netIncome,    color: netColor,  sub: margin != null ? `${margin.toFixed(1)}% net margin` : null },
      ].map(({ label, value, color, sub }) => (
        <div key={label} className="rounded-lg px-4 py-3" style={{ background: 'var(--color-bg)', border: '1px solid var(--color-border)' }}>
          <div className="text-xs mb-1 uppercase tracking-wide" style={{ color: 'var(--color-muted)' }}>{label}</div>
          <div className="text-lg font-mono font-bold" style={{ color }}>
            {value != null ? `${fmtVal(value, units)} ${currency}` : '—'}
          </div>
          {sub && <div className="text-xs mt-0.5" style={{ color: 'var(--color-muted)' }}>{sub}</div>}
        </div>
      ))}
    </div>
  )
}

/* ─── sankey panel ────────────────────────────────────── */

function SankeyPanel({ ticker, year }: { ticker: string; year: number }) {
  const { t } = useTranslation()

  const { data, isLoading, isError } = useQuery({
    queryKey: ['flows', ticker, year],
    queryFn: () => getFlows(ticker, year),
    staleTime: 3600_000,
    retry: false,
  })

  const processed = useMemo(() => (data ? preprocessFlows(data) : null), [data])

  if (isLoading) return <div className="flex justify-center items-center h-96"><Spinner /></div>
  if (isError || !data || !processed) return (
    <div className="flex justify-center items-center h-96 text-sm" style={{ color: 'var(--color-muted)' }}>
      {t('financials.noFlows')}
    </div>
  )

  const sinkCount = processed.nodes.filter(n => !processed.links.some(l => l.source === n.id)).length
  const chartHeight = Math.max(520, sinkCount * 56 + 80)
  const isSafari = /^((?!chrome|android).)*safari/i.test(navigator.userAgent)

  return (
    <div>
      <SummaryCards processed={processed} currency={data.currency} units={data.units} />

      {/* Color legend */}
      <div className="flex gap-4 mb-4 flex-wrap">
        {(Object.entries(KIND_COLOR) as [NodeKind, string][]).map(([kind, color]) => (
          <div key={kind} className="flex items-center gap-1.5 text-xs" style={{ color: 'var(--color-muted)' }}>
            <div className="w-2.5 h-2.5 rounded-sm" style={{ background: color }} />
            {kind}
          </div>
        ))}
      </div>

      {/* isolation+translateZ forces a new compositing layer — fixes Safari SVG gradient in overflow */}
      <div style={{ overflowX: 'auto', isolation: 'isolate', transform: 'translateZ(0)', WebkitOverflowScrolling: 'touch' } as React.CSSProperties}>
        <div style={{ height: chartHeight, minWidth: 680 }}>
        <ResponsiveSankey
          data={processed}
          margin={{ top: 8, right: 160, bottom: 8, left: 160 }}
          align="justify"
          colors={node => (node as unknown as { color: string }).color}
          nodeOpacity={0.92}
          nodeThickness={22}
          nodeInnerPadding={4}
          nodeSpacing={14}
          nodeBorderWidth={0}
          linkOpacity={isSafari ? 0.65 : 0.55}
          linkHoverOpacity={0.85}
          linkContract={1}
          linkBlendMode="normal"
          enableLinkGradient={!isSafari}
          labelPosition="outside"
          labelOrientation="horizontal"
          labelPadding={14}
          label={node => {
            const id = (node as unknown as { id: string }).id
            return id.length > 24 ? id.slice(0, 22) + '…' : id
          }}
          labelTextColor={node => (node as unknown as { color: string }).color}
          theme={{
            text: { fill: 'var(--color-text)', fontSize: 10.5 },
            tooltip: {
              container: {
                background: 'var(--color-surface)',
                color: 'var(--color-heading)',
                border: '1px solid var(--color-border)',
                borderRadius: 6,
                padding: 0,
                boxShadow: '0 4px 16px rgba(0,0,0,0.4)',
              },
            },
          }}
          nodeTooltip={({ node }: { node: { id: string; value: number } }) => {
            const id = node.id
            const val = node.value
            const pct = processed.totalRevenue ? (val / processed.totalRevenue * 100).toFixed(1) : null
            return (
              <div className="px-3 py-2.5" style={{ minWidth: 200 }}>
                <div className="text-xs mb-1.5 font-medium" style={{ color: 'var(--color-heading)' }}>{id}</div>
                <div className="text-base font-mono font-bold" style={{ color: nodeColor(id) }}>
                  {fmtVal(val, data.units)} {data.currency}
                </div>
                {pct && (
                  <div className="text-xs mt-0.5" style={{ color: 'var(--color-muted)' }}>
                    {pct}% of revenue
                  </div>
                )}
              </div>
            )
          }}
          linkTooltip={({ link }) => {
            const src = (link as unknown as { source: { id: string }; target: { id: string }; value: number })
            const pct = processed.totalRevenue ? (src.value / processed.totalRevenue * 100).toFixed(1) : null
            return (
              <div className="px-3 py-2.5" style={{ minWidth: 220, background: 'var(--color-surface)', border: '1px solid var(--color-border)', borderRadius: 6, boxShadow: '0 4px 16px rgba(0,0,0,0.4)' }}>
                <div className="text-xs mb-1.5" style={{ color: 'var(--color-muted)' }}>
                  {src.source.id} → {src.target.id}
                </div>
                <div className="text-base font-mono font-bold" style={{ color: nodeColor(src.source.id) }}>
                  {fmtVal(src.value, data.units)} {data.currency}
                </div>
                {pct && (
                  <div className="text-xs mt-0.5" style={{ color: 'var(--color-muted)' }}>
                    {pct}% of revenue
                  </div>
                )}
              </div>
            )
          }}
        />
        </div>
      </div>
    </div>
  )
}

/* ─── historical table ────────────────────────────────── */

const ALL_COLS = [
  { key: 'revenue',           label: 'Revenue'      },
  { key: 'net_income',        label: 'Net Income'   },
  { key: 'ebitda',            label: 'EBITDA'       },
  { key: 'total_assets',      label: 'Assets'       },
  { key: 'total_liabilities', label: 'Liabilities'  },
  { key: 'total_equity',      label: 'Equity'       },
  { key: 'total_debt',        label: 'Debt'         },
  { key: 'capex',             label: 'CapEx'        },
  { key: 'operating_cash_flow', label: 'Op. CF'     },
] as const

type ColKey = typeof ALL_COLS[number]['key']

function StatementsTable({ years }: { years: StatementYear[] }) {
  const { t } = useTranslation()
  const [hiddenCols,  setHiddenCols]  = useState<Set<ColKey>>(new Set())
  const [hiddenYears, setHiddenYears] = useState<Set<number>>(new Set())

  const toggleCol  = useCallback((k: ColKey)  => setHiddenCols(p  => { const n = new Set(p); n.has(k) ? n.delete(k) : n.add(k); return n }), [])
  const toggleYear = useCallback((y: number)  => setHiddenYears(p => { const n = new Set(p); n.has(y) ? n.delete(y) : n.add(y); return n }), [])

  if (!years.length) return null
  const units    = years[0].units    ?? 'millions'
  const currency = years[0].currency ?? 'KZT'

  const visCols  = ALL_COLS.filter(c => !hiddenCols.has(c.key))

  const getVal = (y: StatementYear, key: ColKey): number | null => {
    if (key === 'revenue') return y.net_interest_income != null ? y.net_interest_income : y.revenue
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (y as any)[key] ?? null
  }

  return (
    <div>
      {/* Column toggles */}
      <div className="flex flex-wrap gap-1.5 mb-4">
        {ALL_COLS.map(c => {
          const hidden = hiddenCols.has(c.key)
          return (
            <button key={c.key} onClick={() => toggleCol(c.key)}
              className="text-xs px-2.5 py-1 rounded-full border transition-all"
              style={{
                border: `1px solid ${hidden ? 'var(--color-border)' : 'var(--color-accent)'}`,
                background: hidden ? 'transparent' : 'var(--color-accent)20',
                color: hidden ? 'var(--color-muted)' : 'var(--color-accent)',
              }}>
              {c.label}
            </button>
          )
        })}
        {hiddenYears.size > 0 && (
          <button onClick={() => setHiddenYears(new Set())}
            className="text-xs px-2.5 py-1 rounded-full border ml-2"
            style={{ border: '1px solid var(--color-border)', color: 'var(--color-muted)' }}>
            Show all years
          </button>
        )}
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm" style={{ color: 'var(--color-text)' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--color-border)' }}>
              <th className="text-left py-2 pr-4 font-medium" style={{ color: 'var(--color-muted)' }}>
                {t('financials.year')}
              </th>
              {visCols.map(c => (
                <th key={c.key}
                  onClick={() => toggleCol(c.key)}
                  className="text-right py-2 px-3 font-medium whitespace-nowrap cursor-pointer select-none hover:opacity-60 transition-opacity"
                  style={{ color: 'var(--color-muted)' }}
                  title="Click to hide">
                  {c.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {years.map(y => {
              const dimmed = hiddenYears.has(y.fiscal_year)
              return (
                <tr key={y.fiscal_year}
                  onClick={() => toggleYear(y.fiscal_year)}
                  className="border-b transition-all cursor-pointer hover:bg-white/5"
                  style={{ borderColor: 'var(--color-border)', opacity: dimmed ? 0.25 : 1 }}
                  title="Click to hide year">
                  <td className="py-2 pr-4 font-medium" style={{ color: 'var(--color-heading)' }}>
                    FY{y.fiscal_year}
                  </td>
                  {visCols.map(c => {
                    const v = getVal(y, c.key)
                    return (
                      <td key={c.key} className="text-right py-2 px-3 font-mono">
                        <span style={{ color: v != null && v < 0 ? '#f87171' : 'var(--color-heading)' }}>
                          {fmtVal(v, units)}
                        </span>
                      </td>
                    )
                  })}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      <p className="mt-2 text-xs" style={{ color: 'var(--color-muted)' }}>
        {currency} · {units} · {t('financials.source')} · click column header or row to hide
      </p>
    </div>
  )
}

/* ─── page ────────────────────────────────────────────── */

const FALLBACK_TICKERS = [
  'HSBK','KSPI','KCEL','KMGZ','KZTK','KEGC','KZTO',
  'AIRA','CCBN','ASBN','AKZM','RAHT','BAST','KMGD','BSUL','KZAP',
]

export default function FinancialsPage() {
  const { t } = useTranslation()
  const [searchParams, setSearchParams] = useSearchParams()
  const [ticker, setTicker] = useState(searchParams.get('ticker') ?? 'AIRA')
  const [year, setYear] = useState<number | null>(null)

  const { data: tickers } = useQuery({
    queryKey: ['fin-tickers'],
    queryFn: getFinTickers,
    staleTime: 3600_000,
  })

  const { data: statements, isLoading: stmtLoading } = useQuery({
    queryKey: ['statements', ticker],
    queryFn: () => getStatements(ticker),
    staleTime: 3600_000,
    retry: false,
  })

  const { data: flowYears } = useQuery({
    queryKey: ['flow-years', ticker],
    queryFn: () => getFlowsYears(ticker),
    staleTime: 3600_000,
    retry: false,
  })

  useEffect(() => {
    if (flowYears?.length) setYear(flowYears[0])
  }, [flowYears])

  const handleTickerChange = (t: string) => {
    setTicker(t)
    setYear(null)
    setSearchParams({ ticker: t })
  }

  const availableTickers = tickers ?? FALLBACK_TICKERS
  const years = statements?.years ?? []
  const selectableYears = flowYears ?? []

  return (
    <div className="max-w-7xl mx-auto px-4 py-6 space-y-5">
      {/* Header */}
      <div className="flex flex-wrap items-start gap-4">
        <div>
          <h1 className="text-xl font-bold" style={{ color: 'var(--color-heading)' }}>
            {t('financials.title')}
          </h1>
          <p className="text-sm mt-0.5" style={{ color: 'var(--color-muted)' }}>
            {t('financials.subtitle')}
          </p>
        </div>
        <div className="flex gap-3 ml-auto">
          <select
            value={ticker}
            onChange={e => handleTickerChange(e.target.value)}
            className="px-3 py-1.5 rounded text-sm outline-none cursor-pointer"
            style={{ background: 'var(--color-surface)', color: 'var(--color-heading)', border: '1px solid var(--color-border)' }}
          >
            {availableTickers.map(t => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
          <select
            value={year ?? ''}
            onChange={e => setYear(Number(e.target.value))}
            disabled={!selectableYears.length}
            className="px-3 py-1.5 rounded text-sm outline-none cursor-pointer"
            style={{ background: 'var(--color-surface)', color: 'var(--color-heading)', border: '1px solid var(--color-border)' }}
          >
            {selectableYears.map(y => <option key={y} value={y}>FY{y}</option>)}
            {!selectableYears.length && <option value="">—</option>}
          </select>
        </div>
      </div>

      {/* Sankey */}
      <div className="rounded-lg p-5" style={{ background: 'var(--color-surface)', border: '1px solid var(--color-border)' }}>
        <h2 className="text-sm font-semibold mb-1" style={{ color: 'var(--color-heading)' }}>
          {t('financials.sankey')}{year ? ` · FY${year}` : ''}
        </h2>
        <p className="text-xs mb-5" style={{ color: 'var(--color-muted)' }}>
          Hover nodes and links to explore values. Small items grouped into Прочие расходы.
        </p>
        {year ? (
          <SankeyPanel ticker={ticker} year={year} />
        ) : (
          <div className="flex justify-center items-center h-80 text-sm" style={{ color: 'var(--color-muted)' }}>
            {t('financials.selectYear')}
          </div>
        )}
      </div>

      {/* Historical KPIs */}
      <div className="rounded-lg p-5" style={{ background: 'var(--color-surface)', border: '1px solid var(--color-border)' }}>
        <h2 className="text-sm font-semibold mb-4" style={{ color: 'var(--color-heading)' }}>
          {t('financials.historical')}
        </h2>
        {stmtLoading ? (
          <div className="flex justify-center py-10"><Spinner /></div>
        ) : years.length ? (
          <StatementsTable years={years} />
        ) : (
          <p className="text-sm py-6 text-center" style={{ color: 'var(--color-muted)' }}>
            {t('financials.noData')}
          </p>
        )}
      </div>
    </div>
  )
}
