import { useParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { useState, useRef, useCallback } from 'react'
import { motion } from 'framer-motion'
import { Star, ExternalLink, Building2 } from 'lucide-react'
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceArea,
  BarChart, Bar,
} from 'recharts'
import { getPrices, getLatestPrice } from '../../api/prices'
import { getNews } from '../../api/news'
import { getFundamentals } from '../../api/fundamentals'
import { useWatchlist } from '../../store/watchlist'
import { Spinner, ChangeBadge, Card, ImpactDot, SourceBadge, SectionHeader } from '../../components/ui'
import { getTicker } from '../../config'

type Timeframe = '1M' | '3M' | '1Y' | 'ALL'

const YAXIS_W = 50

function daysAgo(days: number) {
  const d = new Date()
  d.setDate(d.getDate() - days)
  return d.toISOString().slice(0, 10)
}

const TF_DAYS: Record<Timeframe, number | null> = { '1M': 30, '3M': 90, '1Y': 365, 'ALL': null }

function StatBox({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-xs uppercase tracking-wider" style={{ color: 'var(--color-muted)' }}>{label}</span>
      <span className="text-sm font-semibold tabular" style={{ color: 'var(--color-heading)' }}>{value}</span>
    </div>
  )
}

function MetricPill({ label, value, sub }: { label: string; value: React.ReactNode; sub?: string }) {
  return (
    <div className="flex flex-col gap-0.5 py-3 px-4 rounded-lg" style={{ background: 'var(--color-surface)' }}>
      <span className="text-xs uppercase tracking-wider" style={{ color: 'var(--color-muted)' }}>{label}</span>
      <span className="text-base font-bold tabular" style={{ color: 'var(--color-heading)' }}>{value}</span>
      {sub && <span className="text-xs" style={{ color: 'var(--color-muted)' }}>{sub}</span>}
    </div>
  )
}

function fmt(n: number | null | undefined, decimals = 1): string {
  if (n == null || !isFinite(n)) return '—'
  return n.toFixed(decimals)
}

function fmtBn(n: number): string {
  if (n >= 1000) return `${(n / 1000).toFixed(1)}T`
  return `${n.toFixed(0)}B`
}

// Sector-specific KPI label mapping
const KPI_LABELS: Record<string, string> = {
  // Oil & Gas (KMGZ)
  production_bpd:              'Production (bpd)',
  reserves_bn_bbl:             'Reserves (bn bbl)',
  refining_capacity_bpd:       'Refining cap. (bpd)',
  // Uranium (KZAP)
  production_tU_2023:          'U prod. 2023 (tU)',
  proven_reserves_tU:          'U reserves (tU)',
  cost_per_lb_usd:             'Cost/lb (USD)',
  // Banking (HSBK)
  nim_pct:                     'NIM',
  npl_ratio_pct:               'NPL ratio',
  cost_to_income_pct:          'Cost/income',
  roe_pct:                     'ROE',
  tier1_capital_ratio_pct:     'Tier 1 capital',
  // Super-app (KSPI)
  mau_mn:                      'MAU (mn)',
  total_payment_volume_bn_usd: 'Payments vol. (bn USD)',
  marketplace_gmv_bn_kzt:      'Marketplace GMV (KZT bn)',
  net_interest_margin_pct:     'NIM',
  // Telecom (KZTK)
  broadband_subscribers_mn:    'Broadband subs (mn)',
  mobile_subscribers_mn:       'Mobile subs (mn)',
  arpu_fixed_kzt:              'Fixed ARPU (KZT)',
  // Grid (KEGC)
  transmission_lines_km:       'Grid length (km)',
  electricity_transit_bn_kwh:  'Transit (bn kWh)',
  regulated_tariff_kzt_kwh:    'Tariff (KZT/kWh)',
  // Mobile (KCEL)
  subscribers_mn:              'Subs (mn)',
  data_revenue_share_pct:      'Data rev. share',
  arpu_kzt:                    'ARPU (KZT)',
  churn_rate_pct:              'Churn rate',
  // Pipeline (KZTO)
  pipeline_length_km:          'Pipeline (km)',
  throughput_mn_tonnes:        'Throughput (mn t)',
  tariff_kzt_per_t_per_100km:  'Tariff (KZT/t/100km)',
  // Aviation (AIRA)
  passengers_mn:               'Passengers (mn)',
  destinations:                'Destinations',
  fleet_size:                  'Fleet size',
  load_factor_pct:             'Load factor',
}

export default function TickerPage() {
  const { ticker } = useParams<{ ticker: string }>()
  const { t } = useTranslation()
  const watchlist = useWatchlist()
  const [tf, setTf] = useState<Timeframe>('3M')
  const sym = ticker?.toUpperCase() ?? ''
  const meta = getTicker(sym)
  const isCommodity = meta?.sector === 'Commodity' || meta?.sector === 'FX'

  const fromDate = TF_DAYS[tf] ? daysAgo(TF_DAYS[tf]!) : undefined

  const { data: latest } = useQuery({
    queryKey: ['latest', sym],
    queryFn: () => getLatestPrice(sym),
    staleTime: 60_000,
    enabled: !!sym,
    retry: false,
  })

  const { data: prices, isLoading: pricesLoading } = useQuery({
    queryKey: ['prices', sym, tf],
    queryFn: () => getPrices({ ticker: sym, from: fromDate }),
    staleTime: 60_000,
    enabled: !!sym,
  })

  const { data: news, isLoading: newsLoading } = useQuery({
    queryKey: ['news', sym],
    queryFn: () => getNews({ ticker: sym, limit: 20 }),
    staleTime: 300_000,
    enabled: !!sym,
  })

  const { data: fundamentals, isLoading: fundamentalsLoading, error: fundamentalsError } = useQuery({
    queryKey: ['fundamentals', sym],
    queryFn: () => getFundamentals(sym),
    staleTime: Infinity,
    enabled: !!sym && !isCommodity,
  })

  const chartData = prices ? [...prices].reverse().map(p => ({
    date: p.date,
    close: p.close,
  })) : []

  // ── Brush selection ────────────────────────────────────────────────────────
  const [brushDates, setBrushDates] = useState<{ from: string; to: string } | null>(null)
  const [liveDates,  setLiveDates]  = useState<{ from: string; to: string } | null>(null)

  const chartDataRef = useRef(chartData)
  chartDataRef.current = chartData

  const xToDate = useCallback((clientX: number, rect: DOMRect): string | null => {
    const data = chartDataRef.current
    if (!data.length) return null
    const relX = Math.max(0, clientX - rect.left - YAXIS_W)
    const pct  = Math.min(1, relX / Math.max(1, rect.width - YAXIS_W))
    return data[Math.round(pct * (data.length - 1))].date
  }, [])

  const dragRef = useRef<{ startDate: string; startX: number; startY: number } | null>(null)

  const overlayRef = useCallback((node: HTMLDivElement | null) => {
    if (!node) return

    const onMouseDown = (e: MouseEvent) => {
      const date = xToDate(e.clientX, node.getBoundingClientRect())
      if (!date) return
      dragRef.current = { startDate: date, startX: e.clientX, startY: e.clientY }
      setLiveDates(null)
    }
    const onMouseMove = (e: MouseEvent) => {
      if (!dragRef.current) return
      const date = xToDate(e.clientX, node.getBoundingClientRect())
      if (!date) return
      const [a, b] = [dragRef.current.startDate, date].sort()
      setLiveDates({ from: a, to: b })
    }
    const onMouseUp = (e: MouseEvent) => {
      if (!dragRef.current) return
      const dx = Math.abs(e.clientX - dragRef.current.startX)
      const dy = Math.abs(e.clientY - dragRef.current.startY)
      if (dx < 6 && dy < 6) {
        setBrushDates(null)
      } else {
        const date = xToDate(e.clientX, node.getBoundingClientRect())
        if (date) {
          const [a, b] = [dragRef.current.startDate, date].sort()
          setBrushDates({ from: a, to: b })
        }
      }
      dragRef.current = null
      setLiveDates(null)
    }

    const onTouchStart = (e: TouchEvent) => {
      e.preventDefault()
      const t0 = e.touches[0]
      const date = xToDate(t0.clientX, node.getBoundingClientRect())
      if (!date) return
      dragRef.current = { startDate: date, startX: t0.clientX, startY: t0.clientY }
      setLiveDates(null)
    }
    const onTouchMove = (e: TouchEvent) => {
      e.preventDefault()
      if (!dragRef.current) return
      const t0 = e.touches[0]
      const date = xToDate(t0.clientX, node.getBoundingClientRect())
      if (!date) return
      const [a, b] = [dragRef.current.startDate, date].sort()
      setLiveDates({ from: a, to: b })
    }
    const onTouchEnd = (e: TouchEvent) => {
      if (!dragRef.current) return
      const t0 = e.changedTouches[0]
      const dx = Math.abs(t0.clientX - dragRef.current.startX)
      const dy = Math.abs(t0.clientY - dragRef.current.startY)
      if (dx < 6 && dy < 6) {
        setBrushDates(null)
      } else {
        const date = xToDate(t0.clientX, node.getBoundingClientRect())
        if (date) {
          const [a, b] = [dragRef.current.startDate, date].sort()
          setBrushDates({ from: a, to: b })
        }
      }
      dragRef.current = null
      setLiveDates(null)
    }

    node.addEventListener('mousedown',  onMouseDown)
    node.addEventListener('mousemove',  onMouseMove)
    node.addEventListener('mouseup',    onMouseUp)
    node.addEventListener('mouseleave', onMouseUp)
    node.addEventListener('touchstart', onTouchStart, { passive: false })
    node.addEventListener('touchmove',  onTouchMove,  { passive: false })
    node.addEventListener('touchend',   onTouchEnd)
  }, [xToDate])

  // ── Derived display values ─────────────────────────────────────────────────
  const activeRange = liveDates ?? brushDates

  const fromPt = activeRange ? chartData.find(d => d.date === activeRange.from) : null
  const toPt   = activeRange ? chartData.find(d => d.date === activeRange.to)   : null

  const rangeReturn = fromPt && toPt
    ? ((toPt.close - fromPt.close) / fromPt.close) * 100
    : latest && latest.open
      ? ((latest.close - latest.open) / latest.open) * 100
      : null

  const minClose = chartData.length ? Math.min(...chartData.map(d => d.close)) : 0
  const maxClose = chartData.length ? Math.max(...chartData.map(d => d.close)) : 0
  const domain   = [minClose * 0.995, maxClose * 1.005]
  const isUp     = (rangeReturn ?? 0) >= 0

  // ── Fundamentals ratios ───────────────────────────────────────────────────
  const lastYear = fundamentals?.annual?.at(-1)
  const price    = latest?.close ?? 0
  const shares   = fundamentals?.shares_outstanding ?? 0
  const mktCapBn = price && shares ? (price * shares) / 1e9 : null

  const pe   = lastYear?.eps     && price ? price / lastYear.eps     : null
  const pb   = lastYear?.book_value_ps && price ? price / lastYear.book_value_ps : null
  const divY = lastYear?.dps && price ? (lastYear.dps / price) * 100 : null

  // EV/EBITDA: EV = mktCap + debt - cash  (all in same KZT bn)
  const mktCapBnKZT = mktCapBn  // already in bn if price is KZT
  const evEbitda =
    mktCapBnKZT && lastYear?.ebitda_bn && lastYear.ebitda_bn > 0
      ? (mktCapBnKZT + lastYear.total_debt_bn - lastYear.cash_bn) / lastYear.ebitda_bn
      : null

  // 52-week high/low from price history (ALL mode not loaded — use chartData)
  const allPrices = chartData.map(d => d.close)
  const w52High = allPrices.length ? Math.max(...allPrices) : null
  const w52Low  = allPrices.length ? Math.min(...allPrices) : null

  // Bar chart data
  const finChartData = fundamentals?.annual?.map(a => ({
    year:       String(a.year),
    revenue:    a.revenue_bn,
    net_income: a.net_income_bn,
    ebitda:     a.ebitda_bn,
  })) ?? []

  return (
    <div className="p-4 sm:p-6 max-w-5xl mx-auto space-y-4 sm:space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center gap-3">
            <motion.h1
              initial={{ opacity: 0, y: -8 }}
              animate={{ opacity: 1, y: 0 }}
              className="text-2xl sm:text-4xl font-black tracking-tight"
              style={{ color: 'var(--color-heading)', fontFamily: 'var(--font-mono)' }}
            >
              {sym}
            </motion.h1>
            {meta && (
              <span className="text-sm px-2 py-0.5 rounded"
                style={{ background: 'var(--color-border)', color: 'var(--color-muted)' }}>
                {meta.sector}
              </span>
            )}
            {fundamentals?.exchange && (
              <span className="text-xs px-2 py-0.5 rounded"
                style={{ background: 'var(--color-surface)', color: 'var(--color-muted)' }}>
                {fundamentals.exchange}
              </span>
            )}
            <button onClick={() => watchlist.has(sym) ? watchlist.remove(sym) : watchlist.add(sym)}>
              <Star
                size={20}
                fill={watchlist.has(sym) ? 'var(--color-accent)' : 'none'}
                style={{ color: watchlist.has(sym) ? 'var(--color-accent)' : 'var(--color-muted)' }}
              />
            </button>
          </div>
          {meta && (
            <p className="text-sm mt-0.5" style={{ color: 'var(--color-muted)' }}>{meta.name}</p>
          )}
          {latest && (
            <div className="flex items-baseline gap-3 mt-1">
              <span className="text-xl sm:text-3xl font-bold tabular" style={{ color: 'var(--color-heading)' }}>
                {latest.close.toLocaleString(undefined, { minimumFractionDigits: 2 })}
              </span>
              <span className="text-sm" style={{ color: 'var(--color-muted)' }}>{latest.currency}</span>
              <ChangeBadge value={rangeReturn} />
              {activeRange && fromPt && toPt && (
                <span className="text-xs" style={{ color: 'var(--color-muted)' }}>
                  {activeRange.from} → {activeRange.to}
                </span>
              )}
            </div>
          )}
        </div>
        <span className="text-xs tabular" style={{ color: 'var(--color-muted)' }}>
          {latest?.date}
        </span>
      </div>

      {/* OHLCV stats row */}
      {latest && (
        <div className="grid grid-cols-3 sm:grid-cols-5 gap-2 sm:gap-4">
          {[
            { label: t('ticker.open'),   value: latest.open.toFixed(2) },
            { label: t('ticker.high'),   value: latest.high.toFixed(2) },
            { label: t('ticker.low'),    value: latest.low.toFixed(2) },
            { label: t('ticker.close'),  value: latest.close.toFixed(2) },
            { label: t('ticker.volume'), value: (latest.volume / 1e6).toFixed(1) + 'M' },
          ].map(s => (
            <Card key={s.label} className="p-3">
              <StatBox label={s.label} value={s.value} />
            </Card>
          ))}
        </div>
      )}

      {/* Fundamentals loading/error states */}
      {!isCommodity && fundamentalsLoading && <Spinner />}
      {!isCommodity && fundamentalsError && (
        <p className="text-xs" style={{ color: 'var(--color-muted)' }}>
          Fundamentals unavailable: {String(fundamentalsError)}
        </p>
      )}

      {/* Key metrics strip */}
      {!isCommodity && fundamentals && lastYear && (
        <div className="grid grid-cols-3 sm:grid-cols-6 gap-2">
          <MetricPill
            label="Mkt Cap"
            value={mktCapBn ? fmtBn(mktCapBn) + ' KZT' : '—'}
          />
          <MetricPill
            label="P/E"
            value={fmt(pe, 1) + 'x'}
            sub={`EPS ${lastYear.eps.toLocaleString()}`}
          />
          <MetricPill
            label="P/B"
            value={fmt(pb, 2) + 'x'}
            sub={`BV ${lastYear.book_value_ps.toLocaleString()}`}
          />
          <MetricPill
            label="EV/EBITDA"
            value={fmt(evEbitda, 1) + 'x'}
            sub={`EBITDA ${fmtBn(lastYear.ebitda_bn)}`}
          />
          <MetricPill
            label="Div Yield"
            value={divY != null ? fmt(divY, 1) + '%' : '—'}
            sub={`DPS ${lastYear.dps.toLocaleString()}`}
          />
          <MetricPill
            label="52W Range"
            value={w52High && w52Low ? `${w52Low.toFixed(0)} – ${w52High.toFixed(0)}` : '—'}
          />
        </div>
      )}

      {/* Chart */}
      <Card className="p-4">
        <div className="flex items-center justify-between mb-4">
          <SectionHeader title={sym} />
          <div className="flex gap-1">
            {(['1M', '3M', '1Y', 'ALL'] as Timeframe[]).map(frame => (
              <button key={frame}
                onClick={() => { setTf(frame); setBrushDates(null); setLiveDates(null) }}
                className="px-3 py-1 rounded text-xs font-medium transition-colors"
                style={{
                  background: tf === frame ? 'var(--color-accent)' : 'var(--color-border)',
                  color: tf === frame ? '#000' : 'var(--color-text)',
                }}>
                {t(`ticker.timeframe.${frame}`)}
              </button>
            ))}
          </div>
        </div>

        {pricesLoading ? <Spinner /> : (
          <div style={{ position: 'relative', userSelect: 'none' }}>
            <ResponsiveContainer width="100%" height={240}>
              <AreaChart data={chartData} margin={{ top: 4, right: 0, left: 0, bottom: 0 }}>
                <defs>
                  <linearGradient id="priceGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={isUp ? '#22c55e' : '#ef4444'} stopOpacity={0.3} />
                    <stop offset="100%" stopColor={isUp ? '#22c55e' : '#ef4444'} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="date"
                  tick={{ fontSize: 10, fill: 'var(--color-muted)' }}
                  tickFormatter={v => v.slice(5)}
                  interval="preserveStartEnd"
                  axisLine={false} tickLine={false} />
                <YAxis domain={domain}
                  tick={{ fontSize: 10, fill: 'var(--color-muted)' }}
                  tickFormatter={v => Number(v).toFixed(0)}
                  axisLine={false} tickLine={false} width={YAXIS_W} />
                {activeRange && (
                  <ReferenceArea
                    x1={activeRange.from}
                    x2={activeRange.to}
                    fill="var(--color-accent)"
                    fillOpacity={0.12}
                    stroke="var(--color-accent)"
                    strokeOpacity={0.4}
                    strokeWidth={1}
                  />
                )}
                <Tooltip content={() => null} cursor={false} />
                <Area type="monotone" dataKey="close"
                  stroke={isUp ? 'var(--color-up)' : 'var(--color-down)'}
                  strokeWidth={2} fill="url(#priceGrad)" dot={false} />
              </AreaChart>
            </ResponsiveContainer>

            <div
              ref={overlayRef}
              style={{
                position: 'absolute',
                inset: 0,
                cursor: 'crosshair',
                touchAction: 'none',
                WebkitUserSelect: 'none',
                userSelect: 'none',
              }}
            />
          </div>
        )}
      </Card>

      {/* Financials section */}
      {!isCommodity && fundamentals && finChartData.length > 0 && (
        <div className="space-y-4">
          <SectionHeader title="Financials" />

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {/* Revenue & Net Income bars */}
            <Card className="p-4">
              <p className="text-xs uppercase tracking-wider mb-3" style={{ color: 'var(--color-muted)' }}>
                Revenue &amp; Net Income (KZT bn)
              </p>
              <ResponsiveContainer width="100%" height={160}>
                <BarChart data={finChartData} barCategoryGap="30%">
                  <XAxis dataKey="year" tick={{ fontSize: 11, fill: 'var(--color-muted)' }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fontSize: 10, fill: 'var(--color-muted)' }} axisLine={false} tickLine={false} width={45}
                    tickFormatter={v => fmtBn(v)} />
                  <Tooltip
                    contentStyle={{ background: 'var(--color-surface)', border: '1px solid var(--color-border)', borderRadius: 6, fontSize: 12 }}
                    labelStyle={{ color: 'var(--color-heading)' }}
                    itemStyle={{ color: 'var(--color-text)' }}
                    formatter={(v: unknown) => [fmtBn(v as number), '']}
                  />
                  <Bar dataKey="revenue" name="Revenue" fill="#3b82f6" radius={[3, 3, 0, 0]} />
                  <Bar dataKey="net_income" name="Net Income" fill="#22c55e" radius={[3, 3, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </Card>

            {/* EBITDA bars */}
            <Card className="p-4">
              <p className="text-xs uppercase tracking-wider mb-3" style={{ color: 'var(--color-muted)' }}>
                EBITDA &amp; CapEx (KZT bn)
              </p>
              <ResponsiveContainer width="100%" height={160}>
                <BarChart data={finChartData.map(d => ({ ...d, capex: fundamentals.annual.find(a => String(a.year) === d.year)?.capex_bn ?? 0 }))} barCategoryGap="30%">
                  <XAxis dataKey="year" tick={{ fontSize: 11, fill: 'var(--color-muted)' }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fontSize: 10, fill: 'var(--color-muted)' }} axisLine={false} tickLine={false} width={45}
                    tickFormatter={v => fmtBn(v)} />
                  <Tooltip
                    contentStyle={{ background: 'var(--color-surface)', border: '1px solid var(--color-border)', borderRadius: 6, fontSize: 12 }}
                    labelStyle={{ color: 'var(--color-heading)' }}
                    itemStyle={{ color: 'var(--color-text)' }}
                    formatter={(v: unknown) => [fmtBn(v as number), '']}
                  />
                  <Bar dataKey="ebitda" name="EBITDA" fill="#f59e0b" radius={[3, 3, 0, 0]} />
                  <Bar dataKey="capex" name="CapEx" fill="#8b5cf6" radius={[3, 3, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </Card>
          </div>

          {/* Balance sheet snapshot */}
          <Card className="p-4">
            <p className="text-xs uppercase tracking-wider mb-3" style={{ color: 'var(--color-muted)' }}>
              Balance Sheet (KZT bn) — {lastYear?.year}
            </p>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
              {[
                { label: 'Total Assets',  value: lastYear ? fmtBn(lastYear.total_assets_bn)  : '—' },
                { label: 'Total Equity',  value: lastYear ? fmtBn(lastYear.total_equity_bn)  : '—' },
                { label: 'Total Debt',    value: lastYear ? fmtBn(lastYear.total_debt_bn)    : '—' },
                { label: 'Cash',          value: lastYear ? fmtBn(lastYear.cash_bn)          : '—' },
              ].map(s => (
                <StatBox key={s.label} label={s.label} value={s.value} />
              ))}
            </div>
          </Card>

          {/* Annual table */}
          <Card className="p-4 overflow-x-auto">
            <p className="text-xs uppercase tracking-wider mb-3" style={{ color: 'var(--color-muted)' }}>
              Annual Summary (KZT bn)
            </p>
            <table className="w-full text-xs" style={{ color: 'var(--color-text)', borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid var(--color-border)' }}>
                  {['Year', 'Revenue', 'EBITDA', 'Net Income', 'CapEx', 'EPS', 'DPS'].map(h => (
                    <th key={h} className="text-left py-2 pr-4 font-medium" style={{ color: 'var(--color-muted)' }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {[...fundamentals.annual].reverse().map(a => (
                  <tr key={a.year} style={{ borderBottom: '1px solid var(--color-border)' }}>
                    <td className="py-2 pr-4 font-semibold" style={{ color: 'var(--color-heading)' }}>{a.year}</td>
                    <td className="py-2 pr-4">{fmtBn(a.revenue_bn)}</td>
                    <td className="py-2 pr-4">{fmtBn(a.ebitda_bn)}</td>
                    <td className="py-2 pr-4">{fmtBn(a.net_income_bn)}</td>
                    <td className="py-2 pr-4">{fmtBn(a.capex_bn)}</td>
                    <td className="py-2 pr-4">{a.eps.toLocaleString()}</td>
                    <td className="py-2 pr-4">{a.dps.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        </div>
      )}

      {/* Company overview */}
      {!isCommodity && fundamentals && (
        <Card className="p-5">
          <div className="flex items-start gap-3">
            <Building2 size={16} style={{ color: 'var(--color-muted)', marginTop: 2, flexShrink: 0 }} />
            <div className="space-y-3">
              <p className="text-sm leading-relaxed" style={{ color: 'var(--color-text)' }}>
                {fundamentals.description}
              </p>
              {fundamentals.market_position && (
                <p className="text-sm font-medium" style={{ color: 'var(--color-heading)' }}>
                  {fundamentals.market_position}
                </p>
              )}
              {/* Sector KPIs */}
              {Object.keys(fundamentals.sector_kpis).length > 0 && (
                <div className="flex flex-wrap gap-3 pt-1">
                  {Object.entries(fundamentals.sector_kpis).map(([k, v]) => (
                    <div key={k} className="flex flex-col gap-0.5">
                      <span className="text-xs" style={{ color: 'var(--color-muted)' }}>
                        {KPI_LABELS[k] ?? k}
                      </span>
                      <span className="text-sm font-semibold tabular" style={{ color: 'var(--color-heading)' }}>
                        {typeof v === 'number' ? v.toLocaleString() : v}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </Card>
      )}

      {/* News */}
      <div>
        <SectionHeader title={t('ticker.news')} />
        {newsLoading ? <Spinner /> : !news?.length
          ? <p className="text-sm" style={{ color: 'var(--color-muted)' }}>{t('ticker.noNews')}</p>
          : (
            <div className="space-y-2">
              {news.map((a, i) => (
                <motion.div key={a.article_id}
                  initial={{ opacity: 0, y: 6 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: i * 0.03 }}>
                  <Card className="p-3 hover:border-amber-900/60 transition-colors cursor-pointer">
                    <a href={a.url} target="_blank" rel="noopener noreferrer" className="block">
                      <div className="flex items-start gap-2">
                        <ImpactDot impact={a.impact} />
                        <div className="flex-1 min-w-0">
                          <p className="text-sm font-medium leading-snug" style={{ color: 'var(--color-heading)' }}>
                            {a.title}
                          </p>
                          <div className="flex items-center gap-2 mt-1">
                            <SourceBadge source={a.source} />
                            <span className="text-xs" style={{ color: 'var(--color-muted)' }}>
                              {a.pub_date?.slice(0, 10)}
                            </span>
                          </div>
                        </div>
                        <ExternalLink size={12} style={{ color: 'var(--color-muted)', flexShrink: 0 }} />
                      </div>
                    </a>
                  </Card>
                </motion.div>
              ))}
            </div>
          )}
      </div>
    </div>
  )
}
