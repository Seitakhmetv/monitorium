import { useParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { useState, useRef, useCallback } from 'react'
import { motion } from 'framer-motion'
import { Star, ExternalLink } from 'lucide-react'
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceArea } from 'recharts'
import { getPrices, getLatestPrice } from '../../api/prices'
import { getNews } from '../../api/news'
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

export default function TickerPage() {
  const { ticker } = useParams<{ ticker: string }>()
  const { t } = useTranslation()
  const watchlist = useWatchlist()
  const [tf, setTf] = useState<Timeframe>('3M')
  const sym = ticker?.toUpperCase() ?? ''
  const meta = getTicker(sym)

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

  const chartData = prices ? [...prices].reverse().map(p => ({
    date: p.date,
    close: p.close,
  })) : []

  // ── Brush selection ────────────────────────────────────────────────────────
  // Committed range (persists after drag ends)
  const [brushDates, setBrushDates] = useState<{ from: string; to: string } | null>(null)
  // Live range while dragging
  const [liveDates,  setLiveDates]  = useState<{ from: string; to: string } | null>(null)

  const chartDataRef = useRef(chartData)
  chartDataRef.current = chartData

  // Convert clientX → nearest date string in chartData
  const xToDate = useCallback((clientX: number, rect: DOMRect): string | null => {
    const data = chartDataRef.current
    if (!data.length) return null
    const relX = Math.max(0, clientX - rect.left - YAXIS_W)
    const pct  = Math.min(1, relX / Math.max(1, rect.width - YAXIS_W))
    return data[Math.round(pct * (data.length - 1))].date
  }, [])

  // Callback-ref: fires when the overlay div actually mounts (after data loads)
  const dragRef = useRef<{ startDate: string; startX: number; startY: number } | null>(null)

  const overlayRef = useCallback((node: HTMLDivElement | null) => {
    if (!node) return

    // ── Mouse ────────────────────────────────────────────────────────────────
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
        // Click (no drag) → clear selection
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

    // ── Touch ────────────────────────────────────────────────────────────────
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

  return (
    <div className="p-6 max-w-5xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center gap-3">
            <motion.h1
              initial={{ opacity: 0, y: -8 }}
              animate={{ opacity: 1, y: 0 }}
              className="text-4xl font-black tracking-tight"
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
              <span className="text-3xl font-bold tabular" style={{ color: 'var(--color-heading)' }}>
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

      {/* Stats row */}
      {latest && (
        <div className="grid grid-cols-5 gap-4">
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
                {/* Shaded selection region */}
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

            {/* Drag-to-select overlay */}
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
