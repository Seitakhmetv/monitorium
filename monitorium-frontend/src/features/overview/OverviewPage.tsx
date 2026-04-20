import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { Link } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Star, Globe, AlertCircle, ChevronDown,
} from 'lucide-react'
import { getLatestPrice } from '../../api/prices'
import { getNews } from '../../api/news'
import { getSummary } from '../../api/summary'
import { useWatchlist } from '../../store/watchlist'
import { ChangeBadge, Num, Card, SourceBadge, ImpactDot } from '../../components/ui'
import { TICKERS, COMMODITY_STRIP, NEWS_SOURCES, getTicker } from '../../config'

// ─── helpers ──────────────────────────────────────────────────────────────────

function timeAgo(iso: string): string {
  if (!iso) return ''
  const diffMs = Date.now() - new Date(iso).getTime()
  const mins = Math.floor(diffMs / 60_000)
  if (mins < 1) return 'just now'
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ago`
  return `${Math.floor(hrs / 24)}d ago`
}

// ─── AI Briefing card ─────────────────────────────────────────────────────────

function BriefingCard() {
  const { i18n, t } = useTranslation()
  const lang = i18n.language.startsWith('ru') || i18n.language === 'kz' ? 'ru' : 'en'
  const [expanded, setExpanded] = useState(false)

  const { data, isLoading, isError } = useQuery({
    queryKey: ['summary', lang],
    queryFn: () => getSummary(lang),
    staleTime: 4 * 60 * 60_000,
    retry: false,
  })

  const isEmpty = !data || (!data.headline && data.kz_bullets.length === 0)

  return (
    <Card className="overflow-hidden">
      {/* header row — tap to expand on mobile */}
      <button
        className="w-full flex items-center justify-between px-5 py-3 border-b sm:cursor-default"
        style={{ borderColor: 'var(--color-border)' }}
        onClick={() => setExpanded((e: boolean) => !e)}
      >
        <span className="text-xs font-semibold uppercase tracking-widest" style={{ color: 'var(--color-muted)' }}>
          {t('overview.briefing')}
        </span>
        <ChevronDown
          size={14}
          className="sm:hidden transition-transform"
          style={{
            color: 'var(--color-muted)',
            transform: expanded ? 'rotate(180deg)' : 'rotate(0deg)',
          }}
        />
      </button>

      {/* body — always visible on sm+, toggleable on mobile */}
      <div className={`px-5 py-4 ${expanded ? '' : 'hidden sm:block'}`}>
        {isLoading && (
          <div className="space-y-2 animate-pulse">
            <div className="h-4 rounded w-3/4" style={{ background: 'var(--color-border)' }} />
            <div className="h-3 rounded w-1/2" style={{ background: 'var(--color-border)' }} />
            <div className="h-3 rounded w-2/3" style={{ background: 'var(--color-border)' }} />
          </div>
        )}

        {isError && (
          <div className="flex items-center gap-2 text-sm" style={{ color: 'var(--color-muted)' }}>
            <AlertCircle size={14} />
            {t('overview.briefingUnavailable')}
          </div>
        )}

        {!isLoading && !isError && isEmpty && (
          <p className="text-sm" style={{ color: 'var(--color-muted)' }}>
            {t('overview.briefingUnavailable')}
          </p>
        )}

        {!isLoading && !isError && !isEmpty && data && (
          <AnimatePresence mode="wait">
            <motion.div
              key={data.generated_at}
              initial={{ opacity: 0, y: 6 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.25 }}
            >
              {data.headline && (
                <p
                  className="text-sm font-semibold mb-4 leading-relaxed"
                  style={{ color: 'var(--color-heading)' }}
                >
                  {data.headline}
                </p>
              )}

              <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-3">
                {data.kz_bullets.length > 0 && (
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-widest mb-2"
                      style={{ color: 'var(--color-accent)' }}>
                      Kazakhstan
                    </p>
                    <ul className="space-y-1.5">
                      {data.kz_bullets.map((b, i) => (
                        <li key={i} className="flex gap-2 text-xs leading-relaxed">
                          <span className="shrink-0 mt-0.5" style={{ color: 'var(--color-accent)' }}>•</span>
                          <span>
                            {b.url
                              ? <a href={b.url} target="_blank" rel="noopener noreferrer"
                                  className="hover:underline hover:text-white transition-colors"
                                  style={{ color: 'var(--color-text)' }}>{b.text}</a>
                              : <span style={{ color: 'var(--color-text)' }}>{b.text}</span>
                            }
                            {b.source && <SourceBadge source={b.source} />}
                          </span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

                {data.world_bullets.length > 0 && (
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-widest mb-2"
                      style={{ color: '#60a5fa' }}>
                      Global
                    </p>
                    <ul className="space-y-1.5">
                      {data.world_bullets.map((b, i) => (
                        <li key={i} className="flex gap-2 text-xs leading-relaxed">
                          <span className="shrink-0 mt-0.5" style={{ color: '#60a5fa' }}>•</span>
                          <span>
                            {b.url
                              ? <a href={b.url} target="_blank" rel="noopener noreferrer"
                                  className="hover:underline hover:text-white transition-colors"
                                  style={{ color: 'var(--color-text)' }}>{b.text}</a>
                              : <span style={{ color: 'var(--color-text)' }}>{b.text}</span>
                            }
                            {b.source && <span className="ml-1.5"><SourceBadge source={b.source} /></span>}
                          </span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            </motion.div>
          </AnimatePresence>
        )}
      </div>
    </Card>
  )
}

// ─── Single ticker row ────────────────────────────────────────────────────────

function TickerRow({ ticker, index }: { ticker: string; index: number }) {
  const watchlist = useWatchlist()
  const meta = getTicker(ticker)
  const { data, isLoading, isError } = useQuery({
    queryKey: ['latest', ticker],
    queryFn: () => getLatestPrice(ticker),
    staleTime: 60_000,
    retry: false,
  })

  const changePct = data && data.open && data.open !== 0
    ? ((data.close - data.open) / data.open) * 100
    : null

  return (
    <motion.tr
      initial={{ opacity: 0, x: -8 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ delay: index * 0.03 }}
      className="group border-b"
      style={{ borderColor: 'var(--color-border)' }}
    >
      <td className="py-2.5 pl-4">
        <button
          onClick={() => watchlist.has(ticker) ? watchlist.remove(ticker) : watchlist.add(ticker)}
          className="opacity-0 group-hover:opacity-100 transition-opacity"
        >
          <Star
            size={12}
            fill={watchlist.has(ticker) ? 'var(--color-accent)' : 'none'}
            style={{ color: watchlist.has(ticker) ? 'var(--color-accent)' : 'var(--color-muted)' }}
          />
        </button>
      </td>
      <td className="py-2.5">
        <Link
          to={`/ticker/${ticker}`}
          className="font-bold text-xs hover:text-amber-400 transition-colors"
          style={{ color: 'var(--color-heading)', fontFamily: 'var(--font-mono)' }}
        >
          {ticker}
        </Link>
      </td>
      <td className="py-2.5 pr-2">
        <span className="text-xs truncate block" style={{ color: 'var(--color-muted)' }}>
          {meta?.name ?? ''}
        </span>
      </td>
      <td className="py-2.5 pr-3 text-right">
        {isLoading
          ? <span style={{ color: 'var(--color-muted)' }} className="text-xs">…</span>
          : isError
          ? <span style={{ color: 'var(--color-muted)' }} className="text-xs">—</span>
          : <Num value={data?.close ?? null} />}
      </td>
      <td className="py-2.5 pr-4 text-right">
        {isLoading ? '' : <ChangeBadge value={changePct} />}
      </td>
    </motion.tr>
  )
}

// ─── KZ stocks panel ──────────────────────────────────────────────────────────

function StocksPanel() {
  const { t } = useTranslation()
  const tickers = TICKERS.kz.map(t => t.symbol)

  return (
    <Card className="overflow-hidden flex flex-col">
      <div className="px-4 py-3 border-b shrink-0" style={{ borderColor: 'var(--color-border)' }}>
        <h3 className="text-xs font-semibold uppercase tracking-widest" style={{ color: 'var(--color-muted)' }}>
          KASE / AIX
        </h3>
      </div>
      {/* 5 rows × ~37px + sticky header ~33px ≈ 218px on mobile; unconstrained on lg */}
      <div className="overflow-y-auto flex-1" style={{ maxHeight: 'min(218px, 40vh)' }}
           data-lg-maxheight="none">
        <table className="w-full table-fixed">
          <colgroup>
            <col style={{ width: 28 }} />
            <col style={{ width: 52 }} />
            <col className="hidden sm:table-column" />
            <col style={{ width: 80 }} />
            <col style={{ width: 64 }} />
          </colgroup>
          <thead className="sticky top-0" style={{ background: 'var(--color-surface)' }}>
            <tr className="text-xs border-b" style={{ color: 'var(--color-muted)', borderColor: 'var(--color-border)' }}>
              <th className="py-2 pl-4" />
              <th className="py-2 text-left">{t('overview.ticker')}</th>
              <th className="py-2 pr-2 text-left hidden sm:table-cell">Name</th>
              <th className="py-2 pr-3 text-right">{t('overview.price')}</th>
              <th className="py-2 pr-4 text-right">{t('overview.change')}</th>
            </tr>
          </thead>
          <tbody>
            {tickers.map((sym, i) => (
              <TickerRow key={sym} ticker={sym} index={i} />
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  )
}

// ─── Commodity row ────────────────────────────────────────────────────────────

function CommodityRow({ item, index }: { item: typeof COMMODITY_STRIP[number]; index: number }) {
  const { data, isLoading } = useQuery({
    queryKey: ['latest', item.symbol],
    queryFn: () => getLatestPrice(item.symbol),
    staleTime: 60_000,
    retry: false,
  })

  const changePct = data && data.open && data.open !== 0
    ? ((data.close - data.open) / data.open) * 100
    : null

  return (
    <motion.div
      initial={{ opacity: 0, x: -6 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ delay: index * 0.04 }}
      className="flex items-center justify-between py-2.5 border-b"
      style={{ borderColor: 'var(--color-border)' }}
    >
      <Link to={`/ticker/${item.symbol}`} className="flex items-center gap-2 pl-4 group/link">
        <span className="text-base leading-none">{item.icon}</span>
        <span className="text-xs font-medium group-hover/link:text-amber-400 transition-colors"
              style={{ color: 'var(--color-heading)' }}>
          {item.label}
        </span>
      </Link>
      <div className="flex items-center gap-3 pr-4">
        {isLoading
          ? <span className="text-xs" style={{ color: 'var(--color-muted)' }}>…</span>
          : <Num value={data?.close ?? null} />}
        <ChangeBadge value={changePct} />
      </div>
    </motion.div>
  )
}

// ─── Commodities & FX panel ───────────────────────────────────────────────────

function CommoditiesPanel() {
  return (
    <Card className="overflow-hidden flex flex-col">
      <div className="px-4 py-3 border-b shrink-0" style={{ borderColor: 'var(--color-border)' }}>
        <h3 className="text-xs font-semibold uppercase tracking-widest" style={{ color: 'var(--color-muted)' }}>
          Commodities & FX
        </h3>
      </div>
      <div className="overflow-y-auto flex-1" style={{ maxHeight: 'min(218px, 40vh)' }} data-lg-maxheight="none">
        {[...COMMODITY_STRIP].map((item, i) => (
          <CommodityRow key={item.symbol} item={item} index={i} />
        ))}
      </div>
    </Card>
  )
}

// ─── News feed panel ──────────────────────────────────────────────────────────

function NewsFeed() {
  const { t } = useTranslation()
  const { data, isLoading } = useQuery({
    queryKey: ['news-feed'],
    queryFn: () => getNews({ limit: 20 }),
    staleTime: 120_000,
    retry: false,
  })

  return (
    <Card className="overflow-hidden flex flex-col">
      <div className="px-4 py-3 border-b shrink-0 flex items-center gap-2"
        style={{ borderColor: 'var(--color-border)' }}>
        <Globe size={12} style={{ color: 'var(--color-muted)' }} />
        <h3 className="text-xs font-semibold uppercase tracking-widest" style={{ color: 'var(--color-muted)' }}>
          {t('overview.latestNews')}
        </h3>
        <div className="flex gap-1.5 ml-auto">
          {NEWS_SOURCES.map(s => (
            <span
              key={s.id}
              className="px-1.5 py-0.5 rounded text-xs font-medium"
              style={{ background: s.color + '22', color: s.color }}
            >
              {s.label}
            </span>
          ))}
        </div>
      </div>

      {/* 5 news items × ~64px ≈ 320px on mobile */}
      <div className="overflow-y-auto flex-1" style={{ maxHeight: 'min(320px, 50vh)' }} data-lg-maxheight="none">
        {isLoading && (
          <div className="space-y-3 p-4 animate-pulse">
            {Array.from({ length: 6 }).map((_, i) => (
              <div key={i} className="space-y-1.5">
                <div className="h-3 rounded w-full" style={{ background: 'var(--color-border)' }} />
                <div className="h-3 rounded w-4/5" style={{ background: 'var(--color-border)' }} />
              </div>
            ))}
          </div>
        )}

        {!isLoading && data?.map((article, i) => (
          <motion.a
            key={article.article_id}
            href={article.url}
            target="_blank"
            rel="noopener noreferrer"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: i * 0.025 }}
            className="flex gap-2.5 px-4 py-3 border-b group hover:bg-white/[0.03] transition-colors"
            style={{ borderColor: 'var(--color-border)' }}
          >
            <div className="shrink-0 mt-0.5">
              <ImpactDot impact={article.impact} />
            </div>
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-1.5 mb-1 flex-wrap">
                <SourceBadge source={article.source} />
                <span className="text-xs" style={{ color: 'var(--color-muted)' }}>
                  {timeAgo(article.pub_date)}
                </span>
              </div>
              <p
                className="text-xs leading-relaxed line-clamp-2 group-hover:text-white transition-colors"
                style={{ color: 'var(--color-text)' }}
              >
                {article.title}
              </p>
            </div>
          </motion.a>
        ))}

        {!isLoading && (!data || data.length === 0) && (
          <p className="p-4 text-xs" style={{ color: 'var(--color-muted)' }}>
            {t('news.noArticles')}
          </p>
        )}
      </div>
    </Card>
  )
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function OverviewPage() {
  const { t } = useTranslation()

  return (
    <div className="p-4 lg:p-6 max-w-[1400px] mx-auto h-full flex flex-col gap-4">
      <div className="shrink-0">
        <motion.h1
          initial={{ opacity: 0, y: -8 }}
          animate={{ opacity: 1, y: 0 }}
          className="text-xl font-black tracking-tight"
          style={{ color: 'var(--color-heading)' }}
        >
          {t('overview.title')}
        </motion.h1>
        <motion.p
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.08 }}
          className="text-xs"
          style={{ color: 'var(--color-muted)' }}
        >
          {t('overview.subtitle')}
        </motion.p>
      </div>

      <motion.div
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        className="shrink-0"
      >
        <BriefingCard />
      </motion.div>

      <div className="grid grid-cols-1 lg:grid-cols-[5fr_3fr_4fr] gap-4 flex-1 min-h-0">
        <StocksPanel />
        <CommoditiesPanel />
        <NewsFeed />
      </div>
    </div>
  )
}
