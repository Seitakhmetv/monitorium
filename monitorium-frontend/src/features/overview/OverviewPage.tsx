import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Star } from 'lucide-react'
import { getLatestPrice } from '../../api/prices'
import { useWatchlist } from '../../store/watchlist'
import { ChangeBadge, Num, Card } from '../../components/ui'
import { TICKERS } from '../../config'

const KZ_TICKERS     = TICKERS.kz.map(t => t.symbol)
const GLOBAL_TICKERS = TICKERS.global.map(t => t.symbol)

function TickerRow({ ticker, index }: { ticker: string; index: number }) {
  const watchlist = useWatchlist()
  const { data, isLoading, isError } = useQuery({
    queryKey: ['latest', ticker],
    queryFn: () => getLatestPrice(ticker),
    staleTime: 60_000,
    retry: false,
  })

  const prev = data ? data.open : null
  const changePct = data && prev && prev !== 0
    ? ((data.close - prev) / prev) * 100
    : null

  return (
    <motion.tr
      initial={{ opacity: 0, x: -10 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ delay: index * 0.04 }}
      className="group border-b"
      style={{ borderColor: 'var(--color-border)' }}
    >
      <td className="py-3 pl-4 pr-2 w-8">
        <button
          onClick={() => watchlist.has(ticker) ? watchlist.remove(ticker) : watchlist.add(ticker)}
          className="opacity-0 group-hover:opacity-100 transition-opacity"
        >
          <Star
            size={14}
            fill={watchlist.has(ticker) ? 'var(--color-accent)' : 'none'}
            style={{ color: watchlist.has(ticker) ? 'var(--color-accent)' : 'var(--color-muted)' }}
          />
        </button>
      </td>
      <td className="py-3 pr-4">
        <Link to={`/ticker/${ticker}`}
          className="font-bold text-sm hover:text-amber-400 transition-colors"
          style={{ color: 'var(--color-heading)', fontFamily: 'var(--font-mono)' }}>
          {ticker}
        </Link>
      </td>
      <td className="py-3 pr-4 text-right">
        {isLoading ? <span style={{ color: 'var(--color-muted)' }}>…</span> :
         isError ? <span style={{ color: 'var(--color-muted)' }}>—</span> :
         <Num value={data?.close ?? null} />}
      </td>
      <td className="py-3 pr-4 text-right">
        {isLoading ? '…' : <ChangeBadge value={changePct} />}
      </td>
      <td className="py-3 pr-6 text-right text-xs" style={{ color: 'var(--color-muted)' }}>
        {data?.currency ?? ''}
      </td>
    </motion.tr>
  )
}

function TickerTable({ title, tickers }: { title: string; tickers: string[] }) {
  const { t } = useTranslation()
  return (
    <Card className="overflow-hidden">
      <div className="px-4 py-3 border-b" style={{ borderColor: 'var(--color-border)' }}>
        <h3 className="text-xs font-semibold uppercase tracking-widest" style={{ color: 'var(--color-muted)' }}>
          {title}
        </h3>
      </div>
      <table className="w-full">
        <thead>
          <tr className="text-xs" style={{ color: 'var(--color-muted)' }}>
            <th className="py-2 pl-4 w-8" />
            <th className="py-2 pr-4 text-left">{t('overview.ticker')}</th>
            <th className="py-2 pr-4 text-right">{t('overview.price')}</th>
            <th className="py-2 pr-4 text-right">{t('overview.change')}</th>
            <th className="py-2 pr-6 text-right"></th>
          </tr>
        </thead>
        <tbody>
          {tickers.map((ticker, i) => (
            <TickerRow key={ticker} ticker={ticker} index={i} />
          ))}
        </tbody>
      </table>
    </Card>
  )
}

export default function OverviewPage() {
  const { t } = useTranslation()

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <div className="mb-8">
        <motion.h1
          initial={{ opacity: 0, y: -12 }}
          animate={{ opacity: 1, y: 0 }}
          className="text-3xl font-black tracking-tight mb-1"
          style={{ color: 'var(--color-heading)' }}
        >
          {t('overview.title')}
        </motion.h1>
        <motion.p
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.1 }}
          className="text-sm"
          style={{ color: 'var(--color-muted)' }}
        >
          {t('overview.subtitle')}
        </motion.p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <TickerTable title="Kazakhstan (KASE)" tickers={KZ_TICKERS} />
        <TickerTable title="Global" tickers={GLOBAL_TICKERS} />
      </div>
    </div>
  )
}
