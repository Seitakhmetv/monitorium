import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { getLatestPrice } from '../api/prices'
import { ChangeBadge } from './ui'
import { COMMODITY_STRIP } from '../config'

function StripTile({ symbol, label, icon, currency }: {
  symbol: string
  label: string
  icon: string
  currency: string
}) {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['latest', symbol],
    queryFn: () => getLatestPrice(symbol),
    staleTime: 60_000,
    retry: false,
  })

  const changePct = data && data.open && data.open !== 0
    ? ((data.close - data.open) / data.open) * 100
    : null

  const price = isLoading || isError || !data
    ? '—'
    : data.close.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })

  return (
    <Link
      to={`/ticker/${symbol}`}
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: '2px',
        minWidth: '110px',
        padding: '8px 12px',
        borderRadius: '8px',
        background: 'var(--color-surface)',
        border: '1px solid var(--color-border)',
        textDecoration: 'none',
        flexShrink: 0,
        transition: 'border-color 0.15s',
      }}
      onMouseEnter={e => (e.currentTarget.style.borderColor = 'var(--color-accent)')}
      onMouseLeave={e => (e.currentTarget.style.borderColor = 'var(--color-border)')}
    >
      <span className="text-xs" style={{ color: 'var(--color-muted)' }}>
        {icon} {label}
      </span>
      <div className="flex items-baseline gap-1.5 flex-wrap">
        <span className="text-sm font-semibold tabular-nums" style={{ color: 'var(--color-heading)', fontFamily: 'var(--font-mono)' }}>
          {price}
        </span>
        {!isLoading && !isError && <ChangeBadge value={changePct} />}
      </div>
      <span className="text-xs" style={{ color: 'var(--color-muted)' }}>
        {currency}
      </span>
    </Link>
  )
}

export default function TickerStrip() {
  return (
    <div
      style={{
        display: 'flex',
        gap: '0.5rem',
        overflowX: 'auto',
        paddingBottom: '4px',
        scrollbarWidth: 'none',
        msOverflowStyle: 'none',
      }}
      className="[&::-webkit-scrollbar]:hidden"
    >
      {COMMODITY_STRIP.map(item => (
        <StripTile key={item.symbol} {...item} />
      ))}
    </div>
  )
}
