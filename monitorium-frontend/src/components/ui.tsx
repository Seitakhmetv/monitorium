import { motion, AnimatePresence } from 'framer-motion'
import { Loader2 } from 'lucide-react'
import { useTranslation } from 'react-i18next'

// ── Spinner ────────────────────────────────────────────────────────────────────
export function Spinner({ size = 20 }: { size?: number }) {
  return (
    <div className="flex items-center justify-center p-8">
      <Loader2 size={size} className="animate-spin" style={{ color: 'var(--color-accent)' }} />
    </div>
  )
}

// ── Error state ────────────────────────────────────────────────────────────────
export function ErrorState({ onRetry }: { onRetry?: () => void }) {
  const { t } = useTranslation()
  return (
    <div className="flex flex-col items-center gap-3 p-12 text-center">
      <span style={{ color: 'var(--color-down)' }}>{t('common.error')}</span>
      {onRetry && (
        <button onClick={onRetry}
          className="px-4 py-1.5 rounded text-sm"
          style={{ background: 'var(--color-border)', color: 'var(--color-heading)' }}>
          {t('common.retry')}
        </button>
      )}
    </div>
  )
}

// ── Card ───────────────────────────────────────────────────────────────────────
export function Card({ children, className = '' }: { children: React.ReactNode; className?: string }) {
  return (
    <div
      className={`rounded-xl border ${className}`}
      style={{ background: 'var(--color-surface)', borderColor: 'var(--color-border)' }}
    >
      {children}
    </div>
  )
}

// ── Change badge ───────────────────────────────────────────────────────────────
export function ChangeBadge({ value }: { value: number | null | undefined }) {
  if (value == null) return <span style={{ color: 'var(--color-muted)' }}>—</span>
  const up = value >= 0
  return (
    <motion.span
      initial={{ scale: 0.8, opacity: 0 }}
      animate={{ scale: 1, opacity: 1 }}
      className="tabular font-medium text-sm"
      style={{ color: up ? 'var(--color-up)' : 'var(--color-down)' }}
    >
      {up ? '+' : ''}{value.toFixed(2)}%
    </motion.span>
  )
}

// ── Animated number ────────────────────────────────────────────────────────────
export function Num({ value, decimals = 2, prefix = '' }: { value: number | null; decimals?: number; prefix?: string }) {
  if (value == null) return <span style={{ color: 'var(--color-muted)' }}>—</span>
  return (
    <AnimatePresence mode="wait">
      <motion.span
        key={value}
        initial={{ opacity: 0, y: -4 }}
        animate={{ opacity: 1, y: 0 }}
        className="tabular"
        style={{ color: 'var(--color-heading)' }}
      >
        {prefix}{value.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })}
      </motion.span>
    </AnimatePresence>
  )
}

// ── Impact dot ─────────────────────────────────────────────────────────────────
export function ImpactDot({ impact }: { impact: string }) {
  const color =
    impact === 'positive' ? 'var(--color-up)' :
    impact === 'negative' ? 'var(--color-down)' :
    'var(--color-muted)'
  return <span className="inline-block w-2 h-2 rounded-full mr-1.5" style={{ background: color }} />
}

// ── Source badge ───────────────────────────────────────────────────────────────
const SOURCE_COLORS: Record<string, string> = {
  kursiv:   '#3b82f6',
  kapital:  '#8b5cf6',
  kase_news:'#f59e0b',
  adilet:   '#10b981',
  news:     '#6b7280',
}

export function SourceBadge({ source }: { source: string }) {
  return (
    <span className="px-1.5 py-0.5 rounded text-xs font-medium uppercase"
      style={{ background: SOURCE_COLORS[source] + '22', color: SOURCE_COLORS[source] || 'var(--color-muted)' }}>
      {source}
    </span>
  )
}

// ── Section header ─────────────────────────────────────────────────────────────
export function SectionHeader({ title, action }: { title: string; action?: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between mb-4">
      <h2 className="text-sm font-semibold uppercase tracking-widest"
        style={{ color: 'var(--color-muted)' }}>
        {title}
      </h2>
      {action}
    </div>
  )
}
