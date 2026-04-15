import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { ExternalLink } from 'lucide-react'
import { getNews } from '../../api/news'
import { Spinner, ErrorState, ImpactDot, SourceBadge, Card } from '../../components/ui'
import { useDebounce } from '../../hooks/useDebounce'
import { NEWS_SOURCES, NEWS_TOPICS } from '../../config'

export default function NewsPage() {
  const { t } = useTranslation()
  const [ticker, setTicker] = useState('')
  const [source, setSource] = useState('')
  const [topic, setTopic] = useState('')
  const debouncedTicker = useDebounce(ticker)

  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['news', debouncedTicker, source, topic],
    queryFn: () => getNews({ ticker: debouncedTicker || undefined, source: source || undefined, topic: topic || undefined, limit: 100 }),
    staleTime: 60_000,
  })

  const selectStyle = {
    background: 'var(--color-surface)',
    border: '1px solid var(--color-border)',
    color: 'var(--color-text)',
    borderRadius: 8,
    padding: '6px 10px',
    fontSize: 13,
    outline: 'none',
  }

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <motion.h1
        initial={{ opacity: 0, y: -8 }}
        animate={{ opacity: 1, y: 0 }}
        className="text-3xl font-black tracking-tight mb-6"
        style={{ color: 'var(--color-heading)' }}
      >
        {t('news.title')}
      </motion.h1>

      {/* Filters */}
      <div className="flex flex-wrap gap-2 mb-6">
        <input
          value={ticker}
          onChange={e => setTicker(e.target.value.toUpperCase())}
          placeholder={t('news.filterTicker')}
          style={{ ...selectStyle, width: 130 }}
        />
        <select value={source} onChange={e => setSource(e.target.value)} style={selectStyle}>
          <option value="">{t('news.allSources')}</option>
          {NEWS_SOURCES.map(s => (
            <option key={s.id} value={s.id}>{s.label}</option>
          ))}
        </select>
        <select value={topic} onChange={e => setTopic(e.target.value)} style={selectStyle}>
          <option value="">{t('news.allTopics')}</option>
          {NEWS_TOPICS.map(s => (
            <option key={s.id} value={s.id}>{s.label}</option>
          ))}
        </select>
        {(ticker || source || topic) && (
          <button onClick={() => { setTicker(''); setSource(''); setTopic('') }}
            className="px-3 py-1.5 rounded text-xs"
            style={{ background: 'var(--color-border)', color: 'var(--color-text)' }}>
            Clear
          </button>
        )}
      </div>

      {isLoading && <Spinner />}
      {isError && <ErrorState onRetry={refetch} />}

      {data && (
        <div className="space-y-2">
          <p className="text-xs mb-3" style={{ color: 'var(--color-muted)' }}>
            {data.length} articles
          </p>
          <AnimatePresence mode="popLayout">
            {data.map((a, i) => (
              <motion.div key={a.article_id}
                layout
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, scale: 0.97 }}
                transition={{ delay: i < 20 ? i * 0.02 : 0 }}>
                <Card className="p-4 hover:border-amber-900/50 transition-colors">
                  <a href={a.url} target="_blank" rel="noopener noreferrer" className="flex items-start gap-3">
                    <div className="pt-0.5">
                      <ImpactDot impact={a.impact} />
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium leading-snug mb-2" style={{ color: 'var(--color-heading)' }}>
                        {a.title}
                      </p>
                      <div className="flex flex-wrap items-center gap-2">
                        <SourceBadge source={a.source} />
                        <span className="text-xs tabular" style={{ color: 'var(--color-muted)' }}>
                          {a.pub_date?.slice(0, 10)}
                        </span>
                        {a.companies && (
                          <span className="text-xs px-1.5 py-0.5 rounded"
                            style={{ background: 'var(--color-border)', color: 'var(--color-text)' }}>
                            {a.companies}
                          </span>
                        )}
                        {a.topics && (
                          <span className="text-xs" style={{ color: 'var(--color-accent)' }}>
                            {a.topics}
                          </span>
                        )}
                      </div>
                    </div>
                    <ExternalLink size={12} className="shrink-0 mt-0.5" style={{ color: 'var(--color-muted)' }} />
                  </a>
                </Card>
              </motion.div>
            ))}
          </AnimatePresence>
        </div>
      )}
    </div>
  )
}
