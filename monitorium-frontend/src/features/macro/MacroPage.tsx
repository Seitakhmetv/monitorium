import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { useState } from 'react'
import { motion } from 'framer-motion'
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { getMacro } from '../../api/macro'
import { Spinner, ErrorState, Card, SectionHeader } from '../../components/ui'
import { COUNTRIES, MACRO_INDICATORS } from '../../config'

// Color palette for multi-country lines
const LINE_COLORS = [
  '#f59e0b', '#3b82f6', '#22c55e', '#ef4444', '#8b5cf6',
  '#06b6d4', '#f97316', '#84cc16', '#ec4899', '#14b8a6',
  '#a78bfa', '#fb7185', '#34d399', '#fbbf24', '#60a5fa',
  '#e879f9',
]

export default function MacroPage() {
  const { t } = useTranslation()
  const [indicator, setIndicator] = useState<typeof MACRO_INDICATORS[number]['id']>(MACRO_INDICATORS[0].id)
  const [selectedCountries, setSelectedCountries] = useState<string[]>(['KZ', 'RU', 'US'])

  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['macro', indicator, selectedCountries],
    queryFn: () => getMacro({ indicator }),
    staleTime: 300_000,
  })

  // Pivot: [{year, KZ: val, US: val, ...}]
  const chartData = (() => {
    if (!data) return []
    const filtered = data.filter(d => selectedCountries.includes(d.country_code))
    const years = [...new Set(filtered.map(d => d.year))].sort()
    return years.map(year => {
      const row: Record<string, number | string> = { year: String(year) }
      for (const c of selectedCountries) {
        const point = filtered.find(d => d.year === year && d.country_code === c)
        if (point) row[c] = point.value
      }
      return row
    })
  })()

  const toggleCountry = (code: string) => {
    setSelectedCountries(prev =>
      prev.includes(code) ? prev.filter(c => c !== code) : [...prev, code]
    )
  }

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <motion.h1
        initial={{ opacity: 0, y: -8 }}
        animate={{ opacity: 1, y: 0 }}
        className="text-3xl font-black tracking-tight mb-6"
        style={{ color: 'var(--color-heading)' }}
      >
        {t('macro.title')}
      </motion.h1>

      {/* Indicator selector */}
      <div className="flex flex-wrap items-center gap-2 mb-6">
        {MACRO_INDICATORS.map(ind => (
          <button
            key={ind.id}
            onClick={() => setIndicator(ind.id)}
            className="px-4 py-1.5 rounded text-sm font-medium transition-colors"
            style={{
              background: indicator === ind.id ? 'var(--color-accent)' : 'var(--color-border)',
              color: indicator === ind.id ? '#000' : 'var(--color-text)',
            }}
          >
            {t(ind.labelKey)}
          </button>
        ))}
      </div>

      {/* Country toggles */}
      <div className="flex flex-wrap gap-1.5 mb-6">
        {COUNTRIES.map((c, i) => {
          const active = selectedCountries.includes(c.code)
          const color = LINE_COLORS[i % LINE_COLORS.length]
          return (
            <button
              key={c.code}
              onClick={() => toggleCountry(c.code)}
              className="px-2.5 py-1 rounded text-xs font-medium transition-all"
              style={{
                background: active ? color + '22' : 'var(--color-border)',
                color: active ? color : 'var(--color-muted)',
                border: `1px solid ${active ? color + '44' : 'transparent'}`,
              }}
            >
              {c.flag} {c.code}
            </button>
          )
        })}
      </div>

      {/* Chart */}
      <Card className="p-4">
        <SectionHeader
          title={t(MACRO_INDICATORS.find(i => i.id === indicator)?.labelKey ?? indicator)}
        />
        {isLoading && <Spinner />}
        {isError && <ErrorState onRetry={refetch} />}
        {!isLoading && !isError && chartData.length === 0 && (
          <p className="text-sm py-8 text-center" style={{ color: 'var(--color-muted)' }}>
            {t('macro.noData')}
          </p>
        )}
        {chartData.length > 0 && (
          <ResponsiveContainer width="100%" height={320}>
            <LineChart data={chartData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <XAxis dataKey="year"
                tick={{ fontSize: 10, fill: 'var(--color-muted)' }}
                axisLine={false} tickLine={false} />
              <YAxis
                tick={{ fontSize: 10, fill: 'var(--color-muted)' }}
                axisLine={false} tickLine={false} width={40}
                tickFormatter={v => `${v}%`} />
              <Tooltip
                contentStyle={{
                  background: 'var(--color-surface)',
                  border: '1px solid var(--color-border)',
                  borderRadius: 8,
                  fontSize: 12,
                }}
                labelStyle={{ color: 'var(--color-muted)' }}
                formatter={(value, name) => [`${Number(value).toFixed(2)}%`, name]}
              />
              <Legend
                wrapperStyle={{ fontSize: 11, color: 'var(--color-muted)', paddingTop: 8 }}
              />
              {selectedCountries.map((code) => (
                <Line
                  key={code}
                  type="monotone"
                  dataKey={code}
                  stroke={LINE_COLORS[COUNTRIES.findIndex(c => c.code === code) % LINE_COLORS.length]}
                  strokeWidth={2}
                  dot={false}
                  connectNulls
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        )}
      </Card>

      {/* Data table */}
      {data && selectedCountries.length > 0 && (
        <Card className="overflow-hidden mt-4">
          <div className="px-4 py-3 border-b" style={{ borderColor: 'var(--color-border)' }}>
            <h3 className="text-xs font-semibold uppercase tracking-widest" style={{ color: 'var(--color-muted)' }}>
              Latest values
            </h3>
          </div>
          <table className="w-full">
            <thead>
              <tr className="text-xs border-b" style={{ color: 'var(--color-muted)', borderColor: 'var(--color-border)' }}>
                <th className="py-2 pl-4 text-left">Country</th>
                <th className="py-2 pr-4 text-right">Latest year</th>
                <th className="py-2 pr-4 text-right">Value</th>
              </tr>
            </thead>
            <tbody>
              {selectedCountries.map((code, i) => {
                const countryData = data
                  .filter(d => d.country_code === code)
                  .sort((a, b) => b.year - a.year)
                const latest = countryData[0]
                const country = COUNTRIES.find(c => c.code === code)
                const color = LINE_COLORS[COUNTRIES.findIndex(c => c.code === code) % LINE_COLORS.length]
                return (
                  <motion.tr
                    key={code}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    transition={{ delay: i * 0.03 }}
                    className="border-b text-sm"
                    style={{ borderColor: 'var(--color-border)' }}
                  >
                    <td className="py-2.5 pl-4">
                      <span className="flex items-center gap-2">
                        <span className="w-2 h-2 rounded-full inline-block" style={{ background: color }} />
                        <span style={{ color: 'var(--color-heading)' }}>
                          {country?.flag} {country?.name ?? code}
                        </span>
                      </span>
                    </td>
                    <td className="py-2.5 pr-4 text-right tabular" style={{ color: 'var(--color-muted)' }}>
                      {latest?.year ?? '—'}
                    </td>
                    <td className="py-2.5 pr-4 text-right tabular font-medium" style={{ color: 'var(--color-heading)' }}>
                      {latest ? `${latest.value.toFixed(2)}%` : '—'}
                    </td>
                  </motion.tr>
                )
              })}
            </tbody>
          </table>
        </Card>
      )}
    </div>
  )
}
