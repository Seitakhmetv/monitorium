import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { AnimatePresence, motion } from 'framer-motion'
import { X, ExternalLink, Layers } from 'lucide-react'
import MonitoriumMap, { type LayerKey, type ClickedFeature } from '../../components/MonitoriumMap'
import { fetchGeoJson, getFlights, getVessels } from '../../api/geo'
import { getNews } from '../../api/news'
import { getTicker } from '../../config'
import { ImpactDot } from '../../components/ui'

// ── Layer definitions ─────────────────────────────────────────────────────────

const LAYER_DEFS: { key: LayerKey; label: string; color: string }[] = [
  { key: 'mines',   label: 'Mines',        color: '#22c55e' },
  { key: 'oil',     label: 'Oil & Gas',    color: '#ef4444' },
  { key: 'rail',    label: 'Rail',         color: '#3d4a5c' },
  { key: 'ports',   label: 'Ports',        color: '#8b9ab0' },
  { key: 'flights', label: 'Live Flights', color: '#f59e0b' },
  { key: 'vessels', label: 'Caspian Ships',color: '#3b82f6' },
]

// ── Info sidebar ──────────────────────────────────────────────────────────────

function InfoSidebar({
  feature,
  onClose,
}: {
  feature: ClickedFeature
  onClose: () => void
}) {
  const { layerId, properties: p } = feature
  const ticker = p.ticker as string | null | undefined
  const tickerMeta = ticker ? getTicker(ticker) : undefined

  const { data: news = [] } = useQuery({
    queryKey: ['news', ticker],
    queryFn:  () => getNews({ ticker: ticker!, limit: 5 }),
    enabled:  !!ticker,
    staleTime: 120_000,
  })

  const isLive = layerId === 'flights-circles' || layerId === 'vessels-circles'

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-start justify-between p-4 border-b shrink-0"
        style={{ borderColor: 'var(--color-border)' }}>
        <div>
          <div className="text-xs font-medium uppercase tracking-wide mb-0.5"
            style={{ color: 'var(--color-muted)' }}>
            {layerId.replace('-circles','').replace('-fill','').replace('-hubs','').replace('-ring','')}
          </div>
          <div className="font-semibold text-sm leading-snug"
            style={{ color: 'var(--color-heading)' }}>
            {(p.name as string) || (p.callsign as string) || (p.mmsi as string) || '—'}
          </div>
        </div>
        <button onClick={onClose} className="p-1 rounded hover:bg-white/5 shrink-0 mt-0.5">
          <X size={14} style={{ color: 'var(--color-muted)' }} />
        </button>
      </div>

      {/* Body */}
      <div className="flex-1 overflow-y-auto p-4 space-y-3 text-xs"
        style={{ color: 'var(--color-text)' }}>

        {/* Static asset fields */}
        {!isLive && (
          <div className="space-y-2">
            {p.company    != null && <Row label="Company"    value={String(p.company)} />}
            {p.mineral    != null && <Row label="Mineral"    value={String(p.mineral).charAt(0).toUpperCase() + String(p.mineral).slice(1)} />}
            {p.type       != null && <Row label="Type"       value={String(p.type)} />}
            {p.region     != null && <Row label="Region"     value={String(p.region)} />}
            {p.status     != null && <Row label="Status"     value={String(p.status)} />}
            {p.production_tpa != null && <Row label="Production" value={`${Number(p.production_tpa).toLocaleString()} t/yr`} />}
            {p.capacity_bpd   != null && <Row label="Capacity"   value={`${Number(p.capacity_bpd).toLocaleString()} bpd`} />}
            {p.capacity_tpa   != null && <Row label="Capacity"   value={`${Number(p.capacity_tpa).toLocaleString()} t/yr`} />}
            {p.annual_tonnage_mt != null && <Row label="Tonnage" value={`${p.annual_tonnage_mt}M t/yr`} />}
            {p.length_km  != null && <Row label="Length"     value={`${p.length_km} km`} />}
            {p.description != null && (
              <div className="pt-1 leading-relaxed" style={{ color: 'var(--color-text)' }}>
                {String(p.description)}
              </div>
            )}
          </div>
        )}

        {/* Live flight fields */}
        {layerId === 'flights-circles' && (
          <div className="space-y-2">
            {p.callsign != null && <Row label="Callsign" value={String(p.callsign)} />}
            {p.alt      != null && <Row label="Altitude" value={`${Math.round(Number(p.alt))} m`} />}
            {p.speed    != null && <Row label="Speed"    value={`${Math.round(Number(p.speed))} m/s`} />}
            {p.heading  != null && <Row label="Heading"  value={`${Math.round(Number(p.heading))}°`} />}
            {/* Air Astana link */}
            {typeof p.callsign === 'string' && p.callsign.startsWith('KC') && (
              <div className="pt-1">
                <Link to="/ticker/AIRA"
                  className="inline-flex items-center gap-1 text-xs font-medium"
                  style={{ color: 'var(--color-accent)' }}>
                  <ExternalLink size={11} /> Air Astana (AIRA)
                </Link>
              </div>
            )}
          </div>
        )}

        {/* Vessel fields */}
        {layerId === 'vessels-circles' && (
          <div className="space-y-2">
            {p.mmsi    != null && <Row label="MMSI"    value={String(p.mmsi)} />}
            {p.type    != null && <Row label="Type"    value={String(p.type)} />}
            {p.speed   != null && <Row label="Speed"   value={`${p.speed} kn`} />}
            {p.heading != null && <Row label="Heading" value={`${p.heading}°`} />}
          </div>
        )}

        {/* Ticker link */}
        {tickerMeta && (
          <div className="pt-1 border-t" style={{ borderColor: 'var(--color-border)' }}>
            <Link to={`/ticker/${tickerMeta.symbol}`}
              className="inline-flex items-center gap-1.5 text-xs font-medium"
              style={{ color: 'var(--color-accent)' }}>
              <ExternalLink size={11} />
              {tickerMeta.name} ({tickerMeta.symbol})
            </Link>
          </div>
        )}

        {/* Related news */}
        {news.length > 0 && (
          <div className="pt-2 border-t space-y-2" style={{ borderColor: 'var(--color-border)' }}>
            <div className="text-xs font-medium" style={{ color: 'var(--color-muted)' }}>
              Recent news
            </div>
            {news.map(a => (
              <a key={a.article_id} href={a.url} target="_blank" rel="noopener noreferrer"
                className="block leading-snug hover:opacity-80 transition-opacity">
                <span className="inline-block mr-1.5 align-middle">
                  <ImpactDot impact={a.impact} />
                </span>
                <span style={{ color: 'var(--color-text)' }}>{a.title}</span>
              </a>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between gap-2">
      <span style={{ color: 'var(--color-muted)' }}>{label}</span>
      <span className="text-right font-medium" style={{ color: 'var(--color-heading)' }}>{value}</span>
    </div>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function MapPage() {
  const [activeLayers, setActiveLayers] = useState<LayerKey[]>(['mines', 'oil', 'rail', 'ports'])
  const [selected, setSelected] = useState<ClickedFeature | null>(null)
  const [panelOpen, setPanelOpen] = useState(true)

  const toggleLayer = (key: LayerKey) =>
    setActiveLayers(prev =>
      prev.includes(key) ? prev.filter(k => k !== key) : [...prev, key]
    )

  // ── Static GeoJSON (fetch once) ───────────────────────────────────────────
  const { data: mines }     = useQuery({ queryKey: ['geo','mines'],      queryFn: () => fetchGeoJson('mines'),      staleTime: Infinity })
  const { data: oilFields } = useQuery({ queryKey: ['geo','oil_fields'], queryFn: () => fetchGeoJson('oil_fields'), staleTime: Infinity })
  const { data: rail }      = useQuery({ queryKey: ['geo','rail'],       queryFn: () => fetchGeoJson('rail'),       staleTime: Infinity })
  const { data: ports }     = useQuery({ queryKey: ['geo','ports'],      queryFn: () => fetchGeoJson('ports'),      staleTime: Infinity })

  // ── Live data (poll only when layer is active) ────────────────────────────
  const { data: flights = [] } = useQuery({
    queryKey:      ['geo', 'flights'],
    queryFn:       getFlights,
    staleTime:     0,
    refetchInterval: 30_000,
    enabled:       activeLayers.includes('flights'),
  })
  const { data: vessels = [] } = useQuery({
    queryKey:      ['geo', 'vessels'],
    queryFn:       getVessels,
    staleTime:     0,
    refetchInterval: 60_000,
    enabled:       activeLayers.includes('vessels'),
  })

  return (
    <div className="relative h-full overflow-hidden">
      <MonitoriumMap
        activeLayers={activeLayers}
        mines={mines ?? null}
        oilFields={oilFields ?? null}
        rail={rail ?? null}
        ports={ports ?? null}
        flights={flights}
        vessels={vessels}
        onFeatureClick={setSelected}
      />

      {/* Layer toggle panel */}
      <div className="absolute top-4 left-4 z-10">
        <div className="rounded-xl border overflow-hidden"
          style={{
            background: 'color-mix(in srgb, var(--color-surface) 90%, transparent)',
            backdropFilter: 'blur(8px)',
            borderColor: 'var(--color-border)',
          }}>
          {/* Header */}
          <button
            onClick={() => setPanelOpen(v => !v)}
            className="flex items-center gap-2 px-3 py-2 w-full text-left"
            style={{ color: 'var(--color-heading)' }}>
            <Layers size={13} />
            <span className="text-xs font-semibold tracking-wide">Layers</span>
          </button>

          {/* Layer list */}
          {panelOpen && (
            <div className="border-t px-2 pb-2 pt-1 space-y-0.5"
              style={{ borderColor: 'var(--color-border)' }}>
              {LAYER_DEFS.map(({ key, label, color }) => {
                const active = activeLayers.includes(key)
                return (
                  <button
                    key={key}
                    onClick={() => toggleLayer(key)}
                    className="flex items-center gap-2 w-full text-left px-2 py-1.5 rounded transition-colors hover:bg-white/5"
                    style={{ color: active ? 'var(--color-heading)' : 'var(--color-muted)' }}>
                    <span className="w-2 h-2 rounded-full shrink-0 transition-colors"
                      style={{ background: active ? color : 'var(--color-border)' }} />
                    <span className="text-xs">{label}</span>
                    {(key === 'flights' || key === 'vessels') && active && (
                      <span className="ml-auto text-[10px] font-mono"
                        style={{ color: 'var(--color-muted)' }}>
                        {key === 'flights' ? `${flights.length}` : `${vessels.length}`}
                      </span>
                    )}
                  </button>
                )
              })}
            </div>
          )}
        </div>
      </div>

      {/* Info sidebar */}
      <AnimatePresence>
        {selected && (
          <motion.div
            initial={{ x: 320, opacity: 0 }}
            animate={{ x: 0, opacity: 1 }}
            exit={{ x: 320, opacity: 0 }}
            transition={{ type: 'spring', damping: 25, stiffness: 200 }}
            className="absolute top-0 right-0 h-full w-72 border-l z-10"
            style={{ background: 'var(--color-surface)', borderColor: 'var(--color-border)' }}>
            <InfoSidebar feature={selected} onClose={() => setSelected(null)} />
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}
