import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { AnimatePresence, motion } from 'framer-motion'
import { X, ExternalLink, Layers, TrendingUp, TrendingDown, Minus } from 'lucide-react'
import MonitoriumMap, { type LayerKey, type ClickedFeature } from '../../components/MonitoriumMap'
import { fetchGeoJson, getFlights } from '../../api/geo'
import { getNews } from '../../api/news'
import { getTicker } from '../../config'
import { ImpactDot } from '../../components/ui'

// ── Layer definitions ─────────────────────────────────────────────────────────

const LAYER_DEFS: { key: LayerKey; label: string; color: string }[] = [
  { key: 'mines',   label: 'Mines',          color: '#22c55e' },
  { key: 'oil',     label: 'Oil & Gas',      color: '#ef4444' },
  { key: 'rail',    label: 'Rail',           color: '#3d4a5c' },
  { key: 'ports',   label: 'Ports',          color: '#8b9ab0' },
  { key: 'vessels', label: 'Caspian Fleet',  color: '#0ea5e9' },
  { key: 'flights', label: 'Live Flights',   color: '#f59e0b' },
]

// ── Airline lookup (ICAO 3-letter tried first, then IATA 2-letter) ────────────

const AIRLINE_CODES: Record<string, string> = {
  // Central Asian
  KZR: 'Air Astana',       KC: 'Air Astana',
  SQS: 'SCAT Airlines',    DV: 'SCAT Airlines',
  // Russian
  AFL: 'Aeroflot',         SU: 'Aeroflot',
  SVR: 'Ural Airlines',    U6: 'Ural Airlines',
  UTA: 'UTair',            UT: 'UTair',
  SDM: 'S7 Airlines',      S7: 'S7 Airlines',
  NWS: 'Nordwind Airlines',
  // Middle East
  UAE: 'Emirates',         EK: 'Emirates',
  QTR: 'Qatar Airways',    QR: 'Qatar Airways',
  FDB: 'Flydubai',         FZ: 'Flydubai',
  GFA: 'Gulf Air',         GF: 'Gulf Air',
  SVA: 'Saudia',           SV: 'Saudia',
  // European
  THY: 'Turkish Airlines', TK: 'Turkish Airlines',
  DLH: 'Lufthansa',        LH: 'Lufthansa',
  BAW: 'British Airways',  BA: 'British Airways',
  AFR: 'Air France',       AF: 'Air France',
  AUA: 'Austrian Airlines',OS: 'Austrian Airlines',
  SWR: 'SWISS',            LX: 'SWISS',
  FIN: 'Finnair',          AY: 'Finnair',
  // Asian
  CES: 'China Eastern',    MU: 'China Eastern',
  CSN: 'China Southern',   CZ: 'China Southern',
  CCA: 'Air China',        CA: 'Air China',
  KAL: 'Korean Air',       KE: 'Korean Air',
  AAR: 'Asiana Airlines',  OZ: 'Asiana Airlines',
  ANA: 'ANA',              NH: 'ANA',
  JAL: 'Japan Airlines',   JL: 'Japan Airlines',
  SIA: 'Singapore Airlines',SQ: 'Singapore Airlines',
  AIC: 'Air India',        AI: 'Air India',
  THA: 'Thai Airways',     TG: 'Thai Airways',
}

function getAirline(callsign: string): string | null {
  const cs = callsign.replace(/\s+/g, '').toUpperCase()
  return AIRLINE_CODES[cs.slice(0, 3)] ?? AIRLINE_CODES[cs.slice(0, 2)] ?? null
}

function fmtSeenPos(sec: number | null): string | null {
  if (sec == null) return null
  if (sec < 15)  return 'just now'
  if (sec < 90)  return `${Math.round(sec)}s ago`
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`
  return `> 1h (stale)`
}

// ── Info sidebar ──────────────────────────────────────────────────────────────

function InfoSidebar({ feature, onClose }: { feature: ClickedFeature; onClose: () => void }) {
  const { layerId, properties: p } = feature
  const ticker     = p.ticker as string | null | undefined
  const tickerMeta = ticker ? getTicker(ticker) : undefined

  const { data: news = [] } = useQuery({
    queryKey:  ['news', ticker],
    queryFn:   () => getNews({ ticker: ticker!, limit: 5 }),
    enabled:   !!ticker,
    staleTime: 120_000,
  })

  const isFlight  = layerId === 'flights-symbols'
  const isVessel  = layerId === 'vessels-symbols'
  const isLive    = isFlight
  const layerName = layerId
    .replace('-symbols', '').replace('-circles', '').replace('-fill', '')
    .replace('-hubs', '').replace('-ring', '')

  // Flight-specific
  const callsign  = (p.callsign as string | null) ?? ''
  const airline   = isFlight ? getAirline(callsign) : null
  const vertRate  = p.vert_rate as number | null | undefined
  const seenPos   = p.seen_pos as number | null | undefined
  const squawk    = p.squawk as string | null | undefined
  const isEmergency = squawk === '7500' || squawk === '7600' || squawk === '7700'

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-start justify-between p-4 border-b shrink-0"
        style={{ borderColor: 'var(--color-border)' }}>
        <div>
          <div className="text-xs font-medium uppercase tracking-wide mb-0.5"
            style={{ color: 'var(--color-muted)' }}>
            {layerName}
            {isFlight && (
              <span className="ml-1.5 normal-case font-normal">
                {seenPos != null && seenPos < 60
                  ? <span className="text-green-500">● live</span>
                  : <span style={{ color: 'var(--color-muted)' }}>○ {fmtSeenPos(seenPos as number | null)}</span>
                }
              </span>
            )}
          </div>
          <div className="font-semibold text-sm leading-snug"
            style={{ color: 'var(--color-heading)' }}>
            {isFlight
              ? (callsign || (p.icao as string) || '—')
              : ((p.name as string) || (p.mmsi as string) || '—')}
          </div>
          {isFlight && airline && (
            <div className="text-xs mt-0.5" style={{ color: 'var(--color-muted)' }}>{airline}</div>
          )}
          {isVessel && p.company != null && (
            <div className="text-xs mt-0.5" style={{ color: 'var(--color-muted)' }}>{String(p.company)}</div>
          )}
        </div>
        <button onClick={onClose} className="p-1 rounded hover:bg-white/5 shrink-0 mt-0.5">
          <X size={14} style={{ color: 'var(--color-muted)' }} />
        </button>
      </div>

      {/* Body */}
      <div className="flex-1 overflow-y-auto p-4 space-y-3 text-xs"
        style={{ color: 'var(--color-text)' }}>

        {/* Static asset fields */}
        {!isLive && !isVessel && (
          <div className="space-y-2">
            {p.company    != null && <Row label="Company"    value={String(p.company)} />}
            {p.mineral    != null && <Row label="Mineral"    value={String(p.mineral).charAt(0).toUpperCase() + String(p.mineral).slice(1)} />}
            {p.type       != null && <Row label="Type"       value={String(p.type).replace(/_/g,' ')} />}
            {p.region     != null && <Row label="Region"     value={String(p.region)} />}
            {p.status     != null && <Row label="Status"     value={String(p.status)} />}
            {p.production_tpa  != null && <Row label="Production" value={`${Number(p.production_tpa).toLocaleString()} t/yr`} />}
            {p.capacity_bpd    != null && <Row label="Capacity"   value={`${Number(p.capacity_bpd).toLocaleString()} bpd`} />}
            {p.capacity_tpa    != null && <Row label="Capacity"   value={`${(Number(p.capacity_tpa)/1e6).toFixed(1)}M t/yr`} />}
            {p.annual_tonnage_mt != null && <Row label="Tonnage"  value={`${p.annual_tonnage_mt}M t/yr`} />}
            {p.length_km  != null && <Row label="Length"     value={`${p.length_km} km`} />}
            {p.description != null && (
              <div className="pt-1 leading-relaxed" style={{ color: 'var(--color-text)' }}>
                {String(p.description)}
              </div>
            )}
          </div>
        )}

        {/* Flight fields */}
        {isFlight && (
          <div className="space-y-2">
            {p.aircraft != null && <Row label="Aircraft" value={String(p.aircraft)} />}
            {p.type_code != null && <Row label="Type code" value={String(p.type_code)} />}
            {p.reg != null && <Row label="Registration" value={String(p.reg)} />}

            {p.alt != null && (
              <div className="flex justify-between gap-2">
                <span style={{ color: 'var(--color-muted)' }}>Altitude</span>
                <span className="text-right font-medium flex items-center gap-1" style={{ color: 'var(--color-heading)' }}>
                  {Number(p.alt).toLocaleString()} m
                  {vertRate != null && Math.abs(vertRate) > 200 && (
                    vertRate > 0
                      ? <TrendingUp size={11} className="text-green-500" />
                      : <TrendingDown size={11} className="text-red-400" />
                  )}
                  {(vertRate == null || Math.abs(vertRate) <= 200) && (
                    <Minus size={11} style={{ color: 'var(--color-muted)' }} />
                  )}
                </span>
              </div>
            )}

            {vertRate != null && Math.abs(vertRate) > 200 && (
              <Row
                label="Vert. rate"
                value={`${vertRate > 0 ? '+' : ''}${Math.round(vertRate)} ft/min`}
              />
            )}

            {p.speed    != null && <Row label="Speed"   value={`${Math.round(Number(p.speed))} kn`} />}
            {p.heading  != null && <Row label="Heading" value={`${Math.round(Number(p.heading))}°`} />}

            {squawk && (
              <Row
                label="Squawk"
                value={isEmergency ? `⚠️ ${squawk} EMERGENCY` : squawk}
              />
            )}

            {callsign.startsWith('KC') && (
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
        {isVessel && (
          <div className="space-y-2">
            {p.type != null && <Row label="Type" value={String(p.type).replace(/_/g,' ')} />}
            {p.cargo != null && <Row label="Cargo" value={String(p.cargo)} />}
            {p.capacity_dwt != null && <Row label="Capacity" value={`${Number(p.capacity_dwt).toLocaleString()} DWT`} />}
            {p.flag != null && <Row label="Flag" value={String(p.flag)} />}
            {p.imo != null && <Row label="IMO" value={String(p.imo)} />}
            {(p.from != null || p.to != null) && (
              <Row label="Route" value={`${p.from ?? '?'} → ${p.to ?? '?'}`} />
            )}
            {p.route_note != null && (
              <div className="pt-1 leading-relaxed" style={{ color: 'var(--color-muted)' }}>
                {String(p.route_note)}
              </div>
            )}
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
    setActiveLayers(prev => prev.includes(key) ? prev.filter(k => k !== key) : [...prev, key])

  const { data: mines }     = useQuery({ queryKey: ['geo','mines'],      queryFn: () => fetchGeoJson('mines'),      staleTime: Infinity })
  const { data: oilFields } = useQuery({ queryKey: ['geo','oil_fields'], queryFn: () => fetchGeoJson('oil_fields'), staleTime: Infinity })
  const { data: rail }      = useQuery({ queryKey: ['geo','rail'],       queryFn: () => fetchGeoJson('rail'),       staleTime: Infinity })
  const { data: ports }     = useQuery({ queryKey: ['geo','ports'],      queryFn: () => fetchGeoJson('ports'),      staleTime: Infinity })
  const { data: vessels }   = useQuery({ queryKey: ['geo','vessels'],    queryFn: () => fetchGeoJson('vessels'),    staleTime: Infinity })

  const { data: flights = [] } = useQuery({
    queryKey:        ['geo','flights'],
    queryFn:         getFlights,
    staleTime:       0,
    refetchInterval: 30_000,
    enabled:         activeLayers.includes('flights'),
  })

  return (
    <div className="relative h-full overflow-hidden">
      <MonitoriumMap
        activeLayers={activeLayers}
        mines={mines ?? null}
        oilFields={oilFields ?? null}
        rail={rail ?? null}
        ports={ports ?? null}
        vessels={vessels ?? null}
        flights={flights}
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
          <button
            onClick={() => setPanelOpen(v => !v)}
            className="flex items-center gap-2 px-3 py-2 w-full text-left"
            style={{ color: 'var(--color-heading)' }}>
            <Layers size={13} />
            <span className="text-xs font-semibold tracking-wide">Layers</span>
          </button>

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
                    {key === 'flights' && active && (
                      <span className="ml-auto text-[10px] font-mono"
                        style={{ color: 'var(--color-muted)' }}>
                        {flights.length}
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
