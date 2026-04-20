import { useEffect, useRef, useState } from 'react'
import maplibregl, { GeoJSONSource } from 'maplibre-gl'
import type { Flight } from '../api/geo'

export type LayerKey = 'mines' | 'oil' | 'rail' | 'ports' | 'flights' | 'vessels'

export interface ClickedFeature {
  layerId: string
  properties: Record<string, unknown>
}

interface Props {
  activeLayers: LayerKey[]
  mines:     GeoJSON.FeatureCollection | null
  oilFields: GeoJSON.FeatureCollection | null
  rail:      GeoJSON.FeatureCollection | null
  ports:     GeoJSON.FeatureCollection | null
  vessels:   GeoJSON.FeatureCollection | null
  flights:   Flight[]
  onFeatureClick: (f: ClickedFeature) => void
}

const STYLE_URL = 'https://tiles.openfreemap.org/styles/liberty'

const LAYER_GROUPS: Record<LayerKey, string[]> = {
  mines:   ['mines-symbols', 'mines-labels'],
  oil:     ['oil-symbols', 'oil-labels'],
  rail:    ['rail-lines', 'rail-hubs'],
  ports:   ['ports-symbols', 'ports-labels'],
  flights: ['flights-symbols'],
  vessels: ['vessels-symbols', 'vessels-labels'],
}

const CLICKABLE_LAYERS = [
  'mines-symbols', 'oil-symbols', 'rail-hubs',
  'ports-symbols', 'flights-symbols', 'vessels-symbols',
]

// ── Canvas image helpers ───────────────────────────────────────────────────────

/** Emoji centred on a solid coloured circle. Use pixelRatio:2 when adding to map. */
function makeEmojiDot(emoji: string, bg: string, size = 64): ImageData {
  const c = document.createElement('canvas')
  c.width = size; c.height = size
  const ctx = c.getContext('2d')!
  const r = size / 2
  ctx.beginPath()
  ctx.arc(r, r, r - 2, 0, Math.PI * 2)
  ctx.fillStyle = bg
  ctx.fill()
  ctx.strokeStyle = 'rgba(8,11,18,0.65)'
  ctx.lineWidth = 2.5
  ctx.stroke()
  ctx.font = `${Math.round(size * 0.50)}px sans-serif`
  ctx.textAlign = 'center'
  ctx.textBaseline = 'middle'
  ctx.fillText(emoji, r, r + 1)
  return ctx.getImageData(0, 0, size, size)
}

/** Pointed triangle for vessels — bow at top. 48 px canvas → 24 px CSS at pixelRatio 2. */
function makeVesselTriangle(color: string, size = 48): ImageData {
  const c = document.createElement('canvas')
  c.width = size; c.height = size
  const ctx = c.getContext('2d')!
  const cx = size / 2
  const s  = size
  ctx.fillStyle   = color
  ctx.strokeStyle = 'rgba(8,11,18,0.85)'
  ctx.lineWidth   = s * 0.06
  ctx.lineJoin    = 'round'
  ctx.beginPath()
  ctx.moveTo(cx,          s * 0.06)  // bow (point at top)
  ctx.lineTo(s * 0.93,    s * 0.88)  // starboard quarter
  ctx.lineTo(cx,          s * 0.70)  // stern notch (gives ship-like silhouette)
  ctx.lineTo(s * 0.07,    s * 0.88)  // port quarter
  ctx.closePath()
  ctx.fill()
  ctx.stroke()
  return ctx.getImageData(0, 0, size, size)
}

/** Top-down commercial airliner silhouette, nose pointing UP (heading 0° = north).
 *  Rotate with icon-rotate. 64 px canvas → 32 px CSS at pixelRatio 2. */
function makePlaneIcon(color: string, size = 64): ImageData {
  const c = document.createElement('canvas')
  c.width = size; c.height = size
  const ctx = c.getContext('2d')!
  const cx = size / 2
  const s  = size

  ctx.fillStyle   = color
  ctx.strokeStyle = 'rgba(8,11,18,0.88)'
  ctx.lineWidth   = s * 0.04
  ctx.lineJoin    = 'round'
  ctx.lineCap     = 'round'

  // ── Fuselage — tapering cigar, nose at top ────────────────────────────────
  ctx.beginPath()
  ctx.moveTo(cx, s * 0.04)
  ctx.bezierCurveTo(cx + s*0.07, s*0.12, cx + s*0.07, s*0.26, cx + s*0.055, s*0.38)
  ctx.bezierCurveTo(cx + s*0.05, s*0.55, cx + s*0.04, s*0.72, cx, s * 0.96)
  ctx.bezierCurveTo(cx - s*0.04, s*0.72, cx - s*0.05, s*0.55, cx - s*0.055, s*0.38)
  ctx.bezierCurveTo(cx - s*0.07, s*0.26, cx - s*0.07, s*0.12, cx, s * 0.04)
  ctx.closePath()
  ctx.fill()
  ctx.stroke()

  // ── Right main wing — swept back ──────────────────────────────────────────
  ctx.beginPath()
  ctx.moveTo(cx + s*0.055, s*0.30)
  ctx.lineTo(s  * 0.97,    s*0.60)
  ctx.lineTo(s  * 0.83,    s*0.66)
  ctx.lineTo(cx + s*0.055, s*0.46)
  ctx.closePath()
  ctx.fill()
  ctx.stroke()

  // ── Left main wing ────────────────────────────────────────────────────────
  ctx.beginPath()
  ctx.moveTo(cx - s*0.055, s*0.30)
  ctx.lineTo(s  * 0.03,    s*0.60)
  ctx.lineTo(s  * 0.17,    s*0.66)
  ctx.lineTo(cx - s*0.055, s*0.46)
  ctx.closePath()
  ctx.fill()
  ctx.stroke()

  // ── Right horizontal stabilizer ───────────────────────────────────────────
  ctx.beginPath()
  ctx.moveTo(cx + s*0.04, s*0.78)
  ctx.lineTo(s  * 0.72,   s*0.89)
  ctx.lineTo(s  * 0.65,   s*0.95)
  ctx.lineTo(cx + s*0.04, s*0.91)
  ctx.closePath()
  ctx.fill()
  ctx.stroke()

  // ── Left horizontal stabilizer ────────────────────────────────────────────
  ctx.beginPath()
  ctx.moveTo(cx - s*0.04, s*0.78)
  ctx.lineTo(s  * 0.28,   s*0.89)
  ctx.lineTo(s  * 0.35,   s*0.95)
  ctx.lineTo(cx - s*0.04, s*0.91)
  ctx.closePath()
  ctx.fill()
  ctx.stroke()

  return ctx.getImageData(0, 0, size, size)
}

// ── Flight → GeoJSON ───────────────────────────────────────────────────────────

function flightsToGeoJSON(flights: Flight[]): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: flights.map((f): GeoJSON.Feature => ({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [f.lon, f.lat] },
      properties: { ...f },
    })),
  }
}

// ── Component ──────────────────────────────────────────────────────────────────

export default function MonitoriumMap({
  activeLayers, mines, oilFields, rail, ports, vessels, flights, onFeatureClick,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef       = useRef<maplibregl.Map | null>(null)
  const [mapReady, setMapReady] = useState(false)

  useEffect(() => {
    if (!containerRef.current) return

    const map = new maplibregl.Map({
      container: containerRef.current,
      style:     STYLE_URL,
      center:    [67.0, 48.0],
      zoom:      4.5,
      attributionControl: false,
    })

    map.addControl(new maplibregl.NavigationControl(), 'bottom-right')
    map.addControl(new maplibregl.AttributionControl({ compact: true }), 'bottom-left')

    map.on('load', () => {
      // ── Sprite images ─────────────────────────────────────────────────────
      const PR = 2
      map.addImage('mine-uranium', makeEmojiDot('☢️', '#22c55e'), { pixelRatio: PR })
      map.addImage('mine-gold',    makeEmojiDot('🥇', '#f59e0b'), { pixelRatio: PR })
      map.addImage('mine-copper',  makeEmojiDot('🔶', '#f97316'), { pixelRatio: PR })
      map.addImage('mine-coal',    makeEmojiDot('⛏️', '#94a3b8'), { pixelRatio: PR })
      map.addImage('oil-field',    makeEmojiDot('🛢️', '#ef4444'), { pixelRatio: PR })
      map.addImage('port-sea',     makeEmojiDot('⚓', '#8b9ab0'), { pixelRatio: PR })
      map.addImage('port-border',  makeEmojiDot('🚧', '#e5b800'), { pixelRatio: PR })
      map.addImage('port-dry',     makeEmojiDot('📦', '#6b7280'), { pixelRatio: PR })
      map.addImage('rail-hub',     makeEmojiDot('🚉', '#3d4a5c'), { pixelRatio: PR })
      map.addImage('vessel-shape',  makeVesselTriangle('#0ea5e9'),   { pixelRatio: PR })
      map.addImage('plane-icon',    makePlaneIcon('#f59e0b'),        { pixelRatio: PR })

      // ── Sources ───────────────────────────────────────────────────────────
      const empty: GeoJSON.FeatureCollection = { type: 'FeatureCollection', features: [] }
      for (const id of ['mines','oil-fields','rail','ports','vessels','flights']) {
        map.addSource(id, { type: 'geojson', data: empty })
      }

      // ── Mines — sized by production_tpa ───────────────────────────────────
      map.addLayer({
        id: 'mines-symbols', type: 'symbol', source: 'mines',
        layout: {
          'icon-image': ['match', ['get', 'mineral'],
            'uranium', 'mine-uranium', 'gold', 'mine-gold',
            'copper',  'mine-copper',  'coal', 'mine-coal',
            'mine-uranium',
          ],
          'icon-size': ['step', ['coalesce', ['get', 'production_tpa'], 1000],
            0.55, 4000, 0.72, 15000, 0.90, 1000000, 1.15],
          'icon-allow-overlap': true, 'icon-ignore-placement': true,
        },
      })
      map.addLayer({
        id: 'mines-labels', type: 'symbol', source: 'mines', minzoom: 6,
        layout: { 'text-field': ['get','name'], 'text-size': 11,
                  'text-offset': [0,1.6], 'text-anchor': 'top' },
        paint: { 'text-color': '#e8edf5', 'text-halo-color': '#080b12', 'text-halo-width': 1 },
      })

      // ── Oil fields — sized by capacity_bpd ───────────────────────────────
      map.addLayer({
        id: 'oil-symbols', type: 'symbol', source: 'oil-fields',
        layout: {
          'icon-image': 'oil-field',
          'icon-size': ['step', ['coalesce', ['get', 'capacity_bpd'], 80000],
            0.55, 150000, 0.75, 450000, 0.98],
          'icon-allow-overlap': true, 'icon-ignore-placement': true,
        },
      })
      map.addLayer({
        id: 'oil-labels', type: 'symbol', source: 'oil-fields', minzoom: 5,
        layout: { 'text-field': ['get','name'], 'text-size': 11,
                  'text-offset': [0,1.6], 'text-anchor': 'top' },
        paint: { 'text-color': '#e8edf5', 'text-halo-color': '#080b12', 'text-halo-width': 1 },
      })

      // ── Rail ──────────────────────────────────────────────────────────────
      map.addLayer({
        id: 'rail-lines', type: 'line', source: 'rail',
        filter: ['==', ['geometry-type'], 'LineString'],
        paint: { 'line-color': '#3d4a5c',
                 'line-width': ['interpolate',['linear'],['zoom'],3,1,8,2.5] },
      })
      map.addLayer({
        id: 'rail-hubs', type: 'symbol', source: 'rail',
        filter: ['==', ['geometry-type'], 'Point'],
        layout: {
          'icon-image': 'rail-hub',
          'icon-size': ['step', ['coalesce', ['get', 'annual_tonnage_mt'], 10],
            0.42, 11, 0.55, 21, 0.70],
          'icon-allow-overlap': true, 'icon-ignore-placement': true,
        },
      })

      // ── Ports — sized by capacity_tpa ─────────────────────────────────────
      map.addLayer({
        id: 'ports-symbols', type: 'symbol', source: 'ports',
        layout: {
          'icon-image': ['match', ['get','type'],
            'seaport','port-sea', 'border_crossing','port-border',
            'dry_port','port-dry', 'port-sea'],
          'icon-size': ['step', ['coalesce', ['get','capacity_tpa'], 5000000],
            0.55, 8000000, 0.75, 18000000, 0.95],
          'icon-allow-overlap': true, 'icon-ignore-placement': true,
        },
      })
      map.addLayer({
        id: 'ports-labels', type: 'symbol', source: 'ports', minzoom: 5,
        layout: { 'text-field': ['get','name'], 'text-size': 11,
                  'text-offset': [0,1.6], 'text-anchor': 'top' },
        paint: { 'text-color': '#e8edf5', 'text-halo-color': '#080b12', 'text-halo-width': 1 },
      })

      // ── Caspian vessels ───────────────────────────────────────────────────
      map.addLayer({
        id: 'vessels-symbols', type: 'symbol', source: 'vessels',
        layout: {
          'icon-image': 'vessel-shape',
          'icon-size': 0.85,
          'icon-allow-overlap': true, 'icon-ignore-placement': true,
        },
      })
      map.addLayer({
        id: 'vessels-labels', type: 'symbol', source: 'vessels', minzoom: 5,
        layout: { 'text-field': ['get','name'], 'text-size': 10,
                  'text-offset': [0,1.4], 'text-anchor': 'top' },
        paint: { 'text-color': '#e8edf5', 'text-halo-color': '#080b12', 'text-halo-width': 1 },
      })

      // ── Flights — rotated aircraft silhouette ─────────────────────────────
      map.addLayer({
        id: 'flights-symbols', type: 'symbol', source: 'flights',
        layout: {
          'icon-image': 'plane-icon',
          'icon-size': 0.82,
          'icon-rotate': ['coalesce', ['get','heading'], 0],
          'icon-rotation-alignment': 'map',
          'icon-allow-overlap': true, 'icon-ignore-placement': true,
        },
      })

      // ── Click & cursor ────────────────────────────────────────────────────
      map.on('click', e => {
        const features = map.queryRenderedFeatures(e.point, { layers: CLICKABLE_LAYERS })
        if (features.length) {
          onFeatureClick({
            layerId:    features[0].layer.id,
            properties: features[0].properties as Record<string, unknown>,
          })
        }
      })
      CLICKABLE_LAYERS.forEach(id => {
        map.on('mouseenter', id, () => { map.getCanvas().style.cursor = 'pointer' })
        map.on('mouseleave', id, () => { map.getCanvas().style.cursor = '' })
      })

      setMapReady(true)
    })

    mapRef.current = map
    return () => { map.remove() }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Layer visibility ───────────────────────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    const map = mapRef.current
    Object.entries(LAYER_GROUPS).forEach(([key, ids]) => {
      const vis = activeLayers.includes(key as LayerKey) ? 'visible' : 'none'
      ids.forEach(id => { if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', vis) })
    })
  }, [mapReady, activeLayers])

  // ── Data updates ───────────────────────────────────────────────────────────
  useEffect(() => { if (mapReady && mines)     (mapRef.current!.getSource('mines')      as GeoJSONSource).setData(mines)     }, [mapReady, mines])
  useEffect(() => { if (mapReady && oilFields) (mapRef.current!.getSource('oil-fields') as GeoJSONSource).setData(oilFields) }, [mapReady, oilFields])
  useEffect(() => { if (mapReady && rail)      (mapRef.current!.getSource('rail')       as GeoJSONSource).setData(rail)      }, [mapReady, rail])
  useEffect(() => { if (mapReady && ports)     (mapRef.current!.getSource('ports')      as GeoJSONSource).setData(ports)     }, [mapReady, ports])
  useEffect(() => { if (mapReady && vessels)   (mapRef.current!.getSource('vessels')    as GeoJSONSource).setData(vessels)   }, [mapReady, vessels])
  useEffect(() => {
    if (!mapReady) return
    ;(mapRef.current!.getSource('flights') as GeoJSONSource).setData(flightsToGeoJSON(flights))
  }, [mapReady, flights])

  return <div ref={containerRef} className="w-full h-full" />
}
