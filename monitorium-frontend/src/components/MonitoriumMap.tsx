import { useEffect, useRef, useState } from 'react'
import maplibregl, { GeoJSONSource } from 'maplibre-gl'
import type { Flight, Vessel } from '../api/geo'

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
  flights:   Flight[]
  vessels:   Vessel[]
  onFeatureClick: (f: ClickedFeature) => void
}

const STYLE_URL = 'https://tiles.openfreemap.org/styles/liberty'

// Map of LayerKey → MapLibre layer IDs within that group
const LAYER_GROUPS: Record<LayerKey, string[]> = {
  mines:   ['mines-circles', 'mines-labels'],
  oil:     ['oil-ring', 'oil-fill', 'oil-labels'],
  rail:    ['rail-lines', 'rail-hubs'],
  ports:   ['ports-circles', 'ports-labels'],
  flights: ['flights-circles'],
  vessels: ['vessels-circles'],
}

const CLICKABLE_LAYERS = [
  'mines-circles', 'oil-fill', 'rail-hubs',
  'ports-circles', 'flights-circles', 'vessels-circles',
]

function toGeoJSON(items: Flight[] | Vessel[], type: 'flight' | 'vessel'): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: items.map((item): GeoJSON.Feature => ({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [(item as any).lon, (item as any).lat] },
      properties: { ...item, _type: type },
    })),
  }
}

export default function MonitoriumMap({
  activeLayers, mines, oilFields, rail, ports, flights, vessels, onFeatureClick,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef       = useRef<maplibregl.Map | null>(null)
  const [mapReady, setMapReady] = useState(false)

  // ── Init map (runs once) ───────────────────────────────────────────────────
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
      // ── Register all sources with empty data ──────────────────────────────
      const empty: GeoJSON.FeatureCollection = { type: 'FeatureCollection', features: [] }
      for (const id of ['mines', 'oil-fields', 'rail', 'ports', 'flights', 'vessels']) {
        map.addSource(id, { type: 'geojson', data: empty })
      }

      // ── Mines layer ───────────────────────────────────────────────────────
      map.addLayer({
        id: 'mines-circles', type: 'circle', source: 'mines',
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 3, 4, 8, 9],
          'circle-color': ['match', ['get', 'mineral'],
            'uranium', '#22c55e',
            'gold',    '#f59e0b',
            'copper',  '#f97316',
            'coal',    '#94a3b8',
            '#6b7280',
          ],
          'circle-stroke-width': 1.5,
          'circle-stroke-color': '#080b12',
          'circle-opacity': 0.9,
        },
      })
      map.addLayer({
        id: 'mines-labels', type: 'symbol', source: 'mines',
        minzoom: 6,
        layout: {
          'text-field': ['get', 'name'],
          'text-size': 11,
          'text-offset': [0, 1.2],
          'text-anchor': 'top',
        },
        paint: { 'text-color': '#e8edf5', 'text-halo-color': '#080b12', 'text-halo-width': 1 },
      })

      // ── Oil fields layer ──────────────────────────────────────────────────
      map.addLayer({
        id: 'oil-ring', type: 'circle', source: 'oil-fields',
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 3, 8, 8, 14],
          'circle-color': 'transparent',
          'circle-stroke-width': 2,
          'circle-stroke-color': '#ef4444',
          'circle-opacity': 0.8,
        },
      })
      map.addLayer({
        id: 'oil-fill', type: 'circle', source: 'oil-fields',
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 3, 4, 8, 7],
          'circle-color': '#ef4444',
          'circle-opacity': 0.7,
          'circle-stroke-width': 1,
          'circle-stroke-color': '#080b12',
        },
      })
      map.addLayer({
        id: 'oil-labels', type: 'symbol', source: 'oil-fields',
        minzoom: 5,
        layout: {
          'text-field': ['get', 'name'],
          'text-size': 11,
          'text-offset': [0, 1.4],
          'text-anchor': 'top',
        },
        paint: { 'text-color': '#e8edf5', 'text-halo-color': '#080b12', 'text-halo-width': 1 },
      })

      // ── Rail layer ────────────────────────────────────────────────────────
      map.addLayer({
        id: 'rail-lines', type: 'line', source: 'rail',
        filter: ['==', ['geometry-type'], 'LineString'],
        paint: {
          'line-color': '#3d4a5c',
          'line-width': ['interpolate', ['linear'], ['zoom'], 3, 1, 8, 2.5],
          'line-dasharray': ['case',
            ['==', ['get', 'passenger'], false],
            ['literal', [4, 2]],
            ['literal', [1, 0]],
          ],
        },
      })
      map.addLayer({
        id: 'rail-hubs', type: 'circle', source: 'rail',
        filter: ['==', ['geometry-type'], 'Point'],
        paint: {
          'circle-radius': 5,
          'circle-color': '#3d4a5c',
          'circle-stroke-width': 1.5,
          'circle-stroke-color': '#8b9ab0',
        },
      })

      // ── Ports layer ───────────────────────────────────────────────────────
      map.addLayer({
        id: 'ports-circles', type: 'circle', source: 'ports',
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 3, 5, 8, 10],
          'circle-color': '#8b9ab0',
          'circle-stroke-width': 2,
          'circle-stroke-color': '#e8edf5',
        },
      })
      map.addLayer({
        id: 'ports-labels', type: 'symbol', source: 'ports',
        minzoom: 5,
        layout: {
          'text-field': ['get', 'name'],
          'text-size': 11,
          'text-offset': [0, 1.2],
          'text-anchor': 'top',
        },
        paint: { 'text-color': '#e8edf5', 'text-halo-color': '#080b12', 'text-halo-width': 1 },
      })

      // ── Flights layer ─────────────────────────────────────────────────────
      map.addLayer({
        id: 'flights-circles', type: 'circle', source: 'flights',
        paint: {
          'circle-radius': 4,
          'circle-color': '#f59e0b',
          'circle-stroke-width': 1,
          'circle-stroke-color': '#080b12',
          'circle-opacity': 0.9,
        },
      })

      // ── Vessels layer ─────────────────────────────────────────────────────
      map.addLayer({
        id: 'vessels-circles', type: 'circle', source: 'vessels',
        paint: {
          'circle-radius': 5,
          'circle-color': ['match', ['get', 'type'],
            'tanker', '#ef4444',
            '#3b82f6',
          ],
          'circle-stroke-width': 1.5,
          'circle-stroke-color': '#e8edf5',
          'circle-opacity': 0.9,
        },
      })

      // ── Click handler ─────────────────────────────────────────────────────
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

  // ── Update layer visibility ────────────────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    const map = mapRef.current
    Object.entries(LAYER_GROUPS).forEach(([key, layerIds]) => {
      const vis = activeLayers.includes(key as LayerKey) ? 'visible' : 'none'
      layerIds.forEach(id => {
        if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', vis)
      })
    })
  }, [mapReady, activeLayers])

  // ── Update data sources ────────────────────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mines) return
    ;(mapRef.current!.getSource('mines') as GeoJSONSource).setData(mines)
  }, [mapReady, mines])

  useEffect(() => {
    if (!mapReady || !oilFields) return
    ;(mapRef.current!.getSource('oil-fields') as GeoJSONSource).setData(oilFields)
  }, [mapReady, oilFields])

  useEffect(() => {
    if (!mapReady || !rail) return
    ;(mapRef.current!.getSource('rail') as GeoJSONSource).setData(rail)
  }, [mapReady, rail])

  useEffect(() => {
    if (!mapReady || !ports) return
    ;(mapRef.current!.getSource('ports') as GeoJSONSource).setData(ports)
  }, [mapReady, ports])

  useEffect(() => {
    if (!mapReady) return
    ;(mapRef.current!.getSource('flights') as GeoJSONSource).setData(toGeoJSON(flights, 'flight'))
  }, [mapReady, flights])

  useEffect(() => {
    if (!mapReady) return
    ;(mapRef.current!.getSource('vessels') as GeoJSONSource).setData(toGeoJSON(vessels, 'vessel'))
  }, [mapReady, vessels])

  return <div ref={containerRef} className="w-full h-full" />
}
