import get from './client'

export interface Flight {
  icao:      string
  callsign:  string
  lat:       number
  lon:       number
  alt:       number | null   // metres (converted from feet)
  speed:     number | null   // knots
  heading:   number | null   // degrees true
  reg:       string | null   // tail number, e.g. "UP-B5706"
  aircraft:  string | null   // e.g. "AIRBUS A-350-900"
  type_code: string | null   // ICAO type designator, e.g. "A359"
  vert_rate: number | null   // ft/min, positive = climbing
  seen_pos:  number | null   // seconds since last position fix
  squawk:    string | null
}

export interface Vessel {
  mmsi:    string
  name:    string
  lat:     number
  lon:     number
  type:    string
  speed:   number | null
  heading: number | null
}

export const fetchGeoJson = (name: string): Promise<GeoJSON.FeatureCollection> =>
  fetch(`/geo/${name}.geojson`).then(r => {
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
    return r.json()
  })

// ── Flights ───────────────────────────────────────────────────────────────────
// airplanes.live Community API — free, no key, CORS wildcard.
const AIRPLANESLIVE_URL = 'https://api.airplanes.live/v2/point/48/67/1500'

interface AirplanesLiveAC {
  hex?: string
  flight?: string
  lat?: number
  lon?: number
  alt_baro?: number | string  // can be "ground"
  gs?: number
  track?: number
  r?: string        // registration
  t?: string        // ICAO type code
  desc?: string     // aircraft description
  baro_rate?: number
  seen_pos?: number
  squawk?: string
}

export async function getFlights(): Promise<Flight[]> {
  try {
    const resp = await fetch(AIRPLANESLIVE_URL, { headers: { Accept: 'application/json' } })
    if (!resp.ok) return []
    const data = await resp.json() as { ac?: AirplanesLiveAC[] }
    const ac = data?.ac ?? []
    return ac
      .filter(a =>
        a.lat != null && a.lon != null &&
        a.lat >= 40 && a.lat <= 56 &&
        a.lon >= 48 && a.lon <= 88 &&
        typeof a.alt_baro === 'number'   // exclude "ground"
      )
      .map(a => ({
        icao:      a.hex ?? '',
        callsign:  (a.flight ?? '').trim(),
        lat:       a.lat!,
        lon:       a.lon!,
        alt:       typeof a.alt_baro === 'number' ? Math.round(a.alt_baro * 0.3048) : null,
        speed:     a.gs ?? null,
        heading:   a.track ?? null,
        reg:       a.r ?? null,
        aircraft:  a.desc ? titleCase(a.desc) : null,
        type_code: a.t ?? null,
        vert_rate: a.baro_rate ?? null,
        seen_pos:  a.seen_pos ?? null,
        squawk:    a.squawk ?? null,
      }))
  } catch {
    return []
  }
}

function titleCase(s: string): string {
  return s.toLowerCase().replace(/\b\w/g, c => c.toUpperCase())
}

// ── Vessels ───────────────────────────────────────────────────────────────────
export const getVessels = () =>
  get<Vessel[]>('/geo/vessels').catch(() => [] as Vessel[])
