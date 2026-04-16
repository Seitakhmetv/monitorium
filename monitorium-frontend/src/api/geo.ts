import get from './client'

export interface Flight {
  icao:     string
  callsign: string
  lat:      number
  lon:      number
  alt:      number | null
  speed:    number | null
  heading:  number | null
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

export const getFlights = () => get<Flight[]>('/geo/flights')
export const getVessels = () => get<Vessel[]>('/geo/vessels')
