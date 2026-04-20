import get from './client'

export interface Bullet {
  text:   string
  url:    string    // article URL, may be empty
  source: string    // e.g. "kursiv", may be empty
}

export interface MarketSummary {
  headline:      string
  kz_bullets:    Bullet[]
  world_bullets: Bullet[]
  generated_at:  string
  article_count: number
  error?:        string
}

export const getSummary = (lang = 'en') =>
  get<MarketSummary>('/summary', { lang })
