import get from './client'

export interface Article {
  article_id: string
  source: string
  title: string
  url: string
  pub_date: string
  companies: string
  sectors: string
  topics: string
  impact: string
  weight: number
}

export const getNews = (params: {
  ticker?: string
  topic?: string
  source?: string
  from?: string
  to?: string
  limit?: number
}) => get<Article[]>('/news', params)
