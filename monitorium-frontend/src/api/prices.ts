import get from './client'

export interface Price {
  date: string
  ticker: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  currency: string
}

export const getPrices = (params: { ticker?: string; from?: string; to?: string }) =>
  get<Price[]>('/prices', params)

export const getLatestPrice = (ticker: string) =>
  get<Price>(`/prices/${ticker}/latest`)
