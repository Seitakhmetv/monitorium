import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface WatchlistStore {
  tickers: string[]
  add: (ticker: string) => void
  remove: (ticker: string) => void
  has: (ticker: string) => boolean
}

export const useWatchlist = create<WatchlistStore>()(
  persist(
    (set, get) => ({
      tickers: ['KSPI', 'HSBK', 'KMG'],
      add: (ticker) => set((s) => ({ tickers: [...new Set([...s.tickers, ticker.toUpperCase()])] })),
      remove: (ticker) => set((s) => ({ tickers: s.tickers.filter((t) => t !== ticker.toUpperCase()) })),
      has: (ticker) => get().tickers.includes(ticker.toUpperCase()),
    }),
    { name: 'monitorium-watchlist' }
  )
)
