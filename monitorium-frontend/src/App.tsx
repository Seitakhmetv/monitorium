import { lazy, Suspense } from 'react'
import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import { Spinner } from './components/ui'

const OverviewPage = lazy(() => import('./features/overview/OverviewPage'))
const TickerPage   = lazy(() => import('./features/ticker/TickerPage'))
const NewsPage     = lazy(() => import('./features/news/NewsPage'))
const MacroPage    = lazy(() => import('./features/macro/MacroPage'))

function PageFallback() {
  return (
    <div className="flex items-center justify-center h-64">
      <Spinner />
    </div>
  )
}

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Suspense fallback={<PageFallback />}><OverviewPage /></Suspense>} />
        <Route path="ticker/:ticker" element={<Suspense fallback={<PageFallback />}><TickerPage /></Suspense>} />
        <Route path="news" element={<Suspense fallback={<PageFallback />}><NewsPage /></Suspense>} />
        <Route path="macro" element={<Suspense fallback={<PageFallback />}><MacroPage /></Suspense>} />
      </Route>
    </Routes>
  )
}
