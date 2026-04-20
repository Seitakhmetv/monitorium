import { NavLink, Outlet, useNavigate } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import { motion } from 'framer-motion'
import { BarChart2, Newspaper, TrendingUp, Search, Map, GitBranch, LineChart, Sun, Moon, X } from 'lucide-react'
import { useState } from 'react'
import { useDebounce } from '../hooks/useDebounce'
import { useTheme } from '../hooks/useTheme'

const LANGS = ['en', 'ru', 'kz'] as const

export default function Layout() {
  const { t, i18n } = useTranslation()
  const navigate = useNavigate()
  const [search, setSearch] = useState('')
  const [searchOpen, setSearchOpen] = useState(false)
  const debouncedSearch = useDebounce(search)
  const { theme, toggle: toggleTheme } = useTheme()

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    if (debouncedSearch.trim()) {
      navigate(`/ticker/${debouncedSearch.trim().toUpperCase()}`)
      setSearch('')
      setSearchOpen(false)
    }
  }

  const navItems = [
    { to: '/',           icon: BarChart2,  label: t('nav.overview')    },
    { to: '/news',       icon: Newspaper,  label: t('nav.news')        },
    { to: '/macro',      icon: TrendingUp, label: t('nav.macro')       },
    { to: '/map',        icon: Map,        label: t('nav.map')         },
    { to: '/financials', icon: GitBranch,  label: t('nav.financials')  },
    { to: '/analytics',  icon: LineChart,  label: 'Analytics'          },
  ]

  return (
    <div className="flex flex-col h-full min-h-screen" style={{ background: 'var(--color-bg)' }}>
      {/* Top bar */}
      <header
        className="flex items-center gap-2 px-3 sm:px-6 sm:gap-4 h-14 border-b shrink-0"
        style={{ borderColor: 'var(--color-border)', background: 'var(--color-surface)' }}
      >
        {/* Logo */}
        <NavLink to="/" className="flex items-center gap-2 shrink-0">
          <div className="w-7 h-7 rounded flex items-center justify-center"
            style={{ background: 'var(--color-accent)' }}>
            <span className="text-black font-black text-xs">M</span>
          </div>
          <span className="font-bold tracking-wide hidden sm:block"
            style={{ color: 'var(--color-heading)', fontSize: 15 }}>
            MONITORIUM
          </span>
        </NavLink>

        {/* Nav */}
        <nav className="flex items-center gap-0.5 sm:gap-1 sm:ml-2 flex-1 sm:flex-none">
          {navItems.map(({ to, icon: Icon, label }) => (
            <NavLink
              key={to}
              to={to}
              end={to === '/'}
              className={({ isActive }) =>
                `flex items-center gap-1.5 px-2 sm:px-3 py-1.5 rounded text-sm font-medium transition-colors ${
                  isActive ? 'text-amber-400 bg-amber-400/10' : 'hover:text-white hover:bg-white/5'
                }`
              }
              style={({ isActive }) => ({ color: isActive ? 'var(--color-accent)' : 'var(--color-text)' })}
            >
              <Icon size={14} />
              <span className="hidden md:inline">{label}</span>
            </NavLink>
          ))}
        </nav>

        {/* Search — full input on desktop, icon toggle on mobile */}
        <div className="flex items-center gap-2 ml-auto shrink-0">
          {/* Mobile search toggle */}
          <button
            className="sm:hidden p-1.5 rounded"
            style={{ color: 'var(--color-muted)' }}
            onClick={() => setSearchOpen(o => !o)}
          >
            {searchOpen ? <X size={15} /> : <Search size={15} />}
          </button>

          {/* Desktop search */}
          <form onSubmit={handleSearch} className="hidden sm:block w-48">
            <div className="relative">
              <Search size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2"
                style={{ color: 'var(--color-muted)' }} />
              <input
                value={search}
                onChange={e => setSearch(e.target.value)}
                placeholder={t('common.search')}
                className="w-full pl-8 pr-3 py-1.5 rounded text-sm outline-none"
                style={{
                  background: 'var(--color-border)',
                  color: 'var(--color-heading)',
                  border: '1px solid var(--color-border)',
                }}
              />
            </div>
          </form>

          {/* Theme toggle */}
          <button
            onClick={toggleTheme}
            className="p-1.5 rounded transition-colors"
            style={{ color: 'var(--color-muted)' }}
          >
            {theme === 'dark' ? <Sun size={15} /> : <Moon size={15} />}
          </button>

          {/* Lang switcher — desktop only */}
          <div className="hidden sm:flex gap-1">
            {LANGS.map(lang => (
              <button
                key={lang}
                onClick={() => i18n.changeLanguage(lang)}
                className="px-2 py-0.5 rounded text-xs font-medium uppercase transition-colors"
                style={{
                  background: i18n.language === lang ? 'var(--color-accent)' : 'transparent',
                  color: i18n.language === lang ? '#000' : 'var(--color-muted)',
                }}
              >
                {lang}
              </button>
            ))}
          </div>
        </div>
      </header>

      {/* Mobile search bar — slides in below header */}
      {searchOpen && (
        <form
          onSubmit={handleSearch}
          className="sm:hidden px-3 py-2 border-b shrink-0"
          style={{ borderColor: 'var(--color-border)', background: 'var(--color-surface)' }}
        >
          <div className="relative">
            <Search size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2"
              style={{ color: 'var(--color-muted)' }} />
            <input
              autoFocus
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder={t('common.search')}
              className="w-full pl-8 pr-3 py-2 rounded text-sm outline-none"
              style={{
                background: 'var(--color-border)',
                color: 'var(--color-heading)',
                border: '1px solid var(--color-border)',
              }}
            />
          </div>
        </form>
      )}

      {/* Page content */}
      <main className="flex-1 overflow-auto">
        <motion.div
          key={location.pathname}
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.18 }}
          className="h-full"
        >
          <Outlet />
        </motion.div>
      </main>
    </div>
  )
}
