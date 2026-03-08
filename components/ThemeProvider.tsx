'use client'

import { createContext, useContext, useEffect, useState } from 'react'

type Theme = 'dark' | 'light'

interface ThemeCtxType {
  theme:     Theme
  toggle:    () => void
  setTheme:  (t: Theme) => void
}

const ThemeCtx = createContext<ThemeCtxType>({
  theme: 'dark', toggle: () => {}, setTheme: () => {},
})

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  // Initialise from the class already applied by the anti-FOUC script
  const [theme, setThemeState] = useState<Theme>('dark')

  useEffect(() => {
    // Sync React state to the class already on <html>
    const isLight = document.documentElement.classList.contains('light')
    setThemeState(isLight ? 'light' : 'dark')
  }, [])

  const setTheme = (t: Theme) => {
    setThemeState(t)
    if (t === 'light') {
      document.documentElement.classList.add('light')
    } else {
      document.documentElement.classList.remove('light')
    }
    try { localStorage.setItem('chef-theme', t) } catch { /* ignore */ }
  }

  const toggle = () => setTheme(theme === 'dark' ? 'light' : 'dark')

  return (
    <ThemeCtx.Provider value={{ theme, toggle, setTheme }}>
      {children}
    </ThemeCtx.Provider>
  )
}

export function useTheme() {
  return useContext(ThemeCtx)
}
