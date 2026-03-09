'use client'

import { createContext, useContext, useEffect, useMemo, useState } from 'react'
import type { AppSettings } from '@/lib/app-settings-schema'

interface SettingsContextValue {
  settings: AppSettings | null
  loading: boolean
  refresh: () => Promise<void>
  saveSettings: (patch: Partial<AppSettings>) => Promise<AppSettings>
  rotateKey: (key: keyof AppSettings['apiKeys']) => Promise<AppSettings>
  purgeData: () => Promise<void>
  deleteWorkspace: () => Promise<AppSettings>
}

const SettingsContext = createContext<SettingsContextValue | null>(null)

async function fetchSettings(): Promise<AppSettings> {
  const res = await fetch('/api/settings', { cache: 'no-store' })
  if (!res.ok) throw new Error('Failed to load settings')
  return res.json()
}

export function SettingsProvider({ children }: { children: React.ReactNode }) {
  const [settings, setSettings] = useState<AppSettings | null>(null)
  const [loading, setLoading] = useState(true)

  const refresh = async () => {
    const next = await fetchSettings()
    setSettings(next)
  }

  useEffect(() => {
    refresh().finally(() => setLoading(false))
  }, [])

  const value = useMemo<SettingsContextValue>(() => ({
    settings,
    loading,
    refresh,
    saveSettings: async (patch) => {
      const res = await fetch('/api/settings', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(patch),
      })
      if (!res.ok) throw new Error('Failed to save settings')
      const next = await res.json() as AppSettings
      setSettings(next)
      return next
    },
    rotateKey: async (key) => {
      const res = await fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'rotate-key', key }),
      })
      if (!res.ok) throw new Error('Failed to rotate key')
      const next = await res.json() as AppSettings
      setSettings(next)
      return next
    },
    purgeData: async () => {
      const res = await fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'purge-data' }),
      })
      if (!res.ok) throw new Error('Failed to purge data')
    },
    deleteWorkspace: async () => {
      const res = await fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'delete-workspace' }),
      })
      if (!res.ok) throw new Error('Failed to delete workspace')
      const next = await res.json() as AppSettings
      setSettings(next)
      return next
    },
  }), [loading, settings])

  return (
    <SettingsContext.Provider value={value}>
      {children}
    </SettingsContext.Provider>
  )
}

export function useAppSettings() {
  const ctx = useContext(SettingsContext)
  if (!ctx) throw new Error('useAppSettings must be used inside SettingsProvider')
  return ctx
}
