'use client'

import { useEffect, useMemo, useState } from 'react'
import { useRouter } from 'next/navigation'
import { Loader2, Search, CornerDownLeft } from 'lucide-react'

interface SearchDocument {
  id: string
  kind: 'page' | 'dataset' | 'connector' | 'pipeline' | 'recipe' | 'saved_query'
  title: string
  subtitle: string
  href: string
  status?: string
  icon?: string
}

interface SearchResult {
  document: SearchDocument
  score: number
  matchedFields: string[]
}

interface SearchGroup {
  kind: SearchDocument['kind']
  label: string
  results: SearchResult[]
}

export default function GlobalSearch() {
  const router = useRouter()
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const [loading, setLoading] = useState(false)
  const [groups, setGroups] = useState<SearchGroup[]>([])
  const [selectedIndex, setSelectedIndex] = useState(0)

  const flatResults = useMemo(
    () => groups.flatMap(group => group.results.map(result => ({ group: group.label, result }))),
    [groups],
  )

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        setOpen(current => !current)
      }
      if (event.key === 'Escape') {
        setOpen(false)
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  useEffect(() => {
    if (!open) return
    const controller = new AbortController()
    const handle = window.setTimeout(async () => {
      if (!query.trim()) {
        setGroups([])
        setLoading(false)
        setSelectedIndex(0)
        return
      }

      try {
        setLoading(true)
        const res = await fetch(`/api/search?q=${encodeURIComponent(query)}`, { signal: controller.signal })
        const payload = res.ok ? await res.json() : { groups: [] }
        setGroups(Array.isArray(payload.groups) ? payload.groups : [])
        setSelectedIndex(0)
      } catch {
        if (!controller.signal.aborted) setGroups([])
      } finally {
        if (!controller.signal.aborted) setLoading(false)
      }
    }, 120)

    return () => {
      controller.abort()
      window.clearTimeout(handle)
    }
  }, [open, query])

  useEffect(() => {
    if (!open) return

    function onKeyDown(event: KeyboardEvent) {
      if (!flatResults.length) return
      if (event.key === 'ArrowDown') {
        event.preventDefault()
        setSelectedIndex(current => Math.min(flatResults.length - 1, current + 1))
      }
      if (event.key === 'ArrowUp') {
        event.preventDefault()
        setSelectedIndex(current => Math.max(0, current - 1))
      }
      if (event.key === 'Enter') {
        event.preventDefault()
        const selected = flatResults[selectedIndex]
        if (selected) {
          router.push(selected.result.document.href)
          setOpen(false)
        }
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [flatResults, open, router, selectedIndex])

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="hidden md:flex items-center gap-2 text-xs text-chef-muted hover:text-chef-text border border-chef-border rounded-md px-3 py-1.5 transition-colors hover:border-chef-border bg-chef-card"
      >
        <Search size={12} />
        <span>Search everything...</span>
        <kbd className="ml-2 text-[10px] text-chef-muted bg-chef-border rounded px-1">⌘K</kbd>
      </button>
    )
  }

  return (
    <>
      <button
        onClick={() => setOpen(true)}
        className="hidden md:flex items-center gap-2 text-xs text-chef-text border border-indigo-500/30 rounded-md px-3 py-1.5 bg-indigo-500/10"
      >
        <Search size={12} />
        <span>Search everything...</span>
        <kbd className="ml-2 text-[10px] text-chef-muted bg-chef-border rounded px-1">⌘K</kbd>
      </button>

      <div className="fixed inset-0 z-[90] bg-black/40 backdrop-blur-sm px-4 py-16" onClick={() => setOpen(false)}>
        <div className="mx-auto w-full max-w-2xl rounded-2xl border border-chef-border bg-chef-surface shadow-2xl" onClick={event => event.stopPropagation()}>
          <div className="flex items-center gap-3 border-b border-chef-border px-4 py-3">
            <Search size={15} className="text-chef-muted" />
            <input
              autoFocus
              value={query}
              onChange={event => setQuery(event.target.value)}
              placeholder="Search pages, datasets, connectors, pipelines, recipes, and saved queries"
              className="w-full bg-transparent text-sm text-chef-text outline-none placeholder:text-chef-muted"
            />
            {loading ? <Loader2 size={14} className="animate-spin text-chef-muted" /> : null}
          </div>

          <div className="max-h-[60vh] overflow-auto p-2">
            {!query.trim() ? (
              <div className="px-3 py-10 text-center text-sm text-chef-muted">
                Search the tenant for pages, datasets, connectors, pipelines, recipes, and saved queries.
              </div>
            ) : flatResults.length === 0 && !loading ? (
              <div className="px-3 py-10 text-center text-sm text-chef-muted">No results for "{query}".</div>
            ) : (
              groups.map(group => (
                <div key={group.kind} className="mb-3">
                  <div className="px-3 py-2 text-[10px] font-semibold uppercase tracking-widest text-chef-muted">
                    {group.label}
                  </div>
                  {group.results.map(result => {
                    const globalIndex = flatResults.findIndex(item => item.result.document.id === result.document.id)
                    const active = globalIndex === selectedIndex
                    return (
                      <button
                        key={result.document.id}
                        onMouseEnter={() => setSelectedIndex(globalIndex)}
                        onClick={() => {
                          router.push(result.document.href)
                          setOpen(false)
                        }}
                        className={`flex w-full items-center gap-3 rounded-xl px-3 py-3 text-left transition-colors ${active ? 'bg-indigo-500/10' : 'hover:bg-chef-card'}`}
                      >
                        <div className="flex-1 min-w-0">
                          <div className="text-sm font-medium text-chef-text">{result.document.title}</div>
                          <div className="truncate text-xs text-chef-muted">{result.document.subtitle}</div>
                        </div>
                        <div className="flex items-center gap-2 shrink-0 text-[10px] text-chef-muted">
                          {result.document.status ? <span className="rounded bg-chef-card px-1.5 py-0.5">{result.document.status}</span> : null}
                          <CornerDownLeft size={12} />
                        </div>
                      </button>
                    )
                  })}
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </>
  )
}
