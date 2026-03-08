/* Rick & Morty API — types, fetcher, cache, flattener */

export interface RMCharacter {
  id: number
  name: string
  status: 'Alive' | 'Dead' | 'unknown'
  species: string
  type: string
  gender: 'Female' | 'Male' | 'Genderless' | 'unknown'
  origin:   { name: string; url: string }
  location: { name: string; url: string }
  image: string
  episode: string[]
  url: string
  created: string
}

interface RMPage {
  info: { count: number; pages: number; next: string | null; prev: string | null }
  results: RMCharacter[]
}

// ── Module-level cache persists across requests ────────────────────────────
let _cache: RMCharacter[] | null = null

/** Fetch a single page, throwing a descriptive error if the response is not ok */
async function fetchPage(url: string, attempt = 1): Promise<RMPage> {
  const resp = await fetch(url, {
    headers: { 'Accept': 'application/json' },
    // Next.js fetch cache — reuse for 10 minutes
    next: { revalidate: 600 },
  } as RequestInit)

  if (!resp.ok) {
    if (resp.status === 429 && attempt <= 3) {
      // Rate-limited — wait and retry with backoff
      await new Promise(r => setTimeout(r, attempt * 400))
      return fetchPage(url, attempt + 1)
    }
    throw new Error(`Rick & Morty API HTTP ${resp.status} for ${url}`)
  }

  const data: unknown = await resp.json()
  if (typeof data !== 'object' || data === null || !('results' in data)) {
    throw new Error('Unexpected shape from Rick & Morty API')
  }
  return data as RMPage
}

export async function getCharacters(): Promise<RMCharacter[]> {
  if (_cache) return _cache

  // Fetch page 1 to learn total page count
  const first = await fetchPage('https://rickandmortyapi.com/api/character/')
  const pages: RMPage[] = [first]

  // Fetch remaining pages sequentially with a small delay — avoids 429s
  for (let p = 2; p <= first.info.pages; p++) {
    await new Promise(r => setTimeout(r, 60))
    pages.push(await fetchPage(`https://rickandmortyapi.com/api/character/?page=${p}`))
  }

  _cache = pages.flatMap(p => p.results)
  console.log(`[dataChef] RM API: cached ${_cache.length} characters`)
  return _cache
}

// ── Flat structure used by the SQL table ──────────────────────────────────
export interface FlatCharacter {
  id:        number
  name:      string
  status:    string
  species:   string
  type:      string
  gender:    string
  origin:    string
  location:  string
  episodes:  number
  created:   string
}

export function flattenCharacter(c: RMCharacter): FlatCharacter {
  return {
    id:       c.id,
    name:     c.name,
    status:   c.status,
    species:  c.species,
    type:     c.type || '(none)',
    gender:   c.gender,
    origin:   c.origin.name,
    location: c.location.name,
    episodes: c.episode.length,
    created:  c.created.split('T')[0],
  }
}
