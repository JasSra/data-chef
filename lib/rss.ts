import 'server-only'

/**
 * RSS / Atom feed parser with safe fallbacks for broken feeds.
 *
 * - Handles RSS 2.0, RSS 1.0 (RDF), and Atom feeds
 * - Tolerant of malformed XML (missing closing tags, HTML entities, CDATA issues)
 * - Supports delta appending: caller provides `lastSeenGuid` set, only new items returned
 * - Custom headers + auth support for protected feeds
 */

import { buildAuthHeaders, type HttpAuthOptions } from '@/lib/runtime-data'

export interface FeedItem {
  guid: string
  title: string
  link: string
  pubDate: string
  author: string
  description: string
  categories: string
  source: string
}

export interface FeedMeta {
  feedTitle: string
  feedLink: string
  feedDescription: string
  feedType: 'rss2' | 'rss1' | 'atom' | 'unknown'
  itemCount: number
}

export interface FetchFeedResult {
  meta: FeedMeta
  items: FeedItem[]
  newItems: FeedItem[]         // delta: items not in lastSeenGuids
  error?: string
}

/* ── XML helpers (no external dep, tolerant of broken feeds) ──── */

function unescapeXml(s: string): string {
  return s
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCharCode(parseInt(h, 16)))
}

function stripCdata(s: string): string {
  return s.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
}

/** Extract text content of a tag, tolerating CDATA and missing close tags */
function tagText(xml: string, tag: string): string {
  // Try standard <tag>...</tag>
  const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, 'i')
  const m = xml.match(re)
  if (m) return unescapeXml(stripCdata(m[1])).trim()

  // Try self-closing <tag ... />
  const self = new RegExp(`<${tag}\\s+[^/]*/>`, 'i')
  const s = xml.match(self)
  if (s) return ''

  return ''
}

/** Extract href attribute from Atom <link> */
function atomLink(xml: string): string {
  const m = xml.match(/<link[^>]*href=["']([^"']+)["'][^>]*\/?>/i)
  return m ? unescapeXml(m[1]).trim() : tagText(xml, 'link')
}

/** Split XML into blocks matching a given element name */
function splitElements(xml: string, tagName: string): string[] {
  const results: string[] = []
  const openTag = `<${tagName}`
  const closeTag = `</${tagName}>`
  let pos = 0
  while (pos < xml.length) {
    const start = xml.indexOf(openTag, pos)
    if (start === -1) break
    let end = xml.indexOf(closeTag, start)
    if (end === -1) {
      // Broken feed: no closing tag — try to find next opening tag as boundary
      const nextOpen = xml.indexOf(openTag, start + 1)
      end = nextOpen !== -1 ? nextOpen : xml.length
      results.push(xml.slice(start, end))
    } else {
      results.push(xml.slice(start, end + closeTag.length))
    }
    pos = end === xml.length ? end : end + closeTag.length
  }
  return results
}

function detectFeedType(xml: string): 'rss2' | 'rss1' | 'atom' | 'unknown' {
  if (/<feed[\s>]/i.test(xml)) return 'atom'
  if (/<rdf:RDF/i.test(xml)) return 'rss1'
  if (/<rss[\s>]/i.test(xml)) return 'rss2'
  // Some feeds omit the <rss> wrapper
  if (/<channel[\s>]/i.test(xml) && /<item[\s>]/i.test(xml)) return 'rss2'
  return 'unknown'
}

function parseRssItem(itemXml: string, feedSource: string): FeedItem {
  const guid = tagText(itemXml, 'guid') || tagText(itemXml, 'link') || tagText(itemXml, 'title') || `${feedSource}#${Date.now()}-${Math.random()}`
  const pubDateRaw = tagText(itemXml, 'pubDate') || tagText(itemXml, 'dc:date') || tagText(itemXml, 'date')
  let pubDate = ''
  if (pubDateRaw) {
    try { pubDate = new Date(pubDateRaw).toISOString() } catch { pubDate = pubDateRaw }
  }
  return {
    guid,
    title: tagText(itemXml, 'title'),
    link: tagText(itemXml, 'link'),
    pubDate,
    author: tagText(itemXml, 'author') || tagText(itemXml, 'dc:creator'),
    description: tagText(itemXml, 'description') || tagText(itemXml, 'content:encoded'),
    categories: splitElements(itemXml, 'category').map(c => tagText(c, 'category') || unescapeXml(stripCdata(c.replace(/<\/?category[^>]*>/gi, '')))).filter(Boolean).join(', '),
    source: feedSource,
  }
}

function parseAtomEntry(entryXml: string, feedSource: string): FeedItem {
  const id = tagText(entryXml, 'id') || atomLink(entryXml) || `${feedSource}#${Date.now()}-${Math.random()}`
  const updatedRaw = tagText(entryXml, 'updated') || tagText(entryXml, 'published')
  let pubDate = ''
  if (updatedRaw) {
    try { pubDate = new Date(updatedRaw).toISOString() } catch { pubDate = updatedRaw }
  }
  return {
    guid: id,
    title: tagText(entryXml, 'title'),
    link: atomLink(entryXml),
    pubDate,
    author: tagText(entryXml, 'name') || tagText(entryXml, 'author'),
    description: tagText(entryXml, 'summary') || tagText(entryXml, 'content'),
    categories: splitElements(entryXml, 'category').map(c => {
      const termMatch = c.match(/term=["']([^"']+)["']/i)
      return termMatch ? termMatch[1] : tagText(c, 'category')
    }).filter(Boolean).join(', '),
    source: feedSource,
  }
}

function parseFeedXml(xml: string, feedUrl: string): { meta: FeedMeta; items: FeedItem[] } {
  const feedType = detectFeedType(xml)

  let items: FeedItem[] = []
  let feedTitle = ''
  let feedLink = ''
  let feedDescription = ''

  if (feedType === 'atom') {
    feedTitle = tagText(xml, 'title')
    feedLink = atomLink(xml)
    feedDescription = tagText(xml, 'subtitle')
    items = splitElements(xml, 'entry').map(e => parseAtomEntry(e, feedUrl))
  } else {
    // RSS 2.0 or RSS 1.0
    feedTitle = tagText(xml, 'title')
    feedLink = tagText(xml, 'link')
    feedDescription = tagText(xml, 'description')
    items = splitElements(xml, 'item').map(e => parseRssItem(e, feedUrl))
  }

  // Strip HTML from descriptions (keep it simple for table display)
  for (const item of items) {
    item.description = item.description
      .replace(/<[^>]+>/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, 500)
  }

  return {
    meta: {
      feedTitle: feedTitle || feedUrl,
      feedLink: feedLink || feedUrl,
      feedDescription,
      feedType,
      itemCount: items.length,
    },
    items,
  }
}

/* ── Public API ───────────────────────────────────────────────── */

export async function fetchRssFeed(
  url: string,
  auth: HttpAuthOptions = {},
  options: {
    lastSeenGuids?: Set<string>
    rowLimit?: number
    timeoutMs?: number
    customHeaders?: Record<string, string>
  } = {},
): Promise<FetchFeedResult> {
  const rowLimit = options.rowLimit ?? 500
  const lastSeenGuids = options.lastSeenGuids ?? new Set<string>()

  const headers: Record<string, string> = {
    ...buildAuthHeaders(auth, 'dataChef-rss/0.1'),
    Accept: 'application/rss+xml, application/atom+xml, application/xml, text/xml, */*',
  }
  // Apply custom headers (may override defaults)
  if (options.customHeaders) {
    for (const [k, v] of Object.entries(options.customHeaders)) {
      if (k && v) headers[k] = v
    }
  }

  let text: string
  try {
    const res = await fetch(url, {
      headers,
      signal: AbortSignal.timeout(options.timeoutMs ?? 15_000),
    })
    if (!res.ok) {
      return {
        meta: { feedTitle: '', feedLink: url, feedDescription: '', feedType: 'unknown', itemCount: 0 },
        items: [],
        newItems: [],
        error: `HTTP ${res.status} ${res.statusText}`,
      }
    }
    text = await res.text()
  } catch (e) {
    return {
      meta: { feedTitle: '', feedLink: url, feedDescription: '', feedType: 'unknown', itemCount: 0 },
      items: [],
      newItems: [],
      error: e instanceof Error ? e.message : String(e),
    }
  }

  // Try XML parse, fallback for totally broken feeds
  let parsed: { meta: FeedMeta; items: FeedItem[] }
  try {
    parsed = parseFeedXml(text, url)
  } catch (e) {
    return {
      meta: { feedTitle: '', feedLink: url, feedDescription: '', feedType: 'unknown', itemCount: 0 },
      items: [],
      newItems: [],
      error: `Feed parse error: ${e instanceof Error ? e.message : String(e)}`,
    }
  }

  if (parsed.items.length === 0 && !parsed.meta.feedTitle) {
    // Maybe it's JSON feed (some sites serve JSON)
    try {
      const json = JSON.parse(text)
      if (json.items && Array.isArray(json.items)) {
        parsed.meta.feedType = 'unknown'
        parsed.meta.feedTitle = json.title ?? ''
        parsed.meta.feedLink = json.home_page_url ?? url
        parsed.meta.feedDescription = json.description ?? ''
        parsed.items = (json.items as Array<Record<string, unknown>>).slice(0, rowLimit).map((item, i) => ({
          guid: String(item.id ?? item.url ?? `${url}#${i}`),
          title: String(item.title ?? ''),
          link: String(item.url ?? ''),
          pubDate: item.date_published ? new Date(String(item.date_published)).toISOString() : '',
          author: typeof item.author === 'object' && item.author ? String((item.author as Record<string, unknown>).name ?? '') : String(item.author ?? ''),
          description: String(item.content_text ?? item.summary ?? '').slice(0, 500),
          categories: Array.isArray(item.tags) ? (item.tags as string[]).join(', ') : '',
          source: url,
        }))
        parsed.meta.itemCount = parsed.items.length
      }
    } catch {
      // Not JSON either — give up gracefully
    }
  }

  const items = parsed.items.slice(0, rowLimit)
  const newItems = lastSeenGuids.size > 0
    ? items.filter(item => !lastSeenGuids.has(item.guid))
    : items

  return {
    meta: { ...parsed.meta, itemCount: items.length },
    items,
    newItems,
  }
}

/** Probe an RSS feed URL — quick check that it responds and has items */
export async function probeRssFeed(
  url: string,
  auth: HttpAuthOptions = {},
  customHeaders?: Record<string, string>,
): Promise<{ ok: boolean; feedTitle?: string; itemCount?: number; feedType?: string; error?: string }> {
  const result = await fetchRssFeed(url, auth, { rowLimit: 5, timeoutMs: 10_000, customHeaders })
  if (result.error) return { ok: false, error: result.error }
  if (result.items.length === 0) return { ok: false, error: 'Feed returned no items' }
  return {
    ok: true,
    feedTitle: result.meta.feedTitle,
    itemCount: result.meta.itemCount,
    feedType: result.meta.feedType,
  }
}

/** Convert feed items to flat row records for dataset materialization */
export function feedItemsToRows(items: FeedItem[]): Record<string, unknown>[] {
  return items.map(item => ({
    guid: item.guid,
    title: item.title,
    link: item.link,
    pubDate: item.pubDate,
    author: item.author,
    description: item.description,
    categories: item.categories,
    source: item.source,
  }))
}
