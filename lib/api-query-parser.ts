/**
 * ApiQL Parser — hand-written tokenizer + recursive descent parser.
 * Parses pipe-delimited query language into an AST for API execution.
 *
 * Syntax:
 *   service("name") | endpoint("/path", GET) | where(field = "value") | select(a, b) | limit(10)
 */

/* ── Token types ───────────────────────────────────────────────────── */

type TokenType =
  | 'IDENT' | 'STRING' | 'NUMBER' | 'BOOLEAN'
  | 'PIPE' | 'PAREN_OPEN' | 'PAREN_CLOSE'
  | 'COMMA' | 'COLON' | 'DOT'
  | 'EQUALS' | 'NOT_EQUALS' | 'GT' | 'LT' | 'GTE' | 'LTE'
  | 'EOF'

interface Token {
  type: TokenType
  value: string
  pos: number
}

/* ── AST types ─────────────────────────────────────────────────────── */

export interface ApiQueryAST {
  service: string
  stages: QueryStage[]
}

export type QueryStage =
  | EndpointStage
  | WhereStage
  | SelectStage
  | OrderByStage
  | LimitStage
  | OffsetStage
  | BodyStage
  | HeaderStage
  | ChainStage
  | GroupByStage
  | AggregateStage
  | NoCacheStage
  | BenchmarkStage

export interface EndpointStage { type: 'endpoint'; path: string; method: string }
export interface WhereStage { type: 'where'; conditions: WhereCondition[] }
export interface SelectStage { type: 'select'; fields: string[] }
export interface OrderByStage { type: 'order_by'; field: string; direction: 'asc' | 'desc' }
export interface LimitStage { type: 'limit'; count: number }
export interface OffsetStage { type: 'offset'; count: number }
export interface BodyStage { type: 'body'; json: string }
export interface HeaderStage { type: 'header'; name: string; value: string }
export interface ChainStage { type: 'chain'; path: string; method: string; bindings: ChainBinding[] }
export interface GroupByStage { type: 'group_by'; fields: string[] }
export interface AggregateStage { type: 'aggregate'; aggregations: AggregationExpr[] }
export interface NoCacheStage { type: 'no_cache' }
export interface BenchmarkStage { type: 'benchmark'; runs: number; concurrency: number }

export interface WhereCondition {
  field: string
  operator: '=' | '!=' | '>' | '<' | '>=' | '<=' | 'contains' | 'startswith'
  value: string | number | boolean
}

export interface ChainBinding {
  targetParam: string
  sourceField: string
}

export interface AggregationExpr {
  fn: 'count' | 'sum' | 'avg' | 'min' | 'max'
  field: string
  alias: string
}

export interface ParseResult {
  ast: ApiQueryAST | null
  error: string | null
  /** Token position of the error for editor underlining */
  errorPos?: number
}

/* ── Tokenizer ─────────────────────────────────────────────────────── */

function tokenize(input: string): Token[] {
  const tokens: Token[] = []
  let i = 0

  while (i < input.length) {
    // Skip whitespace and newlines
    if (/\s/.test(input[i])) { i++; continue }

    // Skip comments (// to end of line)
    if (input[i] === '/' && input[i + 1] === '/') {
      while (i < input.length && input[i] !== '\n') i++
      continue
    }

    const pos = i

    // String literals
    if (input[i] === '"') {
      i++
      let str = ''
      while (i < input.length && input[i] !== '"') {
        if (input[i] === '\\' && i + 1 < input.length) { str += input[++i]; i++; continue }
        str += input[i++]
      }
      if (i < input.length) i++ // consume closing quote
      tokens.push({ type: 'STRING', value: str, pos })
      continue
    }

    // Single-quoted strings
    if (input[i] === "'") {
      i++
      let str = ''
      while (i < input.length && input[i] !== "'") {
        if (input[i] === '\\' && i + 1 < input.length) { str += input[++i]; i++; continue }
        str += input[i++]
      }
      if (i < input.length) i++
      tokens.push({ type: 'STRING', value: str, pos })
      continue
    }

    // Numbers
    if (/[0-9]/.test(input[i]) || (input[i] === '-' && /[0-9]/.test(input[i + 1] ?? ''))) {
      let num = ''
      if (input[i] === '-') num += input[i++]
      while (i < input.length && /[0-9.]/.test(input[i])) num += input[i++]
      tokens.push({ type: 'NUMBER', value: num, pos })
      continue
    }

    // Multi-char operators
    if (input[i] === '!' && input[i + 1] === '=') { tokens.push({ type: 'NOT_EQUALS', value: '!=', pos }); i += 2; continue }
    if (input[i] === '>' && input[i + 1] === '=') { tokens.push({ type: 'GTE', value: '>=', pos }); i += 2; continue }
    if (input[i] === '<' && input[i + 1] === '=') { tokens.push({ type: 'LTE', value: '<=', pos }); i += 2; continue }

    // Single-char tokens
    const charMap: Record<string, TokenType> = {
      '|': 'PIPE', '(': 'PAREN_OPEN', ')': 'PAREN_CLOSE',
      ',': 'COMMA', ':': 'COLON', '.': 'DOT',
      '=': 'EQUALS', '>': 'GT', '<': 'LT',
    }
    if (charMap[input[i]]) {
      tokens.push({ type: charMap[input[i]], value: input[i], pos })
      i++
      continue
    }

    // Identifiers and keywords
    if (/[a-zA-Z_]/.test(input[i])) {
      let ident = ''
      while (i < input.length && /[a-zA-Z0-9_.]/.test(input[i])) ident += input[i++]
      if (ident === 'true' || ident === 'false') {
        tokens.push({ type: 'BOOLEAN', value: ident, pos })
      } else {
        tokens.push({ type: 'IDENT', value: ident, pos })
      }
      continue
    }

    // Unknown character — skip
    i++
  }

  tokens.push({ type: 'EOF', value: '', pos: i })
  return tokens
}

/* ── Parser ────────────────────────────────────────────────────────── */

export function parseApiQL(input: string): ParseResult {
  const trimmed = input.trim()
  if (!trimmed) return { ast: null, error: 'Empty query' }

  const tokens = tokenize(trimmed)
  let cursor = 0

  function peek(): Token { return tokens[cursor] ?? { type: 'EOF', value: '', pos: trimmed.length } }
  function advance(): Token { return tokens[cursor++] ?? { type: 'EOF', value: '', pos: trimmed.length } }
  function expect(type: TokenType, context: string): Token {
    const t = advance()
    if (t.type !== type) throw new ParseError(`Expected ${type} in ${context}, got ${t.type} "${t.value}"`, t.pos)
    return t
  }

  try {
    // Parse service("name")
    const firstToken = peek()
    if (firstToken.type !== 'IDENT' || firstToken.value !== 'service') {
      throw new ParseError('Query must start with service("name")', firstToken.pos)
    }
    advance() // consume 'service'
    expect('PAREN_OPEN', 'service')
    const serviceName = expect('STRING', 'service name')
    expect('PAREN_CLOSE', 'service')

    const stages: QueryStage[] = []

    // Parse pipe-delimited stages
    while (peek().type === 'PIPE') {
      advance() // consume '|'
      const stageToken = peek()

      if (stageToken.type !== 'IDENT') {
        throw new ParseError(`Expected stage name after |, got "${stageToken.value}"`, stageToken.pos)
      }

      const stageName = advance().value

      switch (stageName) {
        case 'endpoint': stages.push(parseEndpoint()); break
        case 'where': stages.push(parseWhere()); break
        case 'select': stages.push(parseSelect()); break
        case 'order_by': stages.push(parseOrderBy()); break
        case 'limit': stages.push(parseLimit()); break
        case 'offset': stages.push(parseOffset()); break
        case 'body': stages.push(parseBody()); break
        case 'header': stages.push(parseHeader()); break
        case 'chain': stages.push(parseChain()); break
        case 'group_by': stages.push(parseGroupBy()); break
        case 'aggregate': stages.push(parseAggregate()); break
        case 'no_cache': stages.push({ type: 'no_cache' }); break
        case 'benchmark': stages.push(parseBenchmark()); break
        default:
          throw new ParseError(`Unknown stage: "${stageName}"`, stageToken.pos)
      }
    }

    if (peek().type !== 'EOF') {
      throw new ParseError(`Unexpected token: "${peek().value}"`, peek().pos)
    }

    return { ast: { service: serviceName.value, stages }, error: null }
  } catch (e) {
    if (e instanceof ParseError) {
      return { ast: null, error: e.message, errorPos: e.pos }
    }
    return { ast: null, error: String(e) }
  }

  /* ── Stage parsers ──────────────────────────────────────────────── */

  function parseEndpoint(): EndpointStage {
    expect('PAREN_OPEN', 'endpoint')
    const path = expect('STRING', 'endpoint path')
    let method = 'GET'
    if (peek().type === 'COMMA') {
      advance() // consume comma
      const m = advance()
      method = m.value.toUpperCase()
    }
    expect('PAREN_CLOSE', 'endpoint')
    return { type: 'endpoint', path: path.value, method }
  }

  function parseWhere(): WhereStage {
    expect('PAREN_OPEN', 'where')
    const conditions: WhereCondition[] = []

    while (peek().type !== 'PAREN_CLOSE' && peek().type !== 'EOF') {
      if (conditions.length > 0) {
        if (peek().type === 'COMMA') advance()
        // Skip 'and' / 'or' keywords if present
        if (peek().type === 'IDENT' && (peek().value === 'and' || peek().value === 'or')) advance()
      }

      const field = advance()
      if (field.type !== 'IDENT') throw new ParseError(`Expected field name in where, got "${field.value}"`, field.pos)

      let operator: WhereCondition['operator']
      const opToken = advance()
      switch (opToken.type) {
        case 'EQUALS': operator = '='; break
        case 'NOT_EQUALS': operator = '!='; break
        case 'GT': operator = '>'; break
        case 'LT': operator = '<'; break
        case 'GTE': operator = '>='; break
        case 'LTE': operator = '<='; break
        default:
          if (opToken.type === 'IDENT' && opToken.value === 'contains') { operator = 'contains'; break }
          if (opToken.type === 'IDENT' && opToken.value === 'startswith') { operator = 'startswith'; break }
          throw new ParseError(`Expected operator in where, got "${opToken.value}"`, opToken.pos)
      }

      const valueToken = advance()
      let value: string | number | boolean
      if (valueToken.type === 'STRING') value = valueToken.value
      else if (valueToken.type === 'NUMBER') value = Number(valueToken.value)
      else if (valueToken.type === 'BOOLEAN') value = valueToken.value === 'true'
      else throw new ParseError(`Expected value in where, got "${valueToken.value}"`, valueToken.pos)

      conditions.push({ field: field.value, operator, value })
    }

    expect('PAREN_CLOSE', 'where')
    return { type: 'where', conditions }
  }

  function parseSelect(): SelectStage {
    expect('PAREN_OPEN', 'select')
    const fields: string[] = []
    while (peek().type !== 'PAREN_CLOSE' && peek().type !== 'EOF') {
      if (fields.length > 0 && peek().type === 'COMMA') advance()
      const f = advance()
      if (f.type !== 'IDENT') throw new ParseError(`Expected field name in select, got "${f.value}"`, f.pos)
      fields.push(f.value)
    }
    expect('PAREN_CLOSE', 'select')
    return { type: 'select', fields }
  }

  function parseOrderBy(): OrderByStage {
    expect('PAREN_OPEN', 'order_by')
    const field = advance()
    if (field.type !== 'IDENT') throw new ParseError(`Expected field name in order_by, got "${field.value}"`, field.pos)
    let direction: 'asc' | 'desc' = 'asc'
    if (peek().type === 'IDENT' && (peek().value === 'asc' || peek().value === 'desc')) {
      direction = advance().value as 'asc' | 'desc'
    }
    expect('PAREN_CLOSE', 'order_by')
    return { type: 'order_by', field: field.value, direction }
  }

  function parseLimit(): LimitStage {
    expect('PAREN_OPEN', 'limit')
    const n = expect('NUMBER', 'limit count')
    expect('PAREN_CLOSE', 'limit')
    return { type: 'limit', count: Number(n.value) }
  }

  function parseOffset(): OffsetStage {
    expect('PAREN_OPEN', 'offset')
    const n = expect('NUMBER', 'offset count')
    expect('PAREN_CLOSE', 'offset')
    return { type: 'offset', count: Number(n.value) }
  }

  function parseBody(): BodyStage {
    expect('PAREN_OPEN', 'body')
    // Collect everything until matching close paren (handling nesting)
    let depth = 1
    let json = ''
    while (depth > 0 && cursor < tokens.length) {
      const t = tokens[cursor]
      if (t.type === 'PAREN_OPEN') depth++
      if (t.type === 'PAREN_CLOSE') { depth--; if (depth === 0) break }
      json += t.value + ' '
      cursor++
    }
    expect('PAREN_CLOSE', 'body')
    return { type: 'body', json: json.trim() }
  }

  function parseHeader(): HeaderStage {
    expect('PAREN_OPEN', 'header')
    const name = expect('STRING', 'header name')
    expect('COMMA', 'header')
    const value = expect('STRING', 'header value')
    expect('PAREN_CLOSE', 'header')
    return { type: 'header', name: name.value, value: value.value }
  }

  function parseChain(): ChainStage {
    expect('PAREN_OPEN', 'chain')
    const path = expect('STRING', 'chain path')
    let method = 'GET'
    const bindings: ChainBinding[] = []

    while (peek().type === 'COMMA') {
      advance() // consume comma
      const next = peek()

      // Check for method override
      if (next.type === 'IDENT' && ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'].includes(next.value.toUpperCase())) {
        method = advance().value.toUpperCase()
        continue
      }

      // Check for bind: target = source
      if (next.type === 'IDENT' && next.value === 'bind') {
        advance() // consume 'bind'
        expect('COLON', 'chain binding')
        const target = expect('IDENT', 'chain binding target')
        expect('EQUALS', 'chain binding')
        const source = expect('IDENT', 'chain binding source')
        bindings.push({ targetParam: target.value, sourceField: source.value })
        continue
      }

      // Unknown arg
      advance()
    }

    expect('PAREN_CLOSE', 'chain')
    return { type: 'chain', path: path.value, method, bindings }
  }

  function parseGroupBy(): GroupByStage {
    expect('PAREN_OPEN', 'group_by')
    const fields: string[] = []
    while (peek().type !== 'PAREN_CLOSE' && peek().type !== 'EOF') {
      if (fields.length > 0 && peek().type === 'COMMA') advance()
      const f = advance()
      if (f.type !== 'IDENT') throw new ParseError(`Expected field name in group_by, got "${f.value}"`, f.pos)
      fields.push(f.value)
    }
    expect('PAREN_CLOSE', 'group_by')
    return { type: 'group_by', fields }
  }

  function parseAggregate(): AggregateStage {
    expect('PAREN_OPEN', 'aggregate')
    const aggregations: AggregationExpr[] = []

    while (peek().type !== 'PAREN_CLOSE' && peek().type !== 'EOF') {
      if (aggregations.length > 0 && peek().type === 'COMMA') advance()

      const fn = advance()
      if (fn.type !== 'IDENT') throw new ParseError(`Expected aggregate function, got "${fn.value}"`, fn.pos)
      const fnName = fn.value.toLowerCase()
      if (!['count', 'sum', 'avg', 'min', 'max'].includes(fnName)) {
        throw new ParseError(`Unknown aggregate function: ${fnName}`, fn.pos)
      }

      expect('PAREN_OPEN', 'aggregate function')
      let field = '*'
      if (peek().type !== 'PAREN_CLOSE') {
        const f = advance()
        field = f.value
      }
      expect('PAREN_CLOSE', 'aggregate function')

      // Parse 'as alias'
      let alias = `${fnName}_${field}`
      if (peek().type === 'IDENT' && peek().value === 'as') {
        advance() // consume 'as'
        const a = expect('IDENT', 'aggregate alias')
        alias = a.value
      }

      aggregations.push({
        fn: fnName as AggregationExpr['fn'],
        field,
        alias,
      })
    }

    expect('PAREN_CLOSE', 'aggregate')
    return { type: 'aggregate', aggregations }
  }

  function parseBenchmark(): BenchmarkStage {
    expect('PAREN_OPEN', 'benchmark')
    let runs = 10
    let concurrency = 1

    while (peek().type !== 'PAREN_CLOSE' && peek().type !== 'EOF') {
      if (peek().type === 'COMMA') advance()
      const key = advance()
      if (key.type === 'IDENT') {
        if (key.value === 'runs') {
          expect('COLON', 'benchmark runs')
          const n = expect('NUMBER', 'benchmark runs count')
          runs = Math.min(Number(n.value), 100)
        } else if (key.value === 'concurrency') {
          expect('COLON', 'benchmark concurrency')
          const n = expect('NUMBER', 'benchmark concurrency count')
          concurrency = Math.min(Number(n.value), 20)
        }
      }
    }

    expect('PAREN_CLOSE', 'benchmark')
    return { type: 'benchmark', runs, concurrency }
  }
}

/* ── Error class ───────────────────────────────────────────────────── */

class ParseError extends Error {
  pos: number
  constructor(message: string, pos: number) {
    super(message)
    this.name = 'ParseError'
    this.pos = pos
  }
}

/* ── Syntax highlighting ───────────────────────────────────────────── */

const STAGE_KEYWORDS = new Set([
  'service', 'endpoint', 'where', 'select', 'order_by', 'limit', 'offset',
  'body', 'header', 'chain', 'group_by', 'aggregate', 'no_cache', 'benchmark',
])

const OPERATOR_KEYWORDS = new Set(['contains', 'startswith', 'and', 'or', 'not', 'as', 'asc', 'desc', 'bind'])
const AGG_KEYWORDS = new Set(['count', 'sum', 'avg', 'min', 'max'])
const METHOD_KEYWORDS = new Set(['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])

export function highlightApiQL(input: string): string {
  const tokens = tokenize(input)
  let result = ''
  let lastEnd = 0

  for (const token of tokens) {
    if (token.type === 'EOF') break

    // Preserve whitespace/comments between tokens
    if (token.pos > lastEnd) {
      result += escapeHtml(input.slice(lastEnd, token.pos))
    }

    const raw = getTokenRaw(input, token)
    const escaped = escapeHtml(raw)

    switch (token.type) {
      case 'IDENT':
        if (STAGE_KEYWORDS.has(token.value)) result += `<span class="kw">${escaped}</span>`
        else if (AGG_KEYWORDS.has(token.value)) result += `<span class="fn">${escaped}</span>`
        else if (OPERATOR_KEYWORDS.has(token.value)) result += `<span class="op">${escaped}</span>`
        else if (METHOD_KEYWORDS.has(token.value)) result += `<span class="fn">${escaped}</span>`
        else result += escaped
        break
      case 'STRING':
        result += `<span class="str">${escapeHtml(input.slice(token.pos, token.pos + raw.length + 2).charAt(0))}${escaped}${escapeHtml(input.slice(token.pos, token.pos + raw.length + 2).charAt(raw.length + 1))}</span>`
        break
      case 'NUMBER':
        result += `<span class="num">${escaped}</span>`
        break
      case 'BOOLEAN':
        result += `<span class="num">${escaped}</span>`
        break
      case 'PIPE':
        result += `<span class="op">${escaped}</span>`
        break
      case 'EQUALS': case 'NOT_EQUALS': case 'GT': case 'LT': case 'GTE': case 'LTE':
        result += `<span class="op">${escaped}</span>`
        break
      default:
        result += escaped
    }

    lastEnd = token.pos + getTokenLength(input, token)
  }

  // Trailing content
  if (lastEnd < input.length) {
    result += escapeHtml(input.slice(lastEnd))
  }

  return result
}

function getTokenRaw(input: string, token: Token): string {
  if (token.type === 'STRING') return token.value
  return token.value
}

function getTokenLength(input: string, token: Token): number {
  if (token.type === 'STRING') {
    // Account for quotes
    const quote = input[token.pos]
    const endIdx = input.indexOf(quote, token.pos + 1)
    return endIdx - token.pos + 1
  }
  return token.value.length
}

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

/* ── Cursor context (for autocomplete) ─────────────────────────────── */

export type CursorContext =
  | { type: 'service_name' }
  | { type: 'stage' }
  | { type: 'endpoint_path'; partial: string }
  | { type: 'method' }
  | { type: 'param_name'; endpoint?: string; partial: string }
  | { type: 'operator' }
  | { type: 'param_value'; param: string; endpoint?: string }
  | { type: 'field_name'; partial: string }
  | { type: 'aggregate_fn' }
  | { type: 'unknown' }

export function getCursorContext(input: string, cursorPos: number): CursorContext {
  const before = input.slice(0, cursorPos)

  // After a pipe with possible whitespace — suggest stage
  if (/\|\s*$/.test(before)) return { type: 'stage' }

  // Inside service("...")
  if (/service\s*\(\s*"[^"]*$/.test(before)) return { type: 'service_name' }

  // Inside endpoint("...")
  const epMatch = before.match(/endpoint\s*\(\s*"([^"]*)$/)
  if (epMatch) return { type: 'endpoint_path', partial: epMatch[1] }

  // After endpoint path, before close — suggest method
  if (/endpoint\s*\(\s*"[^"]*"\s*,\s*$/.test(before)) return { type: 'method' }

  // Inside where() — determine if param name, operator, or value
  const whereMatch = before.match(/where\s*\([^)]*$/)
  if (whereMatch) {
    const whereContent = whereMatch[0].replace(/^where\s*\(/, '')
    // After comma or start — suggest param name
    if (/,\s*$/.test(whereContent) || whereContent.trim() === '') {
      return { type: 'param_name', partial: '' }
    }
    // After field name — suggest operator
    if (/[a-zA-Z_][a-zA-Z0-9_.]*\s*$/.test(whereContent) && !/[=!<>]/.test(whereContent.slice(-5))) {
      return { type: 'operator' }
    }
    // After operator — suggest value
    const valMatch = whereContent.match(/([a-zA-Z_]\w*)\s*(?:=|!=|>=?|<=?|contains|startswith)\s*$/)
    if (valMatch) return { type: 'param_value', param: valMatch[1] }
    // Partial param name
    const partialMatch = whereContent.match(/(?:,\s*|^\s*)([a-zA-Z_]\w*)$/)
    if (partialMatch) return { type: 'param_name', partial: partialMatch[1] }
  }

  // Inside select(), group_by()
  const fieldStageMatch = before.match(/(?:select|group_by)\s*\([^)]*$/)
  if (fieldStageMatch) {
    const content = fieldStageMatch[0].replace(/^(?:select|group_by)\s*\(/, '')
    const partial = content.match(/(?:,\s*|^\s*)([a-zA-Z_]\w*)$/)?.[1] ?? ''
    return { type: 'field_name', partial }
  }

  // Inside aggregate()
  if (/aggregate\s*\([^)]*$/.test(before)) return { type: 'aggregate_fn' }

  // After start of line or newline — suggest stage
  if (/^\s*$/.test(before)) return { type: 'stage' }

  return { type: 'unknown' }
}
