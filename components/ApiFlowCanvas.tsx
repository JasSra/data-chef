'use client'

import { useState, useRef, useCallback, useMemo, useEffect } from 'react'
import {
  X, Plus, Play, Trash2, ChevronDown, ChevronRight, Circle, Loader2,
} from 'lucide-react'

// ── Types ──────────────────────────────────────────────────────────────

interface ParamInfo {
  name: string; in: string; type: string; required: boolean
  description?: string; enum?: unknown[]; default?: unknown
}

interface EndpointInfo {
  path: string; method: string; summary?: string; description?: string
  tags: string[]; parameters: ParamInfo[]; deprecated: boolean
  operationId?: string
}

interface SchemaRegistry {
  endpoints: EndpointInfo[]
  tags: string[]
  allParamNames: string[]
  enumsByParam: Record<string, unknown[]>
  schemas: Record<string, unknown>
}

interface ServiceInfo {
  id: string
  name: string
}

interface FlowNode {
  id: string
  endpoint: EndpointInfo
  serviceId: string
  serviceName: string
  x: number
  y: number
  params: Record<string, string>
}

interface Connection {
  fromNodeId: string
  toNodeId: string
  fromField: string
  toParam: string
}

interface ApiFlowCanvasProps {
  services: ServiceInfo[]
  initialServiceId?: string
  onQueryGenerated: (query: string) => void
  onClose: () => void
}

// ── Helpers ────────────────────────────────────────────────────────────

const METHOD_COLORS: Record<string, { bg: string; text: string }> = {
  GET:    { bg: 'bg-emerald-500/20', text: 'text-emerald-400' },
  POST:   { bg: 'bg-blue-500/20',    text: 'text-blue-400' },
  PUT:    { bg: 'bg-amber-500/20',   text: 'text-amber-400' },
  DELETE: { bg: 'bg-rose-500/20',    text: 'text-rose-400' },
  PATCH:  { bg: 'bg-purple-500/20',  text: 'text-purple-400' },
}

function methodColor(m: string) {
  return METHOD_COLORS[m.toUpperCase()] ?? { bg: 'bg-gray-500/20', text: 'text-gray-400' }
}

let _nodeId = 0
function nextId() { return `node-${++_nodeId}-${Date.now()}` }

const NODE_W = 280

// ── Component ──────────────────────────────────────────────────────────

export default function ApiFlowCanvas({
  services,
  initialServiceId,
  onQueryGenerated,
  onClose,
}: ApiFlowCanvasProps) {
  const [nodes, setNodes] = useState<FlowNode[]>([])
  const [connections, setConnections] = useState<Connection[]>([])
  const [addMenuOpen, setAddMenuOpen] = useState(false)
  const [expandedServices, setExpandedServices] = useState<Set<string>>(
    new Set(initialServiceId ? [initialServiceId] : [])
  )
  const [expandedTags, setExpandedTags] = useState<Set<string>>(new Set())
  const [expandedParams, setExpandedParams] = useState<Set<string>>(new Set())
  const [editingParam, setEditingParam] = useState<{ nodeId: string; param: string } | null>(null)

  // Per-service schema cache
  const [schemas, setSchemas] = useState<Record<string, SchemaRegistry | null>>({})
  const [loadingSchemas, setLoadingSchemas] = useState<Set<string>>(new Set())

  // Load a service schema if not already cached
  const loadSchema = useCallback(async (serviceId: string) => {
    if (schemas[serviceId] !== undefined || loadingSchemas.has(serviceId)) return
    setLoadingSchemas(prev => new Set(prev).add(serviceId))
    try {
      const res = await fetch(`/api/api-services/${serviceId}/schema`)
      const data = res.ok ? await res.json() : null
      setSchemas(prev => ({ ...prev, [serviceId]: data }))
    } catch {
      setSchemas(prev => ({ ...prev, [serviceId]: null }))
    } finally {
      setLoadingSchemas(prev => { const s = new Set(prev); s.delete(serviceId); return s })
    }
  }, [schemas, loadingSchemas])

  // Auto-load initial service schema
  useEffect(() => {
    if (initialServiceId) loadSchema(initialServiceId)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialServiceId])

  // Dragging nodes
  const canvasRef = useRef<HTMLDivElement>(null)
  const dragRef = useRef<{
    nodeId: string; offsetX: number; offsetY: number
  } | null>(null)

  // Dragging wires
  const wireRef = useRef<{
    fromNodeId: string; fromField: string; mx: number; my: number
  } | null>(null)
  const [wireDraft, setWireDraft] = useState<{
    fromNodeId: string; fromField: string; mx: number; my: number
  } | null>(null)

  // ── Node CRUD ──────────────────────────────────────────────────────

  const addNode = useCallback((ep: EndpointInfo, serviceId: string, serviceName: string) => {
    const id = nextId()
    const x = 60 + (nodes.length % 4) * 320
    const y = 60 + Math.floor(nodes.length / 4) * 220
    setNodes(prev => [...prev, { id, endpoint: ep, serviceId, serviceName, x, y, params: {} }])
    setAddMenuOpen(false)
  }, [nodes.length])

  const removeNode = useCallback((id: string) => {
    setNodes(prev => prev.filter(n => n.id !== id))
    setConnections(prev => prev.filter(c => c.fromNodeId !== id && c.toNodeId !== id))
  }, [])

  const setParam = useCallback((nodeId: string, param: string, value: string) => {
    setNodes(prev => prev.map(n =>
      n.id === nodeId ? { ...n, params: { ...n.params, [param]: value } } : n,
    ))
  }, [])

  // ── Node dragging ──────────────────────────────────────────────────

  const onNodeMouseDown = useCallback((e: React.MouseEvent, nodeId: string) => {
    e.stopPropagation()
    const node = nodes.find(n => n.id === nodeId)
    if (!node || !canvasRef.current) return
    const rect = canvasRef.current.getBoundingClientRect()
    dragRef.current = {
      nodeId,
      offsetX: e.clientX - rect.left - node.x,
      offsetY: e.clientY - rect.top - node.y,
    }
  }, [nodes])

  const onCanvasMouseMove = useCallback((e: React.MouseEvent) => {
    if (!canvasRef.current) return
    const rect = canvasRef.current.getBoundingClientRect()

    if (dragRef.current) {
      const { nodeId, offsetX, offsetY } = dragRef.current
      const nx = e.clientX - rect.left - offsetX
      const ny = e.clientY - rect.top - offsetY
      setNodes(prev => prev.map(n =>
        n.id === nodeId ? { ...n, x: Math.max(0, nx), y: Math.max(0, ny) } : n,
      ))
    }

    if (wireRef.current) {
      wireRef.current.mx = e.clientX - rect.left
      wireRef.current.my = e.clientY - rect.top
      setWireDraft({ ...wireRef.current })
    }
  }, [])

  const onCanvasMouseUp = useCallback(() => {
    dragRef.current = null
    wireRef.current = null
    setWireDraft(null)
  }, [])

  // ── Wire dragging ──────────────────────────────────────────────────

  const onOutputPortMouseDown = useCallback((e: React.MouseEvent, nodeId: string, field: string) => {
    e.stopPropagation()
    if (!canvasRef.current) return
    const rect = canvasRef.current.getBoundingClientRect()
    wireRef.current = {
      fromNodeId: nodeId,
      fromField: field,
      mx: e.clientX - rect.left,
      my: e.clientY - rect.top,
    }
    setWireDraft({ ...wireRef.current })
  }, [])

  const onInputPortMouseUp = useCallback((e: React.MouseEvent, nodeId: string, param: string) => {
    e.stopPropagation()
    if (!wireRef.current) return
    const { fromNodeId, fromField } = wireRef.current
    if (fromNodeId === nodeId) { wireRef.current = null; setWireDraft(null); return }
    // Avoid duplicates
    setConnections(prev => {
      if (prev.some(c => c.fromNodeId === fromNodeId && c.toNodeId === nodeId
        && c.fromField === fromField && c.toParam === param)) return prev
      return [...prev, { fromNodeId, toNodeId: nodeId, fromField, toParam: param }]
    })
    // Mark param as bound
    setParam(nodeId, param, `bind:${fromField}`)
    wireRef.current = null
    setWireDraft(null)
  }, [setParam])

  const removeConnection = useCallback((idx: number) => {
    const conn = connections[idx]
    if (conn) {
      setParam(conn.toNodeId, conn.toParam, '')
    }
    setConnections(prev => prev.filter((_, i) => i !== idx))
  }, [connections, setParam])

  // ── Port positions ─────────────────────────────────────────────────

  const getOutputPortPos = useCallback((node: FlowNode, _field: string) => {
    return { x: node.x + NODE_W, y: node.y + 28 }
  }, [])

  const getInputPortPos = useCallback((node: FlowNode, _param: string) => {
    return { x: node.x, y: node.y + 28 }
  }, [])

  // ── Query generation ──────────────────────────────────────────────

  const generateQuery = useCallback(() => {
    if (nodes.length === 0) return

    // Topological sort
    const hasIncoming = new Set(connections.map(c => c.toNodeId))
    const roots = nodes.filter(n => !hasIncoming.has(n.id))
    const visited = new Set<string>()
    const ordered: FlowNode[] = []

    function bfs(start: FlowNode[]) {
      const queue = [...start]
      while (queue.length) {
        const node = queue.shift()!
        if (visited.has(node.id)) continue
        visited.add(node.id)
        ordered.push(node)
        const children = connections
          .filter(c => c.fromNodeId === node.id)
          .map(c => nodes.find(n => n.id === c.toNodeId)!)
          .filter(Boolean)
        queue.push(...children)
      }
    }
    bfs(roots.length ? roots : [nodes[0]])
    for (const n of nodes) {
      if (!visited.has(n.id)) ordered.push(n)
    }

    const parts: string[] = []
    let lastServiceId = ''

    for (let i = 0; i < ordered.length; i++) {
      const node = ordered[i]
      const method = node.endpoint.method.toUpperCase()
      const incomingConns = connections.filter(c => c.toNodeId === node.id)

      // Emit service() when service changes
      if (node.serviceId !== lastServiceId) {
        parts.push(`service("${node.serviceName}")`)
        lastServiceId = node.serviceId
      }

      if (i === 0 || incomingConns.length === 0) {
        parts.push(`endpoint("${node.endpoint.path}"${method !== 'GET' ? `, ${method}` : ''})`)
      } else {
        const bindings = incomingConns
          .map(c => `${c.toParam} = ${c.fromField}`)
          .join(', ')
        parts.push(`chain("${node.endpoint.path}", bind: ${bindings})`)
      }

      // Non-bound params
      const plainParams = Object.entries(node.params)
        .filter(([, v]) => v && !v.startsWith('bind:'))
      if (plainParams.length) {
        const conds = plainParams.map(([k, v]) => `${k} = ${v}`).join(', ')
        parts.push(`where(${conds})`)
      }
    }

    onQueryGenerated(parts.join('\n| '))
  }, [nodes, connections, onQueryGenerated])

  // ── Endpoint grouping per service ──────────────────────────────────

  const endpointsByTag = useMemo(() => {
    const result: Record<string, Record<string, EndpointInfo[]>> = {}
    for (const svc of services) {
      const schema = schemas[svc.id]
      if (!schema) continue
      const map: Record<string, EndpointInfo[]> = {}
      for (const ep of schema.endpoints) {
        const tag = ep.tags[0] ?? 'Other'
        ;(map[tag] ??= []).push(ep)
      }
      result[svc.id] = map
    }
    return result
  }, [services, schemas])

  // ── Render helpers ─────────────────────────────────────────────────

  function nodeStatus(node: FlowNode) {
    return connections.some(c => c.toNodeId === node.id) || connections.some(c => c.fromNodeId === node.id)
  }

  function filledParamCount(node: FlowNode) {
    return Object.values(node.params).filter(v => v.length > 0).length
  }

  // ── SVG for connections ────────────────────────────────────────────

  function renderConnections() {
    return (
      <svg className="absolute inset-0 w-full h-full pointer-events-none" style={{ zIndex: 1 }}>
        {connections.map((conn, idx) => {
          const fromNode = nodes.find(n => n.id === conn.fromNodeId)
          const toNode = nodes.find(n => n.id === conn.toNodeId)
          if (!fromNode || !toNode) return null
          const p1 = getOutputPortPos(fromNode, conn.fromField)
          const p2 = getInputPortPos(toNode, conn.toParam)
          const cx = (p1.x + p2.x) / 2
          const d = `M ${p1.x} ${p1.y} Q ${cx} ${p1.y}, ${(p1.x + p2.x) / 2} ${(p1.y + p2.y) / 2} Q ${cx} ${p2.y}, ${p2.x} ${p2.y}`
          return (
            <g key={idx} className="pointer-events-auto cursor-pointer" onClick={() => removeConnection(idx)}>
              <path d={d} fill="none" stroke="transparent" strokeWidth={12} />
              <path
                d={d} fill="none"
                stroke="rgb(99 102 241 / 0.5)" strokeWidth={2}
                className="hover:stroke-indigo-400 transition-colors"
              />
            </g>
          )
        })}
        {wireDraft && (() => {
          const fromNode = nodes.find(n => n.id === wireDraft.fromNodeId)
          if (!fromNode) return null
          const p1 = getOutputPortPos(fromNode, wireDraft.fromField)
          const cx = (p1.x + wireDraft.mx) / 2
          const d = `M ${p1.x} ${p1.y} Q ${cx} ${p1.y}, ${(p1.x + wireDraft.mx) / 2} ${(p1.y + wireDraft.my) / 2} Q ${cx} ${wireDraft.my}, ${wireDraft.mx} ${wireDraft.my}`
          return <path d={d} fill="none" stroke="rgb(99 102 241 / 0.3)" strokeWidth={2} strokeDasharray="6 4" />
        })()}
      </svg>
    )
  }

  // ── Render node ────────────────────────────────────────────────────

  function renderNode(node: FlowNode) {
    const mc = methodColor(node.endpoint.method)
    const paramsExpanded = expandedParams.has(node.id)
    const connected = nodeStatus(node)
    const filled = filledParamCount(node)
    const total = node.endpoint.parameters.length

    return (
      <div
        key={node.id}
        className="absolute select-none"
        style={{ left: node.x, top: node.y, width: NODE_W, zIndex: 2 }}
        onMouseDown={e => onNodeMouseDown(e, node.id)}
      >
        <div className="bg-chef-card border border-chef-border rounded-lg shadow-lg overflow-hidden">
          {/* Service label */}
          <div className="flex items-center gap-1 px-3 pt-1.5 pb-0">
            <span className="text-[9px] text-chef-muted/60 font-medium uppercase tracking-widest truncate">
              {node.serviceName}
            </span>
          </div>

          {/* Header */}
          <div className="flex items-center gap-2 px-3 py-2 border-b border-chef-border relative">
            {/* Input port */}
            <div
              className="absolute -left-[7px] top-[22px] w-[14px] h-[14px] rounded-full bg-chef-surface border-2 border-indigo-500 cursor-crosshair z-10 hover:bg-indigo-500 transition-colors"
              onMouseUp={e => onInputPortMouseUp(e, node.id, node.endpoint.parameters[0]?.name ?? 'input')}
            />
            <span className={`px-1.5 py-0.5 rounded text-[10px] font-bold uppercase ${mc.bg} ${mc.text}`}>
              {node.endpoint.method}
            </span>
            <span className="text-chef-text text-xs font-mono truncate flex-1">
              {node.endpoint.path}
            </span>
            {/* Status dot */}
            <Circle
              size={8}
              className={connected ? 'fill-emerald-500 text-emerald-500' : 'fill-gray-500 text-gray-500'}
            />
            {/* Output port */}
            <div
              className="absolute -right-[7px] top-[22px] w-[14px] h-[14px] rounded-full bg-chef-surface border-2 border-indigo-500 cursor-crosshair z-10 hover:bg-indigo-500 transition-colors"
              onMouseDown={e => onOutputPortMouseDown(e, node.id, node.endpoint.operationId ?? node.endpoint.path)}
            />
            {/* Remove */}
            <button
              className="text-chef-muted hover:text-rose-400 transition-colors ml-1"
              onClick={e => { e.stopPropagation(); removeNode(node.id) }}
            >
              <X size={14} />
            </button>
          </div>

          {/* Summary */}
          {node.endpoint.summary && (
            <p className="px-3 py-1 text-[11px] text-chef-muted truncate">
              {node.endpoint.summary}
            </p>
          )}

          {/* Param count + toggle */}
          {total > 0 && (
            <button
              className="flex items-center gap-1 px-3 py-1 text-[11px] text-chef-muted hover:text-chef-text w-full text-left transition-colors"
              onClick={e => {
                e.stopPropagation()
                setExpandedParams(prev => {
                  const next = new Set(prev)
                  next.has(node.id) ? next.delete(node.id) : next.add(node.id)
                  return next
                })
              }}
            >
              {paramsExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
              Parameters ({filled}/{total})
            </button>
          )}

          {/* Parameter list */}
          {paramsExpanded && node.endpoint.parameters.map(p => (
            <div
              key={p.name}
              className="flex items-center gap-1 px-3 py-1 text-[11px] border-t border-chef-border/50"
            >
              <span className={`${p.required ? 'text-chef-text' : 'text-chef-muted'} min-w-[70px] truncate`}>
                {p.name}{p.required && <span className="text-rose-400">*</span>}
              </span>
              <span className="text-chef-muted text-[10px]">{p.type}</span>
              <div className="flex-1" />
              {editingParam?.nodeId === node.id && editingParam.param === p.name ? (
                p.enum && p.enum.length > 0 ? (
                  <select
                    className="bg-chef-surface text-chef-text text-[11px] rounded px-1 py-0.5 border border-chef-border outline-none w-24"
                    value={node.params[p.name] ?? ''}
                    autoFocus
                    onChange={e => { setParam(node.id, p.name, e.target.value); setEditingParam(null) }}
                    onBlur={() => setEditingParam(null)}
                  >
                    <option value="">--</option>
                    {p.enum.map((v, i) => <option key={i} value={String(v)}>{String(v)}</option>)}
                  </select>
                ) : (
                  <input
                    className="bg-chef-surface text-chef-text text-[11px] rounded px-1 py-0.5 border border-chef-border outline-none w-24"
                    value={node.params[p.name] ?? ''}
                    autoFocus
                    onChange={e => setParam(node.id, p.name, e.target.value)}
                    onBlur={() => setEditingParam(null)}
                    onKeyDown={e => { if (e.key === 'Enter') setEditingParam(null) }}
                  />
                )
              ) : (
                <button
                  className="text-[11px] text-indigo-400 hover:text-indigo-300 truncate max-w-[90px] text-right transition-colors"
                  onClick={e => { e.stopPropagation(); setEditingParam({ nodeId: node.id, param: p.name }) }}
                >
                  {node.params[p.name] || '(set)'}
                </button>
              )}
            </div>
          ))}
        </div>
      </div>
    )
  }

  // ── Main render ────────────────────────────────────────────────────

  return (
    <div className="flex flex-col h-full bg-chef-surface rounded-lg border border-chef-border overflow-hidden">
      {/* Toolbar */}
      <div className="flex items-center gap-2 px-3 py-2 border-b border-chef-border bg-chef-card shrink-0">
        {/* Add Node */}
        <div className="relative">
          <button
            onClick={() => setAddMenuOpen(v => !v)}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium bg-indigo-500/20 text-indigo-400 hover:bg-indigo-500/30 transition-colors"
          >
            <Plus size={14} /> Add Node <ChevronDown size={12} />
          </button>

          {addMenuOpen && (
            <div className="absolute top-full left-0 mt-1 w-80 max-h-96 overflow-y-auto bg-chef-card border border-chef-border rounded-lg shadow-xl z-50">
              {services.length === 0 && (
                <div className="px-3 py-4 text-xs text-chef-muted text-center">No services added yet</div>
              )}
              {services.map(svc => {
                const isExpanded = expandedServices.has(svc.id)
                const isLoading = loadingSchemas.has(svc.id)
                const tagMap = endpointsByTag[svc.id] ?? {}
                const totalEndpoints = Object.values(tagMap).reduce((sum, eps) => sum + eps.length, 0)

                return (
                  <div key={svc.id}>
                    {/* Service header */}
                    <button
                      className="flex items-center gap-1.5 w-full px-3 py-2 text-xs font-semibold text-chef-text hover:bg-chef-surface/50 transition-colors border-b border-chef-border/30"
                      onClick={() => {
                        setExpandedServices(prev => {
                          const next = new Set(prev)
                          if (next.has(svc.id)) { next.delete(svc.id) } else {
                            next.add(svc.id)
                            loadSchema(svc.id)
                          }
                          return next
                        })
                      }}
                    >
                      {isExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                      <span className="flex-1 text-left truncate">{svc.name}</span>
                      {isLoading && <Loader2 size={10} className="animate-spin text-chef-muted" />}
                      {!isLoading && isExpanded && totalEndpoints > 0 && (
                        <span className="text-[10px] text-chef-muted">{totalEndpoints}</span>
                      )}
                    </button>

                    {/* Tag groups */}
                    {isExpanded && !isLoading && Object.entries(tagMap).map(([tag, eps]) => (
                      <div key={tag}>
                        <button
                          className="flex items-center gap-1 w-full px-5 py-1.5 text-[11px] font-medium text-chef-muted hover:bg-chef-surface/50 transition-colors"
                          onClick={() => setExpandedTags(prev => {
                            const next = new Set(prev)
                            next.has(`${svc.id}:${tag}`) ? next.delete(`${svc.id}:${tag}`) : next.add(`${svc.id}:${tag}`)
                            return next
                          })}
                        >
                          {expandedTags.has(`${svc.id}:${tag}`) ? <ChevronDown size={10} /> : <ChevronRight size={10} />}
                          {tag} ({eps.length})
                        </button>
                        {expandedTags.has(`${svc.id}:${tag}`) && eps.map(ep => {
                          const mc = methodColor(ep.method)
                          return (
                            <button
                              key={`${ep.method}-${ep.path}`}
                              onClick={() => addNode(ep, svc.id, svc.name)}
                              className="flex items-center gap-2 w-full px-7 py-1.5 text-xs hover:bg-indigo-500/10 transition-colors"
                            >
                              <span className={`px-1 py-0.5 rounded text-[10px] font-bold uppercase ${mc.bg} ${mc.text}`}>
                                {ep.method}
                              </span>
                              <span className="text-chef-text truncate font-mono text-[11px]">{ep.path}</span>
                            </button>
                          )
                        })}
                      </div>
                    ))}

                    {isExpanded && !isLoading && totalEndpoints === 0 && (
                      <div className="px-5 py-2 text-[11px] text-chef-muted">No endpoints loaded</div>
                    )}

                    {isExpanded && isLoading && (
                      <div className="px-5 py-3 text-[11px] text-chef-muted flex items-center gap-2">
                        <Loader2 size={10} className="animate-spin" />
                        Loading endpoints…
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          )}
        </div>

        {/* Generate */}
        <button
          onClick={generateQuery}
          disabled={nodes.length === 0}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium bg-emerald-500/20 text-emerald-400 hover:bg-emerald-500/30 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        >
          <Play size={14} /> Generate Query
        </button>

        {/* Clear */}
        <button
          onClick={() => { setNodes([]); setConnections([]) }}
          disabled={nodes.length === 0}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium text-chef-muted hover:text-rose-400 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        >
          <Trash2 size={14} /> Clear
        </button>

        <div className="flex-1" />

        <span className="text-[11px] text-chef-muted">
          {nodes.length} node{nodes.length !== 1 ? 's' : ''} &middot; {connections.length} connection{connections.length !== 1 ? 's' : ''}
        </span>

        {/* Close */}
        <button
          onClick={onClose}
          className="text-chef-muted hover:text-chef-text transition-colors"
        >
          <X size={16} />
        </button>
      </div>

      {/* Canvas */}
      <div
        ref={canvasRef}
        className="relative flex-1 overflow-auto cursor-default"
        style={{
          backgroundImage:
            'radial-gradient(circle, rgb(99 102 241 / 0.08) 1px, transparent 1px)',
          backgroundSize: '24px 24px',
        }}
        onMouseMove={onCanvasMouseMove}
        onMouseUp={onCanvasMouseUp}
        onMouseLeave={onCanvasMouseUp}
        onClick={() => { setAddMenuOpen(false); setEditingParam(null) }}
      >
        {renderConnections()}
        {nodes.map(renderNode)}

        {/* Empty state */}
        {nodes.length === 0 && (
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="text-center text-chef-muted">
              <p className="text-sm mb-1">No nodes on the canvas</p>
              <p className="text-xs">Click &quot;Add Node&quot; to place endpoint nodes from any service, then wire them together.</p>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
