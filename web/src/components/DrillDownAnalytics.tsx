'use client'

import { useState, useEffect, useCallback } from 'react'

// ── 型定義 ────────────────────────────────────────────────────────────────────

interface BetNode {
  bet_type:     string
  total_bets:   number
  hits:         number
  total_payout: number
  total_profit: number
  roi:          number
}
interface ModelNode extends BetNode { model_type: string; bet_types: BetNode[] }
interface DayNode   extends BetNode { date: string; day: string; models: ModelNode[] }
interface MonthNode extends BetNode { month: string; days: DayNode[] }
interface YearNode  extends BetNode { year: string; months: MonthNode[] }

interface AnalyticsData {
  overall: { total_bets: number; hits: number; total_payout: number; total_profit: number; roi: number }
  years:   YearNode[]
}

// ── スタイル定数 ──────────────────────────────────────────────────────────────

const AMBER   = '#F59E0B'
const AMBER_D = '#D97706'
const RED     = '#EF4444'
const GREEN   = '#22C55E'
const MUTED   = 'rgba(255,255,255,0.35)'
const BORDER  = 'rgba(245,158,11,0.15)'

const MODEL_COLOR: Record<string, string> = {
  '卍':    '#00C8FF',
  '本命':  '#FFD700',
  'Alpha': '#34D399',
  'WIN5':  '#FB923C',
}

function modelColor(name: string): string {
  for (const [k, v] of Object.entries(MODEL_COLOR)) {
    if (name.includes(k)) return v
  }
  return AMBER
}

// ── 数値フォーマット ──────────────────────────────────────────────────────────

const fmt = {
  yen:    (n: number) => `¥${Math.round(Math.abs(n)).toLocaleString()}`,
  pct:    (n: number) => `${n.toFixed(1)}%`,
  signed: (n: number) => (n >= 0 ? '+' : '−') + fmt.yen(n),
}

function profitStyle(profit: number) {
  return { color: profit >= 0 ? GREEN : RED }
}

// ── ProfitBar ─────────────────────────────────────────────────────────────────

function ProfitBar({ profit, maxAbs }: { profit: number; maxAbs: number }) {
  const pct = maxAbs > 0 ? Math.min(Math.abs(profit) / maxAbs, 1) * 100 : 0
  const color = profit >= 0 ? GREEN : RED
  const left  = profit < 0 ? 100 - pct : 50

  if (maxAbs === 0) return <div style={{ height: 4 }} />

  return (
    <div style={{
      position: 'relative', height: 4, background: 'rgba(255,255,255,0.06)',
      borderRadius: 2, overflow: 'hidden', marginTop: 2,
    }}>
      <div style={{
        position: 'absolute', top: 0, height: '100%',
        left: `${profit < 0 ? 100 - pct : 50}%`,
        width: `${profit >= 0 ? pct / 2 : pct / 2}%`,
        background: color,
        transition: 'width 0.4s ease',
      }} />
      <div style={{
        position: 'absolute', top: 0, left: '50%', width: 1, height: '100%',
        background: 'rgba(255,255,255,0.2)',
      }} />
    </div>
  )
  void left
}

// ── ExpandChevron ─────────────────────────────────────────────────────────────

function Chevron({ open }: { open: boolean }) {
  return (
    <span style={{
      display: 'inline-block',
      transform: open ? 'rotate(90deg)' : 'rotate(0deg)',
      transition: 'transform 0.2s ease',
      color: AMBER, fontSize: '0.7rem', marginRight: 4,
    }}>▶</span>
  )
}

// ── ROI バッジ ────────────────────────────────────────────────────────────────

function RoiBadge({ roi }: { roi: number }) {
  const bg    = roi >= 150 ? 'rgba(245,158,11,0.2)'
              : roi >= 100 ? 'rgba(34,197,94,0.15)'
              : 'rgba(239,68,68,0.12)'
  const color = roi >= 150 ? AMBER
              : roi >= 100 ? GREEN
              : RED
  return (
    <span style={{
      display: 'inline-block', background: bg, color, border: `1px solid ${color}33`,
      borderRadius: 3, padding: '1px 5px', fontSize: '0.65rem',
      fontFamily: 'monospace', fontWeight: 700, letterSpacing: '0.04em',
      whiteSpace: 'nowrap',
    }}>
      {fmt.pct(roi)}
    </span>
  )
}

// ── HitRate ───────────────────────────────────────────────────────────────────

function HitRate({ hits, total }: { hits: number; total: number }) {
  const pct = total > 0 ? (hits / total) * 100 : 0
  return (
    <span style={{ color: MUTED, fontSize: '0.65rem', fontFamily: 'monospace' }}>
      {hits}/{total} <span style={{ opacity: 0.6 }}>({pct.toFixed(0)}%)</span>
    </span>
  )
}

// ── BetTypeRow ────────────────────────────────────────────────────────────────

function BetTypeRow({ node }: { node: BetNode & { bet_type: string } }) {
  return (
    <div style={{
      display:        'grid',
      gridTemplateColumns: '5rem 1fr auto auto',
      gap:            '6px 8px',
      alignItems:     'center',
      padding:        '5px 10px 5px 24px',
      borderLeft:     `2px solid ${BORDER}`,
      margin:         '1px 0',
    }}>
      <span style={{
        color: 'rgba(255,255,255,0.55)', fontSize: '0.65rem',
        fontFamily: 'monospace', letterSpacing: '0.05em',
      }}>
        {node.bet_type}
      </span>
      <HitRate hits={node.hits} total={node.total_bets} />
      <span style={{ ...profitStyle(node.total_profit), fontSize: '0.65rem', fontFamily: 'monospace', textAlign: 'right' }}>
        {fmt.signed(node.total_profit)}
      </span>
      <RoiBadge roi={node.roi} />
    </div>
  )
}

// ── ModelRow ──────────────────────────────────────────────────────────────────

function ModelRow({
  node, maxAbs, defaultOpen,
}: { node: ModelNode; maxAbs: number; defaultOpen?: boolean }) {
  const [open, setOpen] = useState(defaultOpen ?? false)
  const color = modelColor(node.model_type)

  return (
    <div>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          width: '100%', textAlign: 'left', background: 'none', border: 'none',
          cursor: 'pointer', padding: '5px 10px 5px 16px',
          display: 'grid', gridTemplateColumns: '1fr auto auto auto',
          gap: '6px 8px', alignItems: 'center',
        }}
      >
        <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <Chevron open={open} />
          <span style={{
            color, fontSize: '0.7rem', fontWeight: 700,
            letterSpacing: '0.06em', textShadow: `0 0 8px ${color}66`,
          }}>
            {node.model_type}
          </span>
        </span>
        <HitRate hits={node.hits} total={node.total_bets} />
        <span style={{ ...profitStyle(node.total_profit), fontSize: '0.7rem', fontFamily: 'monospace', textAlign: 'right' }}>
          {fmt.signed(node.total_profit)}
        </span>
        <RoiBadge roi={node.roi} />
      </button>
      <ProfitBar profit={node.total_profit} maxAbs={maxAbs} />

      {open && (
        <div style={{ borderLeft: `2px solid ${color}33`, marginLeft: 8 }}>
          {node.bet_types.map(bt => (
            <BetTypeRow key={bt.bet_type} node={bt} />
          ))}
        </div>
      )}
    </div>
  )
}

// ── DayRow ────────────────────────────────────────────────────────────────────

function DayRow({ node, maxAbs }: { node: DayNode; maxAbs: number }) {
  const [open, setOpen] = useState(false)

  return (
    <div style={{ borderBottom: `1px solid ${BORDER}` }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          width: '100%', textAlign: 'left', background: open ? 'rgba(245,158,11,0.04)' : 'none',
          border: 'none', cursor: 'pointer', padding: '7px 10px',
          display: 'grid', gridTemplateColumns: '2.5rem 1fr auto auto auto',
          gap: '6px 8px', alignItems: 'center', transition: 'background 0.15s',
        }}
      >
        <span style={{
          color: AMBER, fontFamily: 'monospace', fontWeight: 700, fontSize: '0.85rem',
        }}>
          <Chevron open={open} />{node.day}日
        </span>
        <div>
          <HitRate hits={node.hits} total={node.total_bets} />
          <ProfitBar profit={node.total_profit} maxAbs={maxAbs} />
        </div>
        <span style={{ color: MUTED, fontSize: '0.6rem', fontFamily: 'monospace' }}>
          {fmt.yen(node.total_payout)}
        </span>
        <span style={{ ...profitStyle(node.total_profit), fontSize: '0.7rem', fontFamily: 'monospace', textAlign: 'right', minWidth: '5rem' }}>
          {fmt.signed(node.total_profit)}
        </span>
        <RoiBadge roi={node.roi} />
      </button>

      {open && (
        <div style={{
          background: 'rgba(0,0,0,0.3)', borderTop: `1px solid ${BORDER}`,
          paddingBottom: 4,
        }}>
          {node.models.map(m => (
            <ModelRow key={m.model_type} node={m}
              maxAbs={Math.max(...node.models.map(mm => Math.abs(mm.total_profit)), 1)} />
          ))}
        </div>
      )}
    </div>
  )
}

// ── MonthSection ──────────────────────────────────────────────────────────────

function MonthSection({ node, year }: { node: MonthNode; year: string }) {
  const [open, setOpen] = useState(false)
  const maxAbs = Math.max(...node.days.map(d => Math.abs(d.total_profit)), 1)

  const monthNames: Record<string, string> = {
    '01':'Jan','02':'Feb','03':'Mar','04':'Apr','05':'May','06':'Jun',
    '07':'Jul','08':'Aug','09':'Sep','10':'Oct','11':'Nov','12':'Dec',
  }

  return (
    <div style={{ marginBottom: 2 }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          width: '100%', textAlign: 'left', background: open
            ? 'rgba(245,158,11,0.08)' : 'rgba(245,158,11,0.03)',
          border: `1px solid ${open ? 'rgba(245,158,11,0.3)' : BORDER}`,
          borderRadius: 6, cursor: 'pointer', padding: '10px 14px',
          display: 'grid',
          gridTemplateColumns: '3.5rem 1fr auto auto',
          gap: '6px 10px', alignItems: 'center', transition: 'all 0.15s',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 3 }}>
          <Chevron open={open} />
          <span style={{ color: AMBER, fontFamily: 'monospace', fontWeight: 800, fontSize: '1.1rem' }}>
            {node.month}
          </span>
          <span style={{ color: MUTED, fontSize: '0.6rem' }}>{monthNames[node.month]}</span>
        </div>
        <div>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <HitRate hits={node.hits} total={node.total_bets} />
            <span style={{ color: MUTED, fontSize: '0.6rem' }}>{node.days.length}日開催</span>
          </div>
          <ProfitBar profit={node.total_profit} maxAbs={maxAbs} />
        </div>
        <div style={{ textAlign: 'right' }}>
          <div style={{ ...profitStyle(node.total_profit), fontFamily: 'monospace', fontSize: '0.85rem', fontWeight: 700 }}>
            {fmt.signed(node.total_profit)}
          </div>
          <div style={{ color: MUTED, fontSize: '0.6rem', fontFamily: 'monospace' }}>
            払戻 {fmt.yen(node.total_payout)}
          </div>
        </div>
        <RoiBadge roi={node.roi} />
      </button>

      {open && (
        <div style={{
          border: `1px solid ${BORDER}`, borderTop: 'none',
          borderRadius: '0 0 6px 6px',
          background: 'rgba(0,0,0,0.2)',
        }}>
          {node.days.map(d => (
            <DayRow key={d.date} node={d} maxAbs={maxAbs} />
          ))}
        </div>
      )}
    </div>
  )
}

// ── YearSection ───────────────────────────────────────────────────────────────

function YearSection({ node }: { node: YearNode }) {
  const [open, setOpen] = useState(true)

  return (
    <div style={{ marginBottom: 16 }}>
      {/* Year header */}
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          width: '100%', textAlign: 'left', border: 'none',
          background: 'transparent', cursor: 'pointer',
          padding: '0 0 8px 0',
          display: 'flex', alignItems: 'center', gap: 12,
          borderBottom: `2px solid ${AMBER}44`,
          marginBottom: 8,
        }}
      >
        <span style={{
          fontFamily: 'monospace', fontSize: '1.6rem', fontWeight: 900,
          color: AMBER, letterSpacing: '-0.02em',
          textShadow: `0 0 20px ${AMBER}88`,
        }}>
          {node.year}
        </span>
        <div style={{ flex: 1 }}>
          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
            <HitRate hits={node.hits} total={node.total_bets} />
            <RoiBadge roi={node.roi} />
            <span style={{ ...profitStyle(node.total_profit), fontFamily: 'monospace', fontSize: '0.85rem', fontWeight: 700 }}>
              {fmt.signed(node.total_profit)}
            </span>
          </div>
        </div>
        <Chevron open={open} />
      </button>

      {open && (
        <div style={{ paddingLeft: 4 }}>
          {node.months.map(m => (
            <MonthSection key={m.month} node={m} year={node.year} />
          ))}
        </div>
      )}
    </div>
  )
}

// ── OverallCard ───────────────────────────────────────────────────────────────

function OverallCard({ data }: { data: AnalyticsData['overall'] }) {
  const invested = data.total_payout - data.total_profit
  const hitRate  = data.total_bets > 0 ? (data.hits / data.total_bets * 100) : 0

  const metrics = [
    { label: '総投資',    value: fmt.yen(invested),      color: MUTED  },
    { label: '総払戻',    value: fmt.yen(data.total_payout), color: data.total_profit >= 0 ? GREEN : MUTED },
    { label: '損益',      value: fmt.signed(data.total_profit), color: data.total_profit >= 0 ? GREEN : RED },
    { label: 'ROI',       value: fmt.pct(data.roi),      color: data.roi >= 100 ? AMBER : RED },
    { label: '的中数',    value: `${data.hits.toLocaleString()}`,    color: AMBER },
    { label: '的中率',    value: `${hitRate.toFixed(1)}%`, color: MUTED },
  ]

  return (
    <div style={{
      background:   'linear-gradient(135deg, rgba(245,158,11,0.08) 0%, rgba(0,0,0,0.4) 100%)',
      border:       `1px solid rgba(245,158,11,0.3)`,
      borderRadius: 10, padding: '16px 20px', marginBottom: 20,
    }}>
      <div style={{
        color: AMBER, fontSize: '0.6rem', fontFamily: 'monospace', fontWeight: 700,
        letterSpacing: '0.2em', marginBottom: 12, opacity: 0.7,
      }}>
        ◆ CUMULATIVE PERFORMANCE
      </div>
      <div style={{
        display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)',
        gap: '10px 8px',
      }}>
        {metrics.map(m => (
          <div key={m.label}>
            <div style={{ color: MUTED, fontSize: '0.55rem', letterSpacing: '0.1em', marginBottom: 2 }}>
              {m.label}
            </div>
            <div style={{
              color: m.color, fontFamily: 'monospace', fontWeight: 700,
              fontSize: '0.85rem', letterSpacing: '-0.01em',
            }}>
              {m.value}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Today P&L ─────────────────────────────────────────────────────────────────

function TodayCard({ years }: { years: YearNode[] }) {
  const today = new Date().toISOString().slice(0, 10)

  let found: DayNode | null = null
  outer: for (const y of years) {
    for (const m of y.months) {
      for (const d of m.days) {
        if (d.date === today) { found = d; break outer }
      }
    }
  }

  if (!found) return null

  const invested = found.total_payout - found.total_profit
  const hitRate  = found.total_bets > 0 ? (found.hits / found.total_bets * 100) : 0

  return (
    <div style={{
      background:   'linear-gradient(135deg, rgba(34,197,94,0.06) 0%, rgba(0,0,0,0.5) 100%)',
      border:       `1px solid rgba(34,197,94,0.25)`,
      borderRadius: 10, padding: '14px 20px', marginBottom: 20,
    }}>
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        marginBottom: 10,
      }}>
        <span style={{
          color: GREEN, fontSize: '0.6rem', fontFamily: 'monospace',
          fontWeight: 700, letterSpacing: '0.2em',
        }}>◆ TODAY {today}</span>
        <span style={{
          background: 'rgba(34,197,94,0.15)', color: GREEN,
          border: '1px solid rgba(34,197,94,0.3)',
          borderRadius: 3, padding: '1px 6px', fontSize: '0.6rem',
          fontFamily: 'monospace',
        }}>LIVE</span>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '8px' }}>
        {[
          { label: '投資',  value: fmt.yen(invested), color: MUTED },
          { label: '払戻',  value: fmt.yen(found.total_payout), color: found.total_profit >= 0 ? GREEN : MUTED },
          { label: '損益',  value: fmt.signed(found.total_profit), color: found.total_profit >= 0 ? GREEN : RED },
          { label: '的中',  value: `${found.hits}件`, color: AMBER },
          { label: '的中率',value: `${hitRate.toFixed(0)}%`, color: MUTED },
          { label: 'ROI',   value: fmt.pct(found.roi), color: found.roi >= 100 ? AMBER : RED },
        ].map(m => (
          <div key={m.label}>
            <div style={{ color: MUTED, fontSize: '0.55rem', letterSpacing: '0.08em', marginBottom: 2 }}>
              {m.label}
            </div>
            <div style={{
              color: m.color, fontFamily: 'monospace', fontWeight: 700, fontSize: '0.8rem',
            }}>
              {m.value}
            </div>
          </div>
        ))}
      </div>

      {/* 的中トップ3 */}
      {found.models.length > 0 && (
        <div style={{ marginTop: 10, borderTop: `1px solid rgba(34,197,94,0.15)`, paddingTop: 8 }}>
          <div style={{ color: MUTED, fontSize: '0.55rem', letterSpacing: '0.1em', marginBottom: 6 }}>
            MODEL BREAKDOWN
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
            {found.models.filter(m => m.hits > 0).slice(0, 4).map(m => (
              <div key={m.model_type} style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '3px 0',
              }}>
                <span style={{
                  color: modelColor(m.model_type), fontSize: '0.65rem',
                  fontFamily: 'monospace', fontWeight: 600,
                }}>
                  {m.model_type}
                </span>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                  <HitRate hits={m.hits} total={m.total_bets} />
                  <span style={{ ...profitStyle(m.total_profit), fontSize: '0.65rem', fontFamily: 'monospace' }}>
                    {fmt.signed(m.total_profit)}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ── メインコンポーネント ──────────────────────────────────────────────────────

export default function DrillDownAnalytics() {
  const [data, setData]     = useState<AnalyticsData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState<string | null>(null)
  const [search,  setSearch]  = useState('')

  const load = useCallback(async () => {
    try {
      setLoading(true)
      const res = await fetch('/api/analytics')
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      setData(await res.json())
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  if (loading) {
    return (
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        minHeight: 300, color: AMBER,
      }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{
            fontFamily: 'monospace', fontSize: '0.7rem',
            letterSpacing: '0.3em', animation: 'pulse 1.5s infinite',
          }}>LOADING ANALYTICS...</div>
        </div>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div style={{ padding: 20, color: RED, fontFamily: 'monospace', fontSize: '0.8rem' }}>
        データ取得エラー: {error}
        <button onClick={load}
          style={{ marginLeft: 12, color: AMBER, background: 'none', border: `1px solid ${AMBER}`, borderRadius: 4, padding: '2px 8px', cursor: 'pointer' }}>
          再試行
        </button>
      </div>
    )
  }

  const filteredYears = search.trim()
    ? data.years.map(y => ({
        ...y,
        months: y.months.map(m => ({
          ...m,
          days: m.days.filter(d =>
            d.date.includes(search) ||
            d.models.some(mo => mo.model_type.includes(search))
          ),
        })).filter(m => m.days.length > 0),
      })).filter(y => y.months.length > 0)
    : data.years

  return (
    <div style={{ padding: '16px', width: '100%', minWidth: 0, boxSizing: 'border-box', overflowX: 'hidden' }}>
    <div style={{ maxWidth: 900, margin: '0 auto' }}>

      {/* ── ヘッダー ──────────────────────────────────── */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        marginBottom: 16, flexWrap: 'wrap', gap: 10,
      }}>
        <div>
          <h1 style={{
            color: AMBER, fontFamily: 'monospace', fontSize: '1.1rem',
            fontWeight: 900, letterSpacing: '0.15em', margin: 0,
            textShadow: `0 0 20px ${AMBER}66`,
          }}>
            ▸ ANALYTICS DRILL-DOWN
          </h1>
          <p style={{ color: MUTED, fontSize: '0.6rem', margin: '2px 0 0', letterSpacing: '0.1em' }}>
            年 › 月 › 日 › モデル › 券種 でタップ展開
          </p>
        </div>
        <button
          onClick={load}
          style={{
            background: 'rgba(245,158,11,0.1)', color: AMBER,
            border: `1px solid rgba(245,158,11,0.3)`, borderRadius: 5,
            padding: '5px 12px', cursor: 'pointer', fontSize: '0.65rem',
            fontFamily: 'monospace', letterSpacing: '0.1em',
          }}
        >↻ REFRESH</button>
      </div>

      {/* ── 全体サマリー ─────────────────────────────── */}
      <OverallCard data={data.overall} />

      {/* ── 本日 P&L ─────────────────────────────────── */}
      <TodayCard years={data.years} />

      {/* ── 検索フィルター ───────────────────────────── */}
      <div style={{ marginBottom: 14 }}>
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="日付 (例: 05-03) またはモデル名で絞り込み..."
          style={{
            width: '100%', background: 'rgba(0,0,0,0.4)',
            border: `1px solid ${BORDER}`,
            borderRadius: 6, padding: '7px 12px',
            color: 'var(--text-primary)', fontSize: '0.75rem',
            fontFamily: 'monospace', outline: 'none', boxSizing: 'border-box',
          }}
        />
      </div>

      {/* ── ドリルダウンツリー ───────────────────────── */}
      {filteredYears.length === 0 ? (
        <div style={{ color: MUTED, textAlign: 'center', padding: 40, fontFamily: 'monospace', fontSize: '0.75rem' }}>
          該当データなし
        </div>
      ) : (
        filteredYears.map(y => <YearSection key={y.year} node={y} />)
      )}

    </div>
    </div>
  )
}
