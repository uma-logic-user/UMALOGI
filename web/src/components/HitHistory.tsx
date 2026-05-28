'use client'

import { useState, useMemo } from 'react'
import type { Prediction } from '@/types/race'

interface Props {
  predictions: Prediction[]
}

type SortKey = 'date' | 'roi' | 'payout'
type Filter  = 'all' | 'jackpot' | 'big' | 'normal'

function rowClass(roi: number): string {
  if (roi >= 500) return 'hit-row-jackpot'
  if (roi >= 200) return 'hit-row-big'
  return 'hit-row-normal'
}

function roiBadgeClass(roi: number): string {
  if (roi >= 500) return 'roi-badge roi-badge-jackpot'
  if (roi >= 200) return 'roi-badge roi-badge-big'
  return 'roi-badge roi-badge-normal'
}

function formatPayout(p: number | null): string {
  if (p == null) return '—'
  return `¥${Math.round(p).toLocaleString()}`
}

const ORDERED_BET_TYPES = new Set(['馬単', '三連単'])

/** combination_json + horse_num_to_name から買い目を整形する。
 *  display: 馬番のみコンパクト表示（セル内）
 *  full:    馬名付き表示（title / ホバー）
 */
function formatCombinations(pred: Prediction): { display: string; full: string } {
  const empty = { display: '—', full: '—' }
  if (!pred.combination_json) return empty

  let raw: unknown
  try { raw = JSON.parse(pred.combination_json) } catch { return empty }
  if (!Array.isArray(raw) || raw.length === 0) return empty

  const combos: number[][] = Array.isArray(raw[0])
    ? (raw as number[][])
    : [(raw as number[])]

  const nameMap: Record<string, string> = pred.horse_num_to_name ?? {}
  const isOrdered = ORDERED_BET_TYPES.has(pred.bet_type ?? '')
  const sep = isOrdered ? '→' : '-'

  const display = combos.map(c => c.join(sep)).join(' / ')
  const full    = combos.map(c =>
    c.map(n => {
      const name = nameMap[String(n)]
      return name ? `${n}(${name})` : String(n)
    }).join(sep)
  ).join('\n')

  return { display, full }
}

export default function HitHistory({ predictions }: Props) {
  const [sortKey, setSortKey] = useState<SortKey>('date')
  const [sortAsc, setSortAsc] = useState(false)
  const [filter,  setFilter]  = useState<Filter>('all')
  const [modelFilter, setModelFilter] = useState<string>('all')

  const hits = useMemo(
    () => predictions.filter(p => p.is_hit === 1),
    [predictions],
  )

  const jackpots = hits.filter(p => (p.roi ?? 0) >= 500).length
  const bigs     = hits.filter(p => (p.roi ?? 0) >= 200 && (p.roi ?? 0) < 500).length
  const normals  = hits.filter(p => (p.roi ?? 0) < 200).length

  const filtered = useMemo(() => {
    let data = hits
    if (filter === 'jackpot') data = data.filter(p => (p.roi ?? 0) >= 500)
    else if (filter === 'big')    data = data.filter(p => (p.roi ?? 0) >= 200)
    else if (filter === 'normal') data = data.filter(p => (p.roi ?? 0) < 200)
    if (modelFilter !== 'all') {
      data = data.filter(p => (p.model_type ?? '').includes(modelFilter))
    }
    return data
  }, [hits, filter, modelFilter])

  const sorted = useMemo(() => {
    const arr = [...filtered]
    arr.sort((a, b) => {
      let va: number, vb: number
      if (sortKey === 'roi')    { va = a.roi    ?? 0; vb = b.roi    ?? 0 }
      else if (sortKey === 'payout') { va = a.payout ?? 0; vb = b.payout ?? 0 }
      else                          { va = a.date.localeCompare(b.date); vb = 0 }
      if (sortKey === 'date') return sortAsc ? va : -va
      return sortAsc ? va - vb : vb - va
    })
    return arr
  }, [filtered, sortKey, sortAsc])

  function SortTh({ col, label }: { col: SortKey; label: string }) {
    const active = sortKey === col
    return (
      <th
        className="cursor-pointer select-none text-left"
        onClick={() => {
          if (col === sortKey) setSortAsc(a => !a)
          else { setSortKey(col); setSortAsc(false) }
        }}
        style={{ color: active ? 'var(--neon-gold)' : undefined }}
      >
        <span className="flex items-center gap-1">
          {label}
          {active && <span className="text-xs">{sortAsc ? '▲' : '▼'}</span>}
        </span>
      </th>
    )
  }

  if (hits.length === 0) {
    return (
      <div className="flex items-center justify-center h-full min-h-[400px]">
        <div className="text-center">
          <div className="text-5xl mb-4 opacity-20">★</div>
          <div className="neon-text-gold text-lg tracking-[0.3em]">NO HITS YET</div>
          <div className="text-sm text-[var(--text-muted)] mt-2">的中実績がまだありません</div>
        </div>
      </div>
    )
  }

  return (
    <div className="p-4 space-y-4 w-full min-w-0">

      {/* ── ヘッダー ──────────────────────────────────────── */}
      <div className="flex items-center gap-3 flex-wrap">
        <h1
          className="text-xl font-bold tracking-wider"
          style={{
            color: 'var(--neon-gold)',
            textShadow: 'none',
          }}
        >
          ★ 的中実績 HIT HISTORY
        </h1>
        <span className="text-sm text-[var(--text-muted)]">
          総的中 <span className="neon-text-gold font-bold text-base">{hits.length}</span> 件
        </span>
      </div>

      {/* ── サマリーカード ──────────────────────────────── */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard
          label="総的中数"
          value={hits.length}
          sub="All hits"
          color="var(--neon-cyan)"
        />
        <StatCard
          label="高配当 (ROI≥200%)"
          value={bigs + jackpots}
          sub="High value"
          color="var(--neon-gold)"
        />
        <StatCard
          label="万馬券級 (ROI≥500%)"
          value={jackpots}
          sub="Jackpot"
          color="#ffaa00"
          glow
        />
        <StatCard
          label="最高ROI"
          value={`${Math.max(...hits.map(p => p.roi ?? 0)).toFixed(0)}%`}
          sub="Best return"
          color="var(--neon-green)"
        />
      </div>

      {/* ── フィルター ─────────────────────────────────── */}
      <div className="flex flex-wrap gap-2">
        {[
          { key: 'all' as Filter,     label: 'すべて',            count: hits.length },
          { key: 'jackpot' as Filter, label: '万馬券級 ROI≥500%', count: jackpots },
          { key: 'big' as Filter,     label: '高配当 ROI≥200%',   count: bigs },
          { key: 'normal' as Filter,  label: '的中 ROI<200%',     count: normals },
        ].map(f => (
          <button
            key={f.key}
            onClick={() => setFilter(f.key)}
            className="px-3 py-1.5 rounded text-xs font-semibold tracking-wider transition-all"
            style={{
              background: filter === f.key
                ? f.key === 'jackpot' ? 'rgba(255,140,0,0.2)' : 'rgba(184,134,11,0.15)'
                : 'rgba(200,168,130,0.04)',
              color: filter === f.key
                ? f.key === 'jackpot' ? '#ffaa00' : 'var(--neon-gold)'
                : 'var(--text-muted)',
              border: `1px solid ${filter === f.key
                ? f.key === 'jackpot' ? 'rgba(255,140,0,0.5)' : 'rgba(184,134,11,0.4)'
                : 'rgba(200,168,130,0.1)'}`,
            }}
          >
            {f.label}
            <span className="ml-1.5 opacity-70">{f.count}</span>
          </button>
        ))}

        {/* モデルフィルター */}
        <div className="flex gap-1.5 flex-wrap">
          {[
            { key: 'all',   label: '全モデル', cyan: false },
            { key: '卍',    label: '⚡卍 (全)',  cyan: true  },
            { key: '卍V2',  label: '⚡卍V2',     cyan: true  },
            { key: '本命',  label: '🎯本命 (全)', cyan: false },
            { key: '本命V2',label: '🎯本命V2',   cyan: false },
            { key: 'Alpha', label: 'Alpha',      cyan: false },
          ].map(({ key, label, cyan }) => (
            <button
              key={key}
              onClick={() => setModelFilter(key)}
              className="px-2 py-1 sm:px-3 sm:py-1.5 rounded text-xs font-semibold transition-all"
              style={{
                background: modelFilter === key
                  ? (cyan ? 'rgba(200,168,130,0.12)' : 'rgba(184,134,11,0.10)')
                  : 'rgba(200,168,130,0.03)',
                color: modelFilter === key
                  ? (cyan ? 'var(--neon-cyan)' : 'var(--neon-gold)')
                  : 'var(--text-muted)',
                border: `1px solid ${modelFilter === key
                  ? (cyan ? 'rgba(200,168,130,0.3)' : 'rgba(184,134,11,0.3)')
                  : 'rgba(200,168,130,0.08)'}`,
              }}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {/* ── テーブル ────────────────────────────────────── */}
      <div className="neon-card">
        <div className="px-4 py-3 border-b border-[rgba(200,168,130,0.12)] flex items-center justify-between">
          <span className="text-sm neon-text-gold tracking-[0.2em] font-semibold">
            HIT RECORDS — {sorted.length} 件
          </span>
          <span className="text-xs text-[var(--text-muted)]">
            合計払戻 ¥{Math.round(filtered.reduce((s, p) => s + (p.payout ?? 0), 0)).toLocaleString()}
          </span>
        </div>
        <div className="w-full overflow-x-auto" style={{ WebkitOverflowScrolling: 'touch' }}>
          <table className="race-table" style={{ minWidth: '700px', width: '100%' }}>
            <thead>
              <tr>
                <SortTh col="date"   label="日付" />
                <th className="text-left whitespace-nowrap">会場 · R</th>
                <th className="text-left whitespace-nowrap">レース名</th>
                <th className="text-left whitespace-nowrap">モデル</th>
                <th className="text-left whitespace-nowrap">券種</th>
                <th className="text-left whitespace-nowrap">予想馬</th>
                <th className="text-right whitespace-nowrap">投資</th>
                <SortTh col="payout" label="払戻" />
                <SortTh col="roi"    label="ROI" />
              </tr>
            </thead>
            <tbody>
              {sorted.map(pred => {
                const roi = pred.roi ?? 0
                return (
                  <tr key={pred.prediction_id} className={rowClass(roi)}>
                    <td className="font-mono text-[var(--text-muted)]">{pred.date}</td>
                    <td className="text-[var(--text-muted)]">
                      {pred.venue}{pred.race_number != null ? ` ${pred.race_number}R` : ''}
                    </td>
                    <td className="max-w-[180px] truncate font-semibold" title={pred.race_name}>
                      {pred.race_name}
                    </td>
                    <td>
                      <span className={`font-bold text-sm ${
                        (pred.model_type ?? '').includes('卍') ? 'neon-text' :
                        (pred.model_type ?? '').includes('Oracle') ? 'neon-text-gold' :
                        (pred.model_type ?? '').includes('Alpha') ? 'neon-text-green' :
                        'neon-text-gold'
                      }`}>
                        {pred.model_type ?? '—'}
                      </span>
                    </td>
                    <td className="text-[var(--text-muted)]">
                      <div>{pred.bet_form ?? pred.bet_type}</div>
                      {pred.n_tickets != null && pred.n_tickets > 0 && (
                        <div className="text-xs opacity-60">{pred.n_tickets}点</div>
                      )}
                    </td>
                    <td
                      className="max-w-[200px] truncate font-mono font-semibold text-[var(--text-primary)]"
                      title={formatCombinations(pred).full}
                    >
                      {formatCombinations(pred).display}
                    </td>
                    <td className="text-right font-mono text-[var(--text-muted)]">
                      {(pred.invested ?? 0) > 0
                        ? `¥${Math.round(pred.invested!).toLocaleString()}`
                        : pred.recommended_bet != null
                          ? `¥${Math.round(pred.recommended_bet).toLocaleString()}`
                          : '—'}
                    </td>
                    <td className="text-right">
                      <PayoutDisplay payout={pred.payout} roi={roi} />
                    </td>
                    <td className="text-right">
                      <span className={roiBadgeClass(roi)}>
                        {roi.toFixed(1)}%
                      </span>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
        <div className="px-4 py-2.5 text-sm text-[var(--text-muted)] border-t border-[var(--border)]">
          表示 {sorted.length} / {hits.length} 件
        </div>
      </div>
    </div>
  )
}

// ── サブコンポーネント ────────────────────────────────────

function StatCard({
  label, value, sub, color, glow = false,
}: {
  label: string; value: string | number; sub: string; color: string; glow?: boolean
}) {
  return (
    <div
      className="neon-card p-4"
      style={glow ? {
        boxShadow: '0 0 20px rgba(255,170,0,0.2), 0 0 40px rgba(255,170,0,0.08)',
        border: '1px solid rgba(255,170,0,0.3)',
      } : undefined}
    >
      <div className="text-xs text-[var(--text-muted)] tracking-wider mb-2 uppercase">{label}</div>
      <div className="text-2xl font-bold" style={{ color }}>
        {value}
      </div>
      <div className="text-xs text-[var(--text-muted)] mt-1">{sub}</div>
    </div>
  )
}

function PayoutDisplay({ payout, roi }: { payout: number | null; roi: number }) {
  if (payout == null) return <span className="text-[var(--text-muted)]">—</span>
  const formatted = `¥${Math.round(payout).toLocaleString()}`
  if (roi >= 500) {
    return (
      <span
        className="font-bold font-mono"
        style={{
          background: 'linear-gradient(90deg, #ff8c00, #ffd700, #ff8c00)',
          WebkitBackgroundClip: 'text',
          WebkitTextFillColor: 'transparent',
          backgroundSize: '200%',
          animation: 'rainbow-shift 2s ease infinite',
          fontSize: '0.95rem',
        }}
      >
        {formatted}
      </span>
    )
  }
  if (roi >= 200) {
    return <span className="font-bold font-mono neon-text-gold">{formatted}</span>
  }
  return <span className="font-mono neon-text-green">{formatted}</span>
}
