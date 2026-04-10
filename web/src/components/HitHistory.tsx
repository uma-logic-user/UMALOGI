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

function horseNames(pred: Prediction): string {
  if (!pred.horses?.length) return '—'
  return pred.horses.map(h => h.horse_name).join(' / ')
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
    if (modelFilter !== 'all') data = data.filter(p => p.model_type === modelFilter)
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
    <div className="p-4 space-y-4 max-w-[1400px]">

      {/* ── ヘッダー ──────────────────────────────────────── */}
      <div className="flex items-center gap-3 flex-wrap">
        <h1
          className="text-xl font-bold tracking-wider"
          style={{
            color: 'var(--neon-gold)',
            textShadow: '0 0 16px rgba(255,215,0,0.7), 0 0 32px rgba(255,215,0,0.3)',
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
                ? f.key === 'jackpot' ? 'rgba(255,140,0,0.2)' : 'rgba(255,215,0,0.15)'
                : 'rgba(0,200,255,0.04)',
              color: filter === f.key
                ? f.key === 'jackpot' ? '#ffaa00' : 'var(--neon-gold)'
                : 'var(--text-muted)',
              border: `1px solid ${filter === f.key
                ? f.key === 'jackpot' ? 'rgba(255,140,0,0.5)' : 'rgba(255,215,0,0.4)'
                : 'rgba(0,200,255,0.1)'}`,
            }}
          >
            {f.label}
            <span className="ml-1.5 opacity-70">{f.count}</span>
          </button>
        ))}

        {/* モデルフィルター */}
        <div className="ml-auto flex gap-2">
          {['all', '卍', '本命'].map(m => (
            <button
              key={m}
              onClick={() => setModelFilter(m)}
              className="px-3 py-1.5 rounded text-xs font-semibold transition-all"
              style={{
                background: modelFilter === m ? 'rgba(0,200,255,0.12)' : 'rgba(0,200,255,0.03)',
                color: modelFilter === m ? 'var(--neon-cyan)' : 'var(--text-muted)',
                border: `1px solid ${modelFilter === m ? 'rgba(0,200,255,0.3)' : 'rgba(0,200,255,0.08)'}`,
              }}
            >
              {m === 'all' ? '全モデル' : m}
            </button>
          ))}
        </div>
      </div>

      {/* ── テーブル ────────────────────────────────────── */}
      <div className="neon-card overflow-hidden">
        <div className="px-4 py-3 border-b border-[rgba(0,200,255,0.12)] flex items-center justify-between">
          <span className="text-sm neon-text-gold tracking-[0.2em] font-semibold">
            HIT RECORDS — {sorted.length} 件
          </span>
          <span className="text-xs text-[var(--text-muted)]">
            合計払戻 ¥{Math.round(filtered.reduce((s, p) => s + (p.payout ?? 0), 0)).toLocaleString()}
          </span>
        </div>
        <div className="table-scroll">
          <table className="race-table w-full">
            <thead>
              <tr>
                <SortTh col="date"   label="日付" />
                <th className="text-left">会場 · R</th>
                <th className="text-left">レース名</th>
                <th className="text-left">モデル</th>
                <th className="text-left">券種</th>
                <th className="text-left">予想馬</th>
                <th className="text-right">投資</th>
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
                        pred.model_type === '卍' ? 'neon-text' : 'neon-text-gold'
                      }`}>
                        {pred.model_type}
                      </span>
                    </td>
                    <td className="text-[var(--text-muted)]">{pred.bet_type}</td>
                    <td
                      className="max-w-[200px] truncate font-semibold text-[var(--text-primary)]"
                      title={horseNames(pred)}
                    >
                      {horseNames(pred)}
                    </td>
                    <td className="text-right font-mono text-[var(--text-muted)]">
                      {pred.recommended_bet != null
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
      <div className="text-2xl font-bold" style={{ color, textShadow: `0 0 10px ${color}88` }}>
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
