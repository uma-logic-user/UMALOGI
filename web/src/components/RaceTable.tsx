'use client'

import { useState } from 'react'
import type { RaceResult } from '@/types/race'

interface Props { results: RaceResult[] }

type SortKey = 'rank' | 'win_odds' | 'popularity' | 'horse_weight'

const MEDAL: Record<number, string> = { 1: '🥇', 2: '🥈', 3: '🥉' }

export default function RaceTable({ results }: Props) {
  const [sortKey, setSortKey]     = useState<SortKey>('rank')
  const [sortAsc, setSortAsc]     = useState(true)
  const [highlight, setHighlight] = useState<string | null>(null)

  function handleSort(key: SortKey) {
    if (key === sortKey) setSortAsc(a => !a)
    else { setSortKey(key); setSortAsc(true) }
  }

  const sorted = [...results].sort((a, b) => {
    const av = a[sortKey] ?? (sortAsc ? Infinity : -Infinity)
    const bv = b[sortKey] ?? (sortAsc ? Infinity : -Infinity)
    return sortAsc ? (av as number) - (bv as number) : (bv as number) - (av as number)
  })

  function SortTh({ col, label }: { col: SortKey; label: string }) {
    const active = sortKey === col
    return (
      <th
        className="cursor-pointer select-none"
        onClick={() => handleSort(col)}
        style={{ color: active ? 'var(--neon-cyan)' : undefined }}
      >
        <span className="flex items-center gap-1">
          {label}
          {active && <span className="text-xs">{sortAsc ? '▲' : '▼'}</span>}
        </span>
      </th>
    )
  }

  return (
    <div className="neon-card overflow-hidden slide-in" style={{ animationDelay: '0.25s' }}>
      <div className="flex items-center justify-between px-4 py-3 border-b border-[rgba(0,200,255,0.12)]">
        <span className="text-sm neon-text tracking-[0.2em] font-semibold">RACE RESULTS</span>
        <span className="text-sm text-[var(--text-muted)]">
          {results.length} runners &nbsp;·&nbsp; click header to sort
        </span>
      </div>

      <div className="table-scroll">
        <table className="w-full race-table">
          <thead>
            <tr>
              <SortTh col="rank"         label="着順" />
              <th>馬名</th>
              <th>父 / 母父</th>
              <th>性齢</th>
              <th>騎手</th>
              <th>タイム</th>
              <th>着差</th>
              <SortTh col="win_odds"     label="単勝" />
              <SortTh col="popularity"   label="人気" />
              <SortTh col="horse_weight" label="馬体重" />
            </tr>
          </thead>
          <tbody>
            {sorted.map(r => (
              <tr
                key={r.horse_name}
                className={`
                  transition-colors cursor-pointer
                  ${r.rank === 1 ? 'row-rank-1' : ''}
                  ${r.rank === 2 ? 'row-rank-2' : ''}
                  ${r.rank === 3 ? 'row-rank-3' : ''}
                  ${highlight === r.horse_name ? 'outline outline-1 outline-[rgba(0,200,255,0.3)]' : ''}
                `}
                onClick={() => setHighlight(h => h === r.horse_name ? null : r.horse_name)}
              >
                {/* 着順 */}
                <td className="font-bold text-center w-12">
                  {r.rank != null
                    ? MEDAL[r.rank]
                      ? <span title={`${r.rank}着`}>{MEDAL[r.rank]}</span>
                      : <span className="text-[var(--text-muted)]">{r.rank}</span>
                    : <span className="text-[var(--text-muted)]">—</span>}
                </td>

                {/* 馬名 */}
                <td>
                  <span
                    className={`font-semibold ${
                      r.rank === 1 ? 'neon-text-gold' :
                      r.rank != null && r.rank <= 3 ? 'text-[var(--text-primary)]' :
                      'text-[var(--text-muted)]'
                    }`}
                  >
                    {r.horse_name}
                  </span>
                </td>

                {/* 父 / 母父 */}
                <td>
                  <div className="text-[var(--text-primary)] leading-tight">
                    {r.sire ?? <span className="text-[var(--text-muted)]">—</span>}
                  </div>
                  {r.dam_sire && (
                    <div className="text-[var(--text-muted)] text-xs mt-0.5">
                      母父 {r.dam_sire}
                    </div>
                  )}
                </td>

                {/* 性齢 */}
                <td className="text-[var(--text-muted)]">{r.sex_age}</td>

                {/* 騎手 */}
                <td className="text-[var(--text-primary)]">{r.jockey}</td>

                {/* タイム */}
                <td className={`font-mono ${r.rank === 1 ? 'neon-text' : 'text-[var(--text-primary)]'}`}>
                  {r.finish_time ?? '—'}
                </td>

                {/* 着差 */}
                <td className="text-[var(--text-muted)]">
                  {r.margin || (r.rank === 1 ? <span className="neon-text">◎</span> : '—')}
                </td>

                {/* 単勝 */}
                <td className="font-mono text-right">
                  <OddsCell odds={r.win_odds} />
                </td>

                {/* 人気 */}
                <td className="text-center">
                  <PopularityBadge pop={r.popularity} />
                </td>

                {/* 馬体重 */}
                <td className="font-mono text-right text-[var(--text-muted)]">
                  {r.horse_weight ?? '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function OddsCell({ odds }: { odds: number | null }) {
  if (odds == null) return <span className="text-[var(--text-muted)]">—</span>
  const color =
    odds <= 5   ? 'var(--neon-green)' :
    odds <= 20  ? 'var(--neon-cyan)'  :
    odds <= 50  ? 'var(--neon-gold)'  : 'var(--neon-red)'
  return (
    <span style={{ color, textShadow: `0 0 6px ${color}66` }}>
      {odds.toFixed(1)}
    </span>
  )
}

function PopularityBadge({ pop }: { pop: number | null }) {
  if (pop == null) return <span className="text-[var(--text-muted)]">—</span>
  const bg =
    pop === 1 ? 'rgba(0,255,136,0.15)' :
    pop <= 3  ? 'rgba(0,200,255,0.10)' :
    pop <= 6  ? 'rgba(0,100,255,0.08)' : 'transparent'
  const col =
    pop === 1 ? 'var(--neon-green)' :
    pop <= 3  ? 'var(--neon-cyan)'  :
    pop <= 6  ? 'var(--neon-blue)'  : 'var(--text-muted)'
  return (
    <span
      className="inline-flex items-center justify-center w-7 h-7 rounded font-bold"
      style={{ background: bg, color: col, border: `1px solid ${col}44`, fontSize: '0.85rem' }}
    >
      {pop}
    </span>
  )
}
