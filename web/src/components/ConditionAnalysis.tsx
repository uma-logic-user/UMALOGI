'use client'

import { useState } from 'react'

interface ConditionRow {
  model_type:     string
  total_bets:     number
  hits:           number
  hit_rate:       number | null
  total_invested: number
  total_payout:   number
  roi:            number | null
  // 各グループキー（どれか1つが存在する）
  venue?:          string
  distance_cat?:   string
  surface?:        string
  track_condition?: string
}

interface CombinedRow extends ConditionRow {
  venue:           string
  distance_cat:    string
  surface:         string
  track_condition: string
}

interface ConditionData {
  generated_at: string
  by_venue:     ConditionRow[]
  by_distance:  ConditionRow[]
  by_surface:   ConditionRow[]
  by_condition: ConditionRow[]
  combined:     CombinedRow[]
}

type GroupKey = 'combined' | 'by_venue' | 'by_distance' | 'by_surface' | 'by_condition'

const GROUP_LABELS: Record<GroupKey, string> = {
  combined:     '総合（全条件組み合わせ）',
  by_venue:     '競馬場別',
  by_distance:  '距離別',
  by_surface:   '馬場別（芝/ダ）',
  by_condition: '馬場状態別',
}

const TABS: { id: GroupKey; label: string }[] = [
  { id: 'combined',     label: '総合' },
  { id: 'by_venue',     label: '競馬場' },
  { id: 'by_distance',  label: '距離' },
  { id: 'by_surface',   label: '馬場' },
  { id: 'by_condition', label: '馬場状態' },
]

function roiClass(roi: number | null): string {
  if (roi === null) return 'text-[var(--text-muted)]'
  if (roi >= 200)   return 'neon-text-gold font-bold'
  if (roi >= 100)   return 'neon-text-green font-bold'
  if (roi < 50)     return 'text-[var(--neon-red)]'
  return 'text-[var(--text-primary)]'
}

function roiBg(roi: number | null): string {
  if (roi === null) return ''
  if (roi >= 200)   return 'bg-[rgba(255,215,0,0.07)]'
  if (roi >= 100)   return 'bg-[rgba(0,255,136,0.06)]'
  if (roi < 50)     return 'bg-[rgba(255,51,102,0.05)]'
  return ''
}

function labelFor(row: ConditionRow | CombinedRow, key: GroupKey): string {
  if (key === 'combined') {
    const r = row as CombinedRow
    return `${r.venue} / ${r.distance_cat} / ${r.surface} / ${r.track_condition}`
  }
  if (key === 'by_venue')     return (row as ConditionRow).venue     ?? '—'
  if (key === 'by_distance')  return (row as ConditionRow).distance_cat ?? '—'
  if (key === 'by_surface')   return (row as ConditionRow).surface    ?? '—'
  if (key === 'by_condition') return (row as ConditionRow).track_condition ?? '—'
  return '—'
}

function fmt(n: number | null): string {
  if (n === null || n === undefined) return '—'
  return n.toLocaleString('ja-JP')
}

interface Props {
  data: ConditionData | null
}

export default function ConditionAnalysis({ data }: Props) {
  const [activeTab, setActiveTab] = useState<GroupKey>('combined')

  if (!data) {
    return (
      <div className="p-6 neon-card text-center">
        <div className="text-[var(--text-muted)] tracking-widest">
          データがありません。<code className="text-xs opacity-60">python web/generate_data.py</code> を実行してください。
        </div>
      </div>
    )
  }

  const rows: (ConditionRow | CombinedRow)[] = data[activeTab] ?? []

  return (
    <div className="p-4 space-y-5 w-full min-w-0">
      {/* ヘッダー */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <div className="text-sm neon-text tracking-[0.2em] font-semibold uppercase">
            得意条件分析 — CONDITION ANALYSIS
          </div>
          <div className="text-xs text-[var(--text-muted)] mt-0.5">
            過去2年 · 最低3件以上 · 生成: {data.generated_at?.slice(0, 16) ?? '—'}
          </div>
        </div>
        <div className="flex gap-3 text-xs text-[var(--text-muted)]">
          <span className="flex items-center gap-1">
            <span className="inline-block w-2.5 h-2.5 rounded-sm bg-[rgba(255,215,0,0.4)]" />
            ROI≥200%
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block w-2.5 h-2.5 rounded-sm bg-[rgba(0,255,136,0.4)]" />
            ROI≥100%
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block w-2.5 h-2.5 rounded-sm bg-[rgba(255,51,102,0.3)]" />
            ROI&lt;50%
          </span>
        </div>
      </div>

      {/* タブバー */}
      <div className="flex gap-0 border-b border-[rgba(0,200,255,0.18)]">
        {TABS.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`relative px-5 py-2.5 text-sm font-semibold tracking-wider transition-colors ${
              activeTab === tab.id
                ? 'text-[var(--neon-cyan)]'
                : 'text-[var(--text-muted)] hover:text-[var(--text-primary)]'
            }`}
          >
            {tab.label}
            {activeTab === tab.id && (
              <span
                className="absolute bottom-0 inset-x-0 h-[2px] bg-[var(--neon-cyan)]"
                style={{ boxShadow: '0 0 8px rgba(0,200,255,0.9)' }}
              />
            )}
          </button>
        ))}
      </div>

      {/* テーブル */}
      <div className="neon-card">
        <div className="px-4 py-2.5 border-b border-[rgba(0,200,255,0.12)]">
          <span className="text-xs text-[var(--text-muted)] tracking-wider">
            {GROUP_LABELS[activeTab]} — {rows.length} 件
          </span>
        </div>
        {rows.length === 0 ? (
          <div className="p-8 text-center text-[var(--text-muted)] text-sm">
            データなし（最低3件以上の条件が必要です）
          </div>
        ) : (
          <div className="w-full overflow-x-auto" style={{ WebkitOverflowScrolling: 'touch' }}>
            <table className="race-table" style={{ minWidth: '640px', width: '100%' }}>
              <thead>
                <tr>
                  <th className="text-left whitespace-nowrap">条件</th>
                  <th className="text-center whitespace-nowrap">モデル</th>
                  <th className="text-right whitespace-nowrap">件数</th>
                  <th className="text-right whitespace-nowrap">的中</th>
                  <th className="text-right whitespace-nowrap">的中率</th>
                  <th className="text-right whitespace-nowrap">ROI</th>
                  <th className="text-right whitespace-nowrap">投資</th>
                  <th className="text-right whitespace-nowrap">払戻</th>
                  <th className="text-right whitespace-nowrap">損益</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row, i) => {
                  const roi    = row.roi ?? null
                  const profit = (row.total_payout ?? 0) - (row.total_invested ?? 0)
                  return (
                    <tr key={i} className={roiBg(roi)}>
                      <td className="font-medium text-[var(--text-primary)] max-w-[260px] truncate">
                        {labelFor(row, activeTab)}
                      </td>
                      <td className="text-center">
                        <span className={`text-xs font-bold px-2 py-0.5 rounded ${
                          row.model_type === '卍'
                            ? 'bg-[rgba(0,200,255,0.1)] text-[var(--neon-cyan)]'
                            : 'bg-[rgba(255,215,0,0.1)] text-[var(--neon-gold)]'
                        }`}>
                          {row.model_type}
                        </span>
                      </td>
                      <td className="text-right font-mono text-[var(--text-muted)]">
                        {row.total_bets}
                      </td>
                      <td className="text-right font-mono text-[var(--text-muted)]">
                        {row.hits}
                      </td>
                      <td className="text-right font-mono">
                        {row.hit_rate !== null ? `${row.hit_rate.toFixed(1)}%` : '—'}
                      </td>
                      <td className={`text-right font-mono ${roiClass(roi)}`}>
                        {roi !== null ? `${roi.toFixed(1)}%` : '—'}
                      </td>
                      <td className="text-right font-mono text-[var(--text-muted)]">
                        ¥{fmt(row.total_invested)}
                      </td>
                      <td className="text-right font-mono text-[var(--text-muted)]">
                        ¥{fmt(row.total_payout)}
                      </td>
                      <td className={`text-right font-mono font-semibold ${
                        profit > 0 ? 'neon-text-green' : profit < 0 ? 'text-[var(--neon-red)]' : 'text-[var(--text-muted)]'
                      }`}>
                        {profit >= 0 ? '+' : ''}¥{fmt(profit)}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
