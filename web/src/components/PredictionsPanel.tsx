'use client'

import { Prediction } from '../types/race'

interface Props {
  predictions: Prediction[]
  raceId?:     string
  modelType?:  string
  limit?:      number
}

function hitClass(pred: Prediction): string {
  if (pred.is_hit === null) return ''
  if (!pred.is_hit)         return 'hit-miss'
  const roi = pred.roi ?? 0
  if (roi >= 500) return 'hit-explosion'
  if (roi >= 200) return 'hit-big'
  return 'hit-normal'
}

function payoutBadgeClass(pred: Prediction): string {
  const roi = pred.roi ?? 0
  if (roi >= 500) return 'payout-badge payout-badge-explosion'
  if (roi >= 200) return 'payout-badge payout-badge-big'
  return 'payout-badge payout-badge-normal'
}

function formatPayout(payout: number | null): string {
  if (payout == null || payout === 0) return '—'
  return `¥${Math.round(payout).toLocaleString()}`
}

function formatBet(bet: number | null): string {
  if (bet == null) return '—'
  return `¥${Math.round(bet).toLocaleString()}`
}

function formatRoi(roi: number | null): string {
  if (roi == null) return '—'
  return `${roi.toFixed(1)}%`
}

function hitLabel(pred: Prediction): string {
  if (pred.is_hit === null) return '—'
  return pred.is_hit ? '◎ 的中' : '✕ 外れ'
}

function hitLabelClass(pred: Prediction): string {
  if (pred.is_hit === null) return 'text-[var(--text-muted)]'
  if (!pred.is_hit)         return 'neon-text-red font-semibold'
  const roi = pred.roi ?? 0
  if (roi >= 200) return 'neon-text-gold font-bold'
  return 'neon-text-green font-bold'
}

function horseNames(pred: Prediction): string {
  if (!pred.horses || pred.horses.length === 0) return '—'
  return pred.horses.map(h => h.horse_name).join(' / ')
}

export default function PredictionsPanel({ predictions, raceId, modelType, limit = 50 }: Props) {
  let filtered = predictions
  if (raceId)    filtered = filtered.filter(p => p.race_id === raceId)
  if (modelType) filtered = filtered.filter(p => p.model_type === modelType)
  const items = filtered.slice(0, limit)

  if (items.length === 0) {
    return (
      <div className="neon-card p-6 text-center text-[var(--text-muted)] text-base">
        予想データなし
      </div>
    )
  }

  return (
    <div className="neon-card overflow-hidden">
      <div className="table-scroll">
        <table className="race-table w-full">
          <thead>
            <tr>
              <th className="text-left">日付</th>
              <th className="text-left">レース</th>
              <th className="text-left">モデル</th>
              <th className="text-left">券種</th>
              <th className="text-left">予想馬</th>
              <th className="text-right">賭金</th>
              <th className="text-center">結果</th>
              <th className="text-right">払戻</th>
              <th className="text-right">ROI</th>
            </tr>
          </thead>
          <tbody>
            {items.map(pred => (
              <tr key={pred.prediction_id} className={hitClass(pred)}>
                <td className="text-[var(--text-muted)] font-mono">
                  {pred.date}
                </td>
                <td className="max-w-[160px] truncate" title={pred.race_name}>
                  <span className="text-[var(--text-muted)]">{pred.venue}</span>{' '}
                  <span className="font-semibold">{pred.race_name}</span>
                </td>
                <td>
                  <span className={`font-bold ${pred.model_type === '卍' ? 'neon-text' : 'neon-text-gold'}`}>
                    {pred.model_type}
                  </span>
                </td>
                <td>{pred.bet_type}</td>
                <td className="max-w-[200px] truncate" title={horseNames(pred)}>
                  {horseNames(pred)}
                </td>
                <td className="text-right text-[var(--text-muted)] font-mono">
                  {formatBet(pred.recommended_bet)}
                </td>
                <td className="text-center">
                  <span className={hitLabelClass(pred)}>
                    {hitLabel(pred)}
                  </span>
                </td>
                <td className="text-right">
                  {pred.is_hit === 1 ? (
                    <span className={payoutBadgeClass(pred)}>
                      {formatPayout(pred.payout)}
                    </span>
                  ) : (
                    <span className="text-[var(--text-muted)]">—</span>
                  )}
                </td>
                <td className="text-right font-mono">
                  {pred.is_hit === 1 ? (
                    <span className={(pred.roi ?? 0) >= 200 ? 'neon-text-gold font-bold' : 'neon-text-green'}>
                      {formatRoi(pred.roi)}
                    </span>
                  ) : (
                    <span className="text-[var(--text-muted)]">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="px-4 py-2.5 text-sm text-[var(--text-muted)] border-t border-[var(--border)]">
        表示 {items.length} / {filtered.length} 件
      </div>
    </div>
  )
}
