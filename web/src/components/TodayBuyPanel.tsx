'use client'

import { useMemo, useState } from 'react'
import { calcKellyFraction, calcKellyStake } from '@/lib/kelly'
import type { RacePrediction } from '@/types/race'

interface TodayBuyPanelProps {
  predictions:  RacePrediction[]
  /** horse_number → win_odds（race.results から構築して渡す） */
  winOddsMap:   Record<number, number>
}

/** combination_json から「3-7-1」形式の馬番文字列を生成する */
function formatHorseNums(combinationJson: string | null): string {
  if (!combinationJson) return '—'
  try {
    const parsed: unknown = JSON.parse(combinationJson)
    if (Array.isArray(parsed)) {
      return (parsed as unknown[])
        .map(item => (Array.isArray(item) ? (item as number[]).join('-') : String(item)))
        .join(' / ')
    }
    return String(parsed)
  } catch {
    return '—'
  }
}

/** combination_json から最初の馬番を返す（単勝オッズ照合用） */
function extractFirstHorseNum(combinationJson: string | null): number | null {
  if (!combinationJson) return null
  try {
    const parsed: unknown = JSON.parse(combinationJson)
    if (Array.isArray(parsed)) {
      const first = (parsed as unknown[])[0]
      if (Array.isArray(first)) return (first as number[])[0] ?? null
      return typeof first === 'number' ? first : null
    }
    return null
  } catch {
    return null
  }
}

const BANKROLL_OPTIONS = [50_000, 100_000, 200_000, 300_000, 500_000]
const KELLY_OPTIONS    = [
  { value: 0.10, label: '10% Kelly' },
  { value: 0.25, label: '1/4 Kelly' },
  { value: 0.50, label: '1/2 Kelly' },
  { value: 1.00, label: 'Full Kelly' },
]

export function TodayBuyPanel({ predictions, winOddsMap }: TodayBuyPanelProps) {
  const [bankroll,  setBankroll]  = useState(100_000)
  const [kellyFrac, setKellyFrac] = useState(0.25)

  const rows = useMemo(() => {
    return predictions.map(p => {
      const ev      = p.expected_value ?? 0
      const betType = p.bet_type

      // 単勝以外は組み合わせオッズが存在しないため、単勝オッズ流用を禁止する。
      // 誤ったオッズでケリー計算すると過剰賭けによる破産リスクが生じる。
      const isWin   = betType === '単勝'
      const noOdds  = !isWin  // 単勝以外は正確なオッズ不明 → ケリー計算スキップ

      const hn   = isWin ? extractFirstHorseNum(p.combination_json) : null
      const odds = (hn != null && isWin) ? (winOddsMap[hn] ?? 0) : 0

      const canCalc = isWin && ev > 0 && odds > 1
      const stake   = canCalc ? calcKellyStake(ev, odds, bankroll, kellyFrac) : 0
      const f       = canCalc ? calcKellyFraction(ev, odds) : 0

      return {
        id:        p.prediction_id,
        betType,
        horseNums: formatHorseNums(p.combination_json),
        odds,
        ev,
        f,
        stake,
        noOdds,                    // 正確なオッズが取得できない券種
        skip:      !noOdds && stake === 0,  // EV<1.0 で購入見送り（単勝のみ）
      }
    })
  }, [predictions, winOddsMap, bankroll, kellyFrac])

  const totalStake  = useMemo(() => rows.reduce((s, r) => s + r.stake, 0), [rows])
  // 推奨購入あり = 単勝かつ stake > 0（オッズ確認要は集計から除外）
  const activeCount = rows.filter(r => !r.noOdds && !r.skip && r.stake > 0).length

  if (predictions.length === 0) {
    return (
      <div className="neon-card p-12 text-center">
        <div className="text-[var(--text-muted)] text-base tracking-widest">
          このレースの予想データがありません
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-4">

      {/* ── 設定パネル ─────────────────────────────────── */}
      <div className="neon-card p-4 flex flex-wrap gap-4 items-center">
        <div className="flex items-center gap-2">
          <span className="text-xs text-[var(--text-muted)] tracking-wider">総資金</span>
          <select
            value={bankroll}
            onChange={e => setBankroll(Number(e.target.value))}
            className="rounded bg-[var(--bg-card)] border border-[rgba(0,200,255,0.2)] px-2 py-1 text-sm text-[var(--text-primary)] focus:outline-none focus:border-[var(--neon-cyan)]"
          >
            {BANKROLL_OPTIONS.map(v => (
              <option key={v} value={v}>¥{v.toLocaleString()}</option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-2">
          <span className="text-xs text-[var(--text-muted)] tracking-wider">ケリー係数</span>
          <select
            value={kellyFrac}
            onChange={e => setKellyFrac(Number(e.target.value))}
            className="rounded bg-[var(--bg-card)] border border-[rgba(0,200,255,0.2)] px-2 py-1 text-sm text-[var(--text-primary)] focus:outline-none focus:border-[var(--neon-cyan)]"
          >
            {KELLY_OPTIONS.map(({ value, label }) => (
              <option key={value} value={value}>{label}</option>
            ))}
          </select>
        </div>

        <div className="ml-auto text-xs text-[var(--text-muted)]">
          推奨購入あり:&nbsp;
          <span className="font-semibold text-[var(--text-primary)]">{activeCount}件</span>
        </div>
      </div>

      {/* ── テーブル ────────────────────────────────────── */}
      <div className="neon-card overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[rgba(0,200,255,0.12)] text-[var(--text-muted)] text-xs tracking-wider">
              <th className="px-4 py-3 text-left">買い目種別</th>
              <th className="px-4 py-3 text-left">馬番</th>
              <th className="px-4 py-3 text-right">オッズ</th>
              <th className="px-4 py-3 text-right">EV</th>
              <th className="px-4 py-3 text-right">f*</th>
              <th className="px-4 py-3 text-right">推奨購入額</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[rgba(0,200,255,0.08)]">
            {rows.map(row => (
              <tr
                key={row.id}
                className={
                  row.skip
                    ? 'opacity-40'
                    : 'hover:bg-[rgba(0,200,255,0.04)]'
                }
              >
                <td className="px-4 py-3 font-semibold text-[var(--text-primary)]">
                  {row.betType}
                </td>
                <td className="px-4 py-3 text-[var(--text-secondary)]">{row.horseNums}</td>
                <td className="px-4 py-3 text-right text-[var(--text-secondary)] font-mono">
                  {row.noOdds ? '—' : row.odds > 0 ? `${row.odds.toFixed(1)}倍` : '—'}
                </td>
                <td className={`px-4 py-3 text-right font-mono font-semibold ${
                  row.ev >= 1.0 ? 'text-green-400' : 'text-[var(--text-muted)]'
                }`}>
                  {row.ev > 0 ? row.ev.toFixed(2) : '—'}
                </td>
                <td className="px-4 py-3 text-right font-mono text-xs text-[var(--text-muted)]">
                  {row.noOdds ? '—' : row.f > 0 ? `${(row.f * 100).toFixed(1)}%` : '—'}
                </td>
                <td className="px-4 py-3 text-right">
                  {row.noOdds ? (
                    <span className="text-xs text-orange-400 opacity-80">オッズ確認要</span>
                  ) : row.skip ? (
                    <span className="text-xs text-[var(--text-muted)] opacity-60">購入見送り</span>
                  ) : (
                    <span className="font-bold text-yellow-400">
                      ¥{row.stake.toLocaleString()}
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* ── 合計 ──────────────────────────────────────── */}
      {totalStake > 0 ? (
        <div className="neon-card px-4 py-3 flex justify-between items-center">
          <span className="text-xs text-[var(--text-muted)] tracking-wider">合計推奨購入額</span>
          <span className="text-lg font-bold text-yellow-400">
            ¥{totalStake.toLocaleString()}
          </span>
        </div>
      ) : (
        <div className="neon-card p-6 text-center text-[var(--text-muted)] text-sm">
          本日の推奨購入なし（全件 EV &lt; 1.0 またはオッズ取得不可）
        </div>
      )}

      {/* ── 注釈 ──────────────────────────────────────── */}
      <p className="text-xs text-[var(--text-muted)] opacity-60 leading-relaxed px-1">
        ※ ケリー基準 f* = (EV−1)÷(オッズ−1)。<strong className="text-orange-400 opacity-100">単勝のみ</strong>計算対象。
        馬連・ワイド・三連複等は組み合わせオッズが不明のため「オッズ確認要」と表示し計算をスキップします（誤った金額提示による過剰賭けを防止）。
        EV&lt;1.0 の単勝は「購入見送り」。推奨額 = 総資金 × ケリー係数 × f*（100円切り捨て）。投資はご自身の判断でお願いします。
      </p>
    </div>
  )
}
