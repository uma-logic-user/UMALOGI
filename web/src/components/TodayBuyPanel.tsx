'use client'

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { calcKellyFraction, calcKellyStake } from '@/lib/kelly'
import type { RacePrediction } from '@/types/race'

// ── SHAP 型定義 ───────────────────────────────────────────────
type ShapEntry = { feature: string; label: string; value: number }
type ShapHorse = { horse_name: string; predicted_rank: number | null; shap: ShapEntry[] | null }

interface TodayBuyPanelProps {
  predictions:  RacePrediction[]
  /** horse_number → win_odds（race.results から構築して渡す・静的フォールバック） */
  winOddsMap:   Record<number, number>
  /** race_id が渡された場合、realtime-odds API を pollingIntervalMs ごとにポーリングする */
  raceId?:            string
  pollingIntervalMs?: number
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

export function TodayBuyPanel({
  predictions,
  winOddsMap,
  raceId,
  pollingIntervalMs = 90_000,
}: TodayBuyPanelProps) {
  const [bankroll,  setBankroll]  = useState(100_000)
  const [kellyFrac, setKellyFrac] = useState(0.25)

  // ── リアルタイムオッズ状態 ──────────────────────────────────────────
  const [liveOddsMap,   setLiveOddsMap]   = useState<Record<number, number>>({})
  const [oddsUpdatedAt, setOddsUpdatedAt] = useState<string | null>(null)
  const [flashNums, setFlashNums]         = useState<Set<number>>(new Set())
  const prevLiveOddsRef = useRef<Record<number, number>>({})
  const isFirstFetchRef = useRef(true)

  // ── SHAP パネル状態 ───────────────────────────────────────────────
  const [shapOpenId, setShapOpenId]   = useState<number | null>(null)
  const [shapCache,  setShapCache]    = useState<Record<number, ShapHorse[] | 'loading'>>({})

  const fetchShap = useCallback(async (predId: number) => {
    if (shapCache[predId]) {
      setShapOpenId(prev => (prev === predId ? null : predId))
      return
    }
    setShapCache(c => ({ ...c, [predId]: 'loading' }))
    setShapOpenId(predId)
    try {
      const res = await fetch(`/api/shap/${predId}`)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json() as { horses: ShapHorse[] }
      setShapCache(c => ({ ...c, [predId]: data.horses }))
    } catch {
      setShapCache(c => ({ ...c, [predId]: [] }))
    }
  }, [shapCache])

  useEffect(() => {
    if (!raceId) return

    const fetchOdds = async () => {
      try {
        const res = await fetch(`/api/realtime-odds/${raceId}`)
        if (!res.ok) return
        const data = await res.json() as {
          updated_at: string | null
          odds: Record<string, { win_odds: number | null }>
        }

        const newMap: Record<number, number> = {}
        const changed = new Set<number>()

        for (const [numStr, entry] of Object.entries(data.odds)) {
          if (entry.win_odds == null) continue
          const num = Number(numStr)
          newMap[num] = entry.win_odds
          if (!isFirstFetchRef.current && prevLiveOddsRef.current[num] !== entry.win_odds) {
            changed.add(num)
          }
        }

        isFirstFetchRef.current = false
        prevLiveOddsRef.current = newMap
        setLiveOddsMap(newMap)
        if (data.updated_at) setOddsUpdatedAt(data.updated_at)

        if (changed.size > 0) {
          setFlashNums(changed)
          setTimeout(() => setFlashNums(new Set()), 2000)
        }
      } catch {
        // ネットワーク障害等は無視（静的 winOddsMap にフォールバック）
      }
    }

    fetchOdds()
    const id = setInterval(fetchOdds, pollingIntervalMs)
    return () => clearInterval(id)
  }, [raceId, pollingIntervalMs])

  // 静的フォールバック（race.results）をライブオッズで上書きする
  const effectiveOddsMap = useMemo<Record<number, number>>(
    () => ({ ...winOddsMap, ...liveOddsMap }),
    [winOddsMap, liveOddsMap],
  )

  const rows = useMemo(() => {
    return predictions.map(p => {
      const ev      = p.expected_value ?? 0
      const betType = p.bet_type

      // 単勝以外は組み合わせオッズが存在しないため、単勝オッズ流用を禁止する。
      // 誤ったオッズでケリー計算すると過剰賭けによる破産リスクが生じる。
      const isWin   = betType === '単勝'
      const noOdds  = !isWin  // 単勝以外は正確なオッズ不明 → ケリー計算スキップ

      const hn   = isWin ? extractFirstHorseNum(p.combination_json) : null
      const odds = (hn != null && isWin) ? (effectiveOddsMap[hn] ?? 0) : 0

      const canCalc = isWin && ev > 0 && odds > 1
      const stake   = canCalc ? calcKellyStake(ev, odds, bankroll, kellyFrac) : 0
      const f       = canCalc ? calcKellyFraction(ev, odds) : 0

      return {
        id:              p.prediction_id,
        betType,
        horseNums:       formatHorseNums(p.combination_json),
        primaryHorseNum: hn,   // フラッシュ判定用（単勝以外は null）
        odds,
        ev,
        f,
        stake,
        noOdds,                              // 正確なオッズが取得できない券種
        skip:       !noOdds && stake === 0,  // EV<1.0 で購入見送り（単勝のみ）
        isHit:      p.is_hit,                // null=未確定, 1=的中, 0=外れ
        hitPayout:  p.payout,               // 確定払い戻し額（的中時のみ存在）
      }
    })
  }, [predictions, effectiveOddsMap, bankroll, kellyFrac])

  const totalStake  = useMemo(() => rows.reduce((s, r) => s + r.stake, 0), [rows])
  // 推奨購入あり = 単勝かつ stake > 0（オッズ確認要は集計から除外）
  const activeCount = rows.filter(r => !r.noOdds && !r.skip && r.stake > 0).length

  // 結果確定済み行のサマリー集計
  const confirmedRows    = useMemo(() => rows.filter(r => r.isHit !== null && r.isHit !== undefined), [rows])
  const hasConfirmed     = confirmedRows.length > 0
  // 投資額: Kelly推奨額の合計（確定済み行のみ。未確定行は実績に含めない）
  const confirmedInvest  = useMemo(() => confirmedRows.reduce((s, r) => s + r.stake, 0), [confirmedRows])
  // 払い戻し: 的中行の payout 合計
  const confirmedPayout  = useMemo(
    () => confirmedRows.filter(r => r.isHit === 1).reduce((s, r) => s + (r.hitPayout ?? 0), 0),
    [confirmedRows],
  )
  const roi = confirmedInvest > 0 ? (confirmedPayout / confirmedInvest) * 100 : null

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

      {/* ── 結果サマリー（確定済み行がある場合のみ表示） ─── */}
      {hasConfirmed && (
        <div className="neon-card grid grid-cols-3 divide-x divide-[rgba(200,168,130,0.12)] text-center">
          <div className="py-3 px-2 sm:px-4">
            <p className="text-[9px] sm:text-[10px] text-[var(--text-muted)] tracking-widest mb-1 truncate">合計投資額</p>
            <p className="text-sm sm:text-base font-bold text-yellow-400 tabular-nums">
              {confirmedInvest > 0 ? `¥${confirmedInvest.toLocaleString()}` : '—'}
            </p>
          </div>
          <div className="py-3 px-2 sm:px-4">
            <p className="text-[9px] sm:text-[10px] text-[var(--text-muted)] tracking-widest mb-1 truncate">合計払い戻し額</p>
            <p className={`text-sm sm:text-base font-bold tabular-nums ${confirmedPayout > 0 ? 'text-green-400' : 'text-[var(--text-muted)]'}`}>
              {confirmedPayout > 0 ? `¥${confirmedPayout.toLocaleString()}` : '¥0'}
            </p>
          </div>
          <div className="py-3 px-2 sm:px-4">
            <p className="text-[9px] sm:text-[10px] text-[var(--text-muted)] tracking-widest mb-1 truncate">回収率</p>
            <p className={`text-sm sm:text-base font-bold tabular-nums ${
              roi === null ? 'text-[var(--text-muted)]'
                : roi >= 100 ? 'text-green-400'
                : 'text-red-400'
            }`}>
              {roi !== null ? `${roi.toFixed(1)}%` : '—'}
            </p>
          </div>
        </div>
      )}

      {/* ── 設定パネル ─────────────────────────────────── */}
      <div className="neon-card p-3 sm:p-4 flex flex-wrap gap-2 sm:gap-4 items-center">
        <div className="flex items-center gap-2 flex-1 sm:flex-none min-w-0">
          <span className="text-xs text-[var(--text-muted)] tracking-wider whitespace-nowrap">総資金</span>
          <select
            value={bankroll}
            onChange={e => setBankroll(Number(e.target.value))}
            className="flex-1 sm:flex-none rounded bg-[var(--bg-card)] border border-[rgba(200,168,130,0.2)] px-2 py-1 text-sm text-[var(--text-primary)] focus:outline-none focus:border-[var(--neon-cyan)] min-w-0"
          >
            {BANKROLL_OPTIONS.map(v => (
              <option key={v} value={v}>¥{v.toLocaleString()}</option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-2 flex-1 sm:flex-none min-w-0">
          <span className="text-xs text-[var(--text-muted)] tracking-wider whitespace-nowrap">Kelly</span>
          <select
            value={kellyFrac}
            onChange={e => setKellyFrac(Number(e.target.value))}
            className="flex-1 sm:flex-none rounded bg-[var(--bg-card)] border border-[rgba(200,168,130,0.2)] px-2 py-1 text-sm text-[var(--text-primary)] focus:outline-none focus:border-[var(--neon-cyan)] min-w-0"
          >
            {KELLY_OPTIONS.map(({ value, label }) => (
              <option key={value} value={value}>{label}</option>
            ))}
          </select>
        </div>

        <div className="w-full sm:w-auto sm:ml-auto text-xs text-[var(--text-muted)] text-right sm:text-left">
          推奨購入あり:&nbsp;
          <span className="font-semibold text-[var(--text-primary)]">{activeCount}件</span>
        </div>

        {/* ── リアルタイムオッズ更新タイムスタンプ ── */}
        {raceId && (
          <div className="w-full sm:w-auto text-[10px] text-right sm:text-left">
            {oddsUpdatedAt ? (
              <span className="text-[var(--neon-cyan)] opacity-70">
                ⚡ オッズ更新&nbsp;{oddsUpdatedAt.slice(11, 16)}
              </span>
            ) : (
              <span className="text-[var(--text-muted)] opacity-50">
                オッズ取得中…
              </span>
            )}
          </div>
        )}
      </div>

      {/* ── テーブル（横スクロール対応） ─────────────────── */}
      <div className="neon-card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full min-w-[480px] text-sm">
            <thead>
              <tr className="border-b border-[rgba(200,168,130,0.12)] text-[var(--text-muted)] text-xs tracking-wider">
                <th className="px-3 sm:px-4 py-2 sm:py-3 text-left whitespace-nowrap">種別</th>
                <th className="px-3 sm:px-4 py-2 sm:py-3 text-left whitespace-nowrap">馬番</th>
                <th className="px-3 sm:px-4 py-2 sm:py-3 text-right whitespace-nowrap">オッズ</th>
                <th className="px-3 sm:px-4 py-2 sm:py-3 text-right whitespace-nowrap">EV</th>
                <th className="px-3 sm:px-4 py-2 sm:py-3 text-right whitespace-nowrap">f*</th>
                <th className="px-3 sm:px-4 py-2 sm:py-3 text-right whitespace-nowrap">推奨購入額</th>
                <th className="px-3 sm:px-4 py-2 sm:py-3 text-center whitespace-nowrap">根拠</th>
                {hasConfirmed && <th className="px-3 sm:px-4 py-2 sm:py-3 text-right whitespace-nowrap">結果</th>}
              </tr>
            </thead>
            <tbody className="divide-y divide-[rgba(200,168,130,0.08)]">
              {rows.map(row => {
                const isFlashing =
                  row.primaryHorseNum != null && flashNums.has(row.primaryHorseNum)

                const rowClass = [
                  row.isHit === 1
                    ? 'bg-[rgba(220,80,80,0.15)]'
                    : row.isHit === 0
                    ? 'opacity-40'
                    : row.skip
                    ? 'opacity-40'
                    : 'hover:bg-[rgba(200,168,130,0.04)]',
                  isFlashing ? 'odds-flash' : '',
                ].join(' ').trim()

                return (
                  <Fragment key={row.id}>
                  <tr className={rowClass}>
                    <td className="px-3 sm:px-4 py-2 sm:py-3 font-semibold text-[var(--text-primary)] whitespace-nowrap">
                      {row.betType}
                    </td>
                    <td className="px-3 sm:px-4 py-2 sm:py-3 text-[var(--text-secondary)] font-mono text-xs sm:text-sm">
                      {row.horseNums}
                    </td>
                    <td className="px-3 sm:px-4 py-2 sm:py-3 text-right text-[var(--text-secondary)] font-mono whitespace-nowrap">
                      {row.noOdds ? '—' : row.odds > 0 ? `${row.odds.toFixed(1)}倍` : '—'}
                    </td>
                    <td className={`px-3 sm:px-4 py-2 sm:py-3 text-right font-mono font-semibold whitespace-nowrap ${
                      row.ev >= 1.0 ? 'text-green-400' : 'text-[var(--text-muted)]'
                    }`}>
                      {row.ev > 0 ? row.ev.toFixed(2) : '—'}
                    </td>
                    <td className="px-3 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs text-[var(--text-muted)] whitespace-nowrap">
                      {row.noOdds ? '—' : row.f > 0 ? `${(row.f * 100).toFixed(1)}%` : '—'}
                    </td>
                    <td className="px-3 sm:px-4 py-2 sm:py-3 text-right whitespace-nowrap">
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
                    <td className="px-3 sm:px-4 py-2 sm:py-3 text-center whitespace-nowrap">
                      <button
                        onClick={() => fetchShap(row.id)}
                        title="AI根拠を表示"
                        className={`text-xs px-2 py-0.5 rounded border transition-colors ${
                          shapOpenId === row.id
                            ? 'border-[var(--neon-cyan)] text-[var(--neon-cyan)] bg-[rgba(200,168,130,0.08)]'
                            : 'border-[rgba(200,168,130,0.3)] text-[var(--text-muted)] hover:border-[var(--neon-cyan)] hover:text-[var(--neon-cyan)]'
                        }`}
                      >
                        🔍
                      </button>
                    </td>
                    {hasConfirmed && (
                      <td className="px-3 sm:px-4 py-2 sm:py-3 text-right whitespace-nowrap">
                        {row.isHit === 1 ? (
                          <span className="text-xs font-bold text-red-400">
                            ✓ 的中{row.hitPayout != null ? `  ¥${row.hitPayout.toLocaleString()}` : ''}
                          </span>
                        ) : row.isHit === 0 ? (
                          <span className="text-xs text-[var(--text-muted)]">✗ 外れ</span>
                        ) : (
                          <span className="text-xs text-[var(--text-muted)] opacity-40">—</span>
                        )}
                      </td>
                    )}
                  </tr>
                  {/* ── SHAP インラインパネル ── */}
                  {shapOpenId === row.id && (
                    <tr className="bg-[rgba(0,0,20,0.4)]">
                      <td colSpan={hasConfirmed ? 8 : 7} className="px-4 py-3">
                        <ShapPanel data={shapCache[row.id]} />
                      </td>
                    </tr>
                  )}
                  </Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
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

// ── SHAP 可視化パネル ──────────────────────────────────────────

function ShapPanel({ data }: { data: ShapHorse[] | 'loading' | undefined }) {
  if (data === 'loading' || data === undefined) {
    return (
      <p className="text-xs text-[var(--text-muted)] animate-pulse">
        AI根拠データ取得中…
      </p>
    )
  }

  const horses = data.filter(h => h.shap && h.shap.length > 0)

  if (horses.length === 0) {
    return (
      <p className="text-xs text-[var(--text-muted)] opacity-60">
        SHAP 未計算（新しいレースの予想から自動生成されます）
      </p>
    )
  }

  return (
    <div className="space-y-4">
      {horses.map((horse, hi) => {
        const entries = horse.shap!
        const maxAbs = Math.max(...entries.map(e => Math.abs(e.value)), 1e-9)
        return (
          <div key={hi}>
            {horses.length > 1 && (
              <p className="text-[10px] text-[var(--text-muted)] mb-1 font-semibold tracking-wider">
                {horse.horse_name}
              </p>
            )}
            <div className="space-y-1">
              {entries.map((e, ei) => {
                const pct = (Math.abs(e.value) / maxAbs) * 100
                const isPos = e.value >= 0
                return (
                  <div key={ei} className="flex items-center gap-2 text-[11px]">
                    <span className="w-32 text-right text-[var(--text-muted)] truncate shrink-0">
                      {e.label}
                    </span>
                    <div className="flex-1 bg-[rgba(255,255,255,0.05)] rounded-sm h-3 overflow-hidden">
                      <div
                        className={`h-full rounded-sm ${isPos ? 'bg-blue-500' : 'bg-red-500'}`}
                        style={{ width: `${pct.toFixed(1)}%`, opacity: 0.85 }}
                      />
                    </div>
                    <span className={`w-14 text-right font-mono shrink-0 ${isPos ? 'text-blue-400' : 'text-red-400'}`}>
                      {isPos ? '+' : ''}{e.value.toFixed(3)}
                    </span>
                  </div>
                )
              })}
            </div>
          </div>
        )
      })}
      <p className="text-[9px] text-[var(--text-muted)] opacity-50 mt-1">
        🔵 プラス寄与（推奨方向）　🔴 マイナス寄与（抑制方向）
      </p>
    </div>
  )
}
