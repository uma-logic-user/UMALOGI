'use client'

interface HorseRank {
  horse_number: number
  horse_name: string
  win_prob: number
  rank: 'S' | 'A' | 'B' | 'C'
}

interface Win5Race {
  race_id: string
  race_name: string
  venue: string
  distance: number
  surface: string
}

interface Win5Entry {
  date: string
  race_ids: string[]
  races: Win5Race[]
  selections: Record<string, number[]>
  horse_ranks: Record<string, HorseRank[]>
  total_combinations: number
  is_hit: number
  payout: number
  actual_numbers: number[]       // 確定結果: 各レースの1着馬番 (5つ)
  per_race_hit: Record<string, boolean>  // レース別的中フラグ
  notes: string
}

interface Props {
  data: Win5Entry[]
}

const RANK_COLOR: Record<string, string> = {
  S: 'text-[#ff4757] font-black',
  A: 'text-[#ffa502] font-bold',
  B: 'text-[#2ed573] font-semibold',
  C: 'text-[var(--text-secondary)]',
}

const RANK_BG: Record<string, string> = {
  S: 'bg-[rgba(255,71,87,0.15)] border border-[rgba(255,71,87,0.4)]',
  A: 'bg-[rgba(255,165,2,0.15)] border border-[rgba(255,165,2,0.4)]',
  B: 'bg-[rgba(46,213,115,0.12)] border border-[rgba(46,213,115,0.3)]',
  C: 'bg-[rgba(255,255,255,0.04)] border border-[rgba(255,255,255,0.08)]',
}

export default function Win5Panel({ data }: Props) {
  if (!data || data.length === 0) {
    return (
      <div className="p-6 text-center text-[var(--text-secondary)]">
        WIN5 予想データがありません
      </div>
    )
  }

  const totalHits = data.filter(e => e.is_hit === 1).length
  const totalPayout = data.reduce((s, e) => s + (e.payout ?? 0), 0)

  return (
    <div className="space-y-4 p-4 w-full min-w-0">
      {/* ヘッダー */}
      <div className="flex flex-wrap items-center gap-3 mb-4">
        <div className="flex items-center gap-2">
          <span className="text-xl">🏆</span>
          <h2 className="text-lg font-bold" style={{ color: 'var(--neon-gold)' }}>
            WIN5 ガチ予想
          </h2>
        </div>
        <span className="text-xs text-[var(--text-secondary)]">S≥30% A≥18% B≥9% C&lt;9%</span>
        <div className="flex items-center gap-3 ml-auto text-xs text-[var(--text-secondary)]">
          <span>的中 <strong className="text-[var(--neon-gold)]">{totalHits}</strong>/{data.length}回</span>
          {totalPayout > 0 && (
            <span>累計払戻 <strong className="text-[var(--neon-gold)]">¥{totalPayout.toLocaleString()}</strong></span>
          )}
        </div>
      </div>

      {data.map((entry, idx) => {
        const hasResult = (entry.actual_numbers ?? []).length === 5
        const hitCount = hasResult
          ? Object.values(entry.per_race_hit ?? {}).filter(Boolean).length
          : null

        return (
          <div
            key={idx}
            className="rounded-lg overflow-hidden"
            style={{
              background: entry.is_hit
                ? 'rgba(0,200,100,0.08)'
                : 'rgba(255,255,255,0.03)',
              border: entry.is_hit
                ? '1px solid rgba(0,200,100,0.4)'
                : '1px solid rgba(255,255,255,0.08)',
            }}
          >
            {/* ── ヘッダー ─────────────────────────── */}
            <div className="flex items-center justify-between px-4 py-2"
                 style={{ borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
              <div className="flex items-center gap-3">
                <span className="text-sm font-mono text-[var(--text-secondary)]">{entry.date}</span>
                {entry.is_hit ? (
                  <span className="px-2 py-0.5 rounded text-xs font-bold"
                        style={{ background: 'rgba(0,200,100,0.2)', color: '#00c864', border: '1px solid rgba(0,200,100,0.4)' }}>
                    🎯 的中！
                  </span>
                ) : hasResult ? (
                  <span className="px-2 py-0.5 rounded text-xs"
                        style={{ background: 'rgba(255,255,255,0.06)', color: 'var(--text-secondary)' }}>
                    ✗ 外れ ({hitCount}/5R通過)
                  </span>
                ) : null}
              </div>
              <div className="flex items-center gap-3">
                <span className="text-xs text-[var(--text-secondary)]">{entry.total_combinations}点</span>
                {entry.payout > 0 && (
                  <span className="font-mono font-bold" style={{ color: 'var(--neon-gold)' }}>
                    ¥{entry.payout.toLocaleString()}
                  </span>
                )}
              </div>
            </div>

            {/* ── レース別行 ─────────────────────── */}
            <div className="p-3 space-y-2">
              {entry.race_ids.map((raceId, ri) => {
                const race = entry.races[ri] ?? { race_id: raceId, race_name: raceId, venue: '', surface: '', distance: 0 }
                const ranks = entry.horse_ranks?.[raceId] ?? []
                const selected = new Set(entry.selections?.[raceId] ?? [])
                const actualNum: number | undefined = (entry.actual_numbers ?? [])[ri]
                const raceHit = entry.per_race_hit?.[raceId]

                return (
                  <div key={raceId} className="flex items-start gap-3">
                    {/* レース名 */}
                    <div className="flex-shrink-0 w-28 sm:w-36">
                      <div className="text-xs font-semibold truncate" style={{ color: 'var(--text-primary)' }}>
                        {race.race_name}
                      </div>
                      <div className="text-xs text-[var(--text-secondary)]">
                        {race.venue} {race.surface}{race.distance}m
                      </div>
                    </div>

                    {/* 予想馬 (SABC ランクバッジ) */}
                    <div className="flex flex-wrap gap-1 min-w-0 flex-1">
                      {ranks.map(h => {
                        const isSel = selected.has(h.horse_number)
                        const isActual = actualNum !== undefined && h.horse_number === actualNum
                        return (
                          <div
                            key={h.horse_number}
                            className={`flex items-center gap-1 px-1.5 py-0.5 rounded text-xs ${RANK_BG[h.rank]}`}
                            style={
                              isActual
                                ? { boxShadow: '0 0 8px rgba(255,215,0,0.7)', outline: '2px solid rgba(255,215,0,0.8)' }
                                : isSel
                                  ? { boxShadow: '0 0 6px rgba(0,200,255,0.5)', outline: '1px solid rgba(0,200,255,0.5)' }
                                  : {}
                            }
                          >
                            <span className={`font-mono text-xs ${RANK_COLOR[h.rank]}`}>{h.rank}</span>
                            <span className="text-[var(--text-secondary)] text-xs">{h.horse_number}</span>
                            <span className="text-xs">{h.horse_name}</span>
                            <span className="text-xs text-[var(--text-secondary)]">
                              {(h.win_prob * 100).toFixed(0)}%
                            </span>
                            {isSel && <span className="text-xs font-bold" style={{ color: 'var(--neon-cyan)' }}>◀</span>}
                            {isActual && <span className="text-xs font-bold" style={{ color: 'var(--neon-gold)' }}>★</span>}
                          </div>
                        )
                      })}
                    </div>

                    {/* 実結果バッジ */}
                    {actualNum !== undefined && (
                      <div className="flex-shrink-0 flex items-center gap-1.5">
                        <div
                          className="flex items-center gap-1 px-2 py-0.5 rounded text-xs font-bold"
                          style={
                            raceHit
                              ? { background: 'rgba(0,200,100,0.2)', color: '#00c864', border: '1px solid rgba(0,200,100,0.5)' }
                              : { background: 'rgba(255,255,255,0.06)', color: 'var(--text-secondary)' }
                          }
                        >
                          {raceHit ? '✓' : '✗'}
                          <span className="font-mono">{actualNum}番</span>
                        </div>
                      </div>
                    )}
                  </div>
                )
              })}
            </div>

            {/* ── 結果未取得の場合のプレースホルダー ── */}
            {!hasResult && (
              <div className="px-4 py-2 text-xs text-[var(--text-secondary)] opacity-60"
                   style={{ borderTop: '1px solid rgba(255,255,255,0.04)' }}>
                確定結果は全レース終了後（17:15以降）に自動取得されます
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}
