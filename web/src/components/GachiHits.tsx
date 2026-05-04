'use client'

interface GachiHit {
  race_id: string
  race_name: string
  date: string
  venue: string
  surface: string
  distance: number
  model_type: string
  bet_type: string
  combination_json: string
  payout: number
  is_hit: number
  rank: string | null
  notes: string
}

interface Props {
  data: GachiHit[]
}

const RANK_STYLE: Record<string, { bg: string; border: string; text: string; label: string }> = {
  S: { bg: 'rgba(255,71,87,0.15)',  border: 'rgba(255,71,87,0.5)',  text: '#ff4757', label: '💎 SUPER HIT' },
  A: { bg: 'rgba(255,165,2,0.15)', border: 'rgba(255,165,2,0.5)',  text: '#ffa502', label: '🥇 BIG HIT' },
  B: { bg: 'rgba(46,213,115,0.12)', border: 'rgba(46,213,115,0.4)', text: '#2ed573', label: '🎯 HIT' },
  C: { bg: 'rgba(0,200,255,0.08)',  border: 'rgba(0,200,255,0.3)',  text: '#00c8ff', label: '✓ 的中' },
}

function parseCombination(json: string): string {
  try {
    const parsed = JSON.parse(json)
    if (!Array.isArray(parsed)) return json
    return parsed
      .map((c: number[]) => Array.isArray(c) ? c.join('-') : String(c))
      .join(', ')
  } catch {
    return json
  }
}

export default function GachiHits({ data }: Props) {
  const hits = data.filter(d => d.is_hit === 1)
  const nonHits = data.filter(d => d.is_hit !== 1).slice(0, 20)

  return (
    <div className="space-y-4 p-4">
      {/* ヒットセクション */}
      <div className="flex items-center gap-2 mb-2">
        <span className="text-xl">🎯</span>
        <h2 className="text-lg font-bold" style={{ color: 'var(--neon-gold)', textShadow: '0 0 10px rgba(255,215,0,0.5)' }}>
          AIガチ予想・的中実績
        </h2>
        {hits.length > 0 && (
          <span className="ml-auto text-sm font-bold px-2 py-1 rounded"
                style={{ background: 'rgba(255,215,0,0.15)', color: 'var(--neon-gold)', border: '1px solid rgba(255,215,0,0.3)' }}>
            {hits.length}件的中
          </span>
        )}
      </div>

      {hits.length === 0 && (
        <div className="text-center py-6 text-[var(--text-secondary)]">
          まだ的中実績がありません
        </div>
      )}

      {hits.map((h, idx) => {
        const style = RANK_STYLE[h.rank ?? 'C'] ?? RANK_STYLE.C
        return (
          <div key={idx} className="rounded-lg p-4 relative overflow-hidden"
               style={{ background: style.bg, border: `1px solid ${style.border}` }}>
            {/* キラキラエフェクト */}
            <div className="absolute inset-0 pointer-events-none"
                 style={{ background: `radial-gradient(ellipse at top left, ${style.border} 0%, transparent 60%)`, opacity: 0.3 }} />

            <div className="relative flex flex-col sm:flex-row items-start justify-between gap-2 sm:gap-4">
              <div>
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-sm font-bold px-2 py-0.5 rounded"
                        style={{ background: style.bg, color: style.text, border: `1px solid ${style.border}` }}>
                    {style.label}
                  </span>
                  <span className="text-xs text-[var(--text-secondary)]">{h.date}</span>
                </div>
                <div className="font-bold text-base" style={{ color: 'var(--text-primary)' }}>
                  {h.race_name}
                </div>
                <div className="text-xs text-[var(--text-secondary)] mt-0.5">
                  {h.venue} {h.surface}{h.distance}m ／ {h.model_type} {h.bet_type}
                </div>
                <div className="mt-1 font-mono text-sm" style={{ color: style.text }}>
                  買い目: {parseCombination(h.combination_json)}
                </div>
              </div>
              <div className="flex-shrink-0 sm:text-right">
                <div className="text-xl sm:text-2xl font-black font-mono" style={{ color: style.text, textShadow: `0 0 15px ${style.border}` }}>
                  ¥{h.payout.toLocaleString()}
                </div>
                <div className="text-xs text-[var(--text-secondary)] mt-1">払戻</div>
              </div>
            </div>
          </div>
        )
      })}

      {/* 未的中分（最近20件） */}
      {nonHits.length > 0 && (
        <>
          <div className="text-xs text-[var(--text-secondary)] mt-4 mb-1">最近の予想（未的中）</div>
          <div className="space-y-1">
            {nonHits.map((h, idx) => (
              <div key={idx} className="flex items-center gap-3 px-3 py-2 rounded text-xs"
                   style={{ background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.06)' }}>
                <span className="text-[var(--text-secondary)] font-mono w-24 flex-shrink-0">{h.date}</span>
                <span className="font-semibold flex-1 truncate">{h.race_name}</span>
                <span className="text-[var(--text-secondary)] flex-shrink-0">{h.bet_type}</span>
                <span className="font-mono text-[var(--text-secondary)] flex-shrink-0">
                  {parseCombination(h.combination_json).slice(0, 20)}
                </span>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  )
}
