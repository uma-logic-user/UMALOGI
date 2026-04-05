import type { RaceResult } from '@/types/race'

interface Props { results: RaceResult[] }

export default function PedigreeChart({ results }: Props) {
  // 父の出現頻度を集計
  const sireCount: Record<string, { count: number; placings: number }> = {}
  for (const r of results) {
    if (!r.sire) continue
    if (!sireCount[r.sire]) sireCount[r.sire] = { count: 0, placings: 0 }
    sireCount[r.sire].count++
    if (r.rank != null && r.rank <= 3) sireCount[r.sire].placings++
  }

  const entries = Object.entries(sireCount).sort((a, b) => b[1].count - a[1].count)
  const maxCount = entries[0]?.[1].count ?? 1

  return (
    <div className="neon-card p-5 slide-in" style={{ animationDelay: '0.32s' }}>
      <div className="text-xs neon-text tracking-[0.2em] font-semibold mb-4">
        SIRE DISTRIBUTION
      </div>
      <div className="space-y-2">
        {entries.map(([sire, { count, placings }]) => (
          <div key={sire} className="flex items-center gap-3">
            {/* 父名 */}
            <div className="w-36 text-xs text-right truncate text-[var(--text-primary)]">
              {sire}
            </div>
            {/* バー */}
            <div className="flex-1 relative h-5 rounded overflow-hidden"
              style={{ background: 'rgba(0,200,255,0.05)', border: '1px solid rgba(0,200,255,0.1)' }}>
              <div
                className="absolute inset-y-0 left-0 rounded transition-all"
                style={{
                  width: `${(count / maxCount) * 100}%`,
                  background: placings > 0
                    ? 'linear-gradient(90deg, rgba(0,200,255,0.3), rgba(0,200,255,0.1))'
                    : 'rgba(0,200,255,0.08)',
                  boxShadow: placings > 0 ? 'inset 0 0 8px rgba(0,200,255,0.2)' : 'none',
                }}
              />
              <div className="absolute inset-y-0 left-2 flex items-center text-[10px] font-mono"
                style={{ color: placings > 0 ? 'var(--neon-cyan)' : 'var(--text-muted)' }}>
                {count}頭
                {placings > 0 && (
                  <span className="ml-1 text-[var(--neon-green)]">/ {placings}着内</span>
                )}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
