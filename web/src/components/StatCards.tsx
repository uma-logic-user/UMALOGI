import type { RaceResult } from '@/types/race'

interface Props { results: RaceResult[] }

export default function StatCards({ results }: Props) {
  const winner        = results.find(r => r.rank === 1)
  const favorite      = results.find(r => r.popularity === 1)
  const maxOdds       = Math.max(...results.map(r => r.win_odds ?? 0))
  const biggestUpset  = results.find(r => r.win_odds === maxOdds)

  const stats = [
    {
      label:    '出走頭数',
      value:    `${results.length}頭`,
      sub:      `${results.filter(r => r.rank !== null).length}頭完走`,
      color:    'var(--neon-cyan)',
    },
    {
      label:    '優勝馬',
      value:    winner?.horse_name ?? '—',
      sub:      winner?.finish_time ? `⏱ ${winner.finish_time}` : '—',
      color:    'var(--neon-gold)',
    },
    {
      label:    '1番人気',
      value:    favorite?.horse_name ?? '—',
      sub:      favorite?.rank != null ? `${favorite.rank}着 / ${favorite.win_odds}倍` : '—',
      color:    favorite?.rank === 1 ? 'var(--neon-green)' : 'var(--neon-red)',
    },
    {
      label:    '最高オッズ',
      value:    `${maxOdds}倍`,
      sub:      biggestUpset?.horse_name ?? '—',
      color:    'var(--neon-red)',
    },
  ]

  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
      {stats.map((s, i) => (
        <div
          key={i}
          className="neon-card p-4 slide-in"
          style={{ animationDelay: `${i * 0.06}s` }}
        >
          <div className="text-xs text-[var(--text-muted)] tracking-widest mb-2 uppercase">
            {s.label}
          </div>
          <div
            className="text-xl font-bold truncate"
            style={{
              color: s.color,
              textShadow: `0 0 8px ${s.color}88`,
            }}
          >
            {s.value}
          </div>
          <div className="text-xs text-[var(--text-muted)] mt-1 truncate">{s.sub}</div>
        </div>
      ))}
    </div>
  )
}
