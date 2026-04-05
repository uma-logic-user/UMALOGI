import type { Race } from '@/types/race'

interface Props { race: Race }

export default function RaceHeader({ race }: Props) {
  const [year, month, day] = race.date.split('/')

  return (
    <div className="neon-card-bright p-6 slide-in">
      {/* レース名 */}
      <div className="flex items-start justify-between mb-4">
        <div>
          <div className="text-xs text-[var(--text-muted)] tracking-[0.2em] mb-1">
            {race.date} &nbsp;|&nbsp; {race.venue} &nbsp;|&nbsp; 第{race.race_number}回
          </div>
          <h1 className="text-2xl font-bold neon-text tracking-wider">
            {race.race_name}
          </h1>
        </div>
        <div className="text-right">
          <div className="text-xs text-[var(--text-muted)] mb-1">RACE ID</div>
          <div className="font-mono text-sm text-[var(--neon-cyan)] opacity-70">
            {race.race_id}
          </div>
        </div>
      </div>

      {/* 詳細バッジ */}
      <div className="flex flex-wrap gap-3">
        <Badge label="距離" value={`${race.surface} ${race.distance}m`} />
        <Badge label="天候" value={race.weather || '—'} />
        <Badge label="馬場" value={race.condition || '—'} highlight={race.condition === '良'} />
        <Badge label="会場" value={race.venue} />
      </div>
    </div>
  )
}

function Badge({
  label,
  value,
  highlight = false,
}: {
  label: string
  value: string
  highlight?: boolean
}) {
  return (
    <div
      className="flex items-center gap-2 px-3 py-1.5 rounded text-xs"
      style={{
        background: 'rgba(0,200,255,0.06)',
        border: '1px solid rgba(0,200,255,0.2)',
      }}
    >
      <span className="text-[var(--text-muted)] tracking-wider">{label}</span>
      <span
        className={highlight ? 'neon-text-green font-semibold' : 'text-[var(--text-primary)] font-semibold'}
      >
        {value}
      </span>
    </div>
  )
}
