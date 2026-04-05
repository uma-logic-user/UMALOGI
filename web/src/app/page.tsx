import racesJson      from '@/data/races.json'
import predictionsJson from '@/data/predictions.json'
import summaryJson     from '@/data/summary.json'
import type { RaceEntry } from '@/types/race'
import NavBar        from '@/components/NavBar'
import RaceHeader    from '@/components/RaceHeader'
import StatCards     from '@/components/StatCards'
import RaceTable     from '@/components/RaceTable'
import PedigreeChart from '@/components/PedigreeChart'

const races       = racesJson       as RaceEntry[]
const predictions = predictionsJson as unknown[]
const summary     = summaryJson     as { total_races_in_db: number; overall: Record<string, unknown> }

export default function HomePage() {
  // ── データガード ────────────────────────────────────────────────
  const hasRaces       = Array.isArray(races) && races.length > 0
  const hasPredictions = Array.isArray(predictions) && predictions.length > 0

  // 最新（先頭）レースを詳細表示
  const featured = hasRaces ? races[0] : null

  return (
    <div className="relative z-10 min-h-screen flex flex-col">
      <NavBar />

      <main className="flex-1 max-w-[1400px] mx-auto w-full px-4 py-6 space-y-5">

        {/* ── パンくず ─────────────────────────────────────────── */}
        <div className="text-xs text-[var(--text-muted)] tracking-widest flex items-center gap-2">
          <span>HOME</span>
          <span className="text-[rgba(0,200,255,0.3)]">›</span>
          <span>RACES</span>
          {featured && (
            <>
              <span className="text-[rgba(0,200,255,0.3)]">›</span>
              <span className="neon-text">{featured.race_name}</span>
            </>
          )}
        </div>

        {/* ── DB サマリーバッジ ────────────────────────────────── */}
        <div className="flex gap-3 flex-wrap">
          <SummaryBadge label="DB総レース数" value={`${summary?.total_races_in_db ?? 0} races`} />
          <SummaryBadge label="取得済み" value={`${races.length} races`} />
          <SummaryBadge
            label="予想データ"
            value={hasPredictions ? `${predictions.length} predictions` : 'NO DATA'}
            dim={!hasPredictions}
          />
        </div>

        {/* ── メインコンテンツ ─────────────────────────────────── */}
        {featured ? (
          <>
            {/* 最新レース詳細 */}
            <RaceHeader race={featured} />
            <StatCards  results={featured.results} />

            <div className="grid grid-cols-1 xl:grid-cols-[1fr_280px] gap-5">
              <RaceTable     results={featured.results} />
              <PedigreeChart results={featured.results} />
            </div>
          </>
        ) : (
          /* レースデータなし */
          <div className="neon-card p-12 text-center slide-in">
            <div className="text-4xl mb-4 opacity-30">🏇</div>
            <div className="neon-text text-lg tracking-[0.3em] mb-2">NO RACE DATA</div>
            <div className="text-xs text-[var(--text-muted)] tracking-widest">
              python web/generate_data.py を実行してデータを生成してください
            </div>
          </div>
        )}

        {/* ── 予想セクション ───────────────────────────────────── */}
        {!hasPredictions && (
          <div
            className="neon-card p-6 text-center"
            style={{ border: '1px solid rgba(0,200,255,0.08)' }}
          >
            <div className="text-xs neon-text tracking-[0.3em] mb-1">PREDICTIONS</div>
            <div className="text-[var(--text-muted)] text-sm tracking-widest">
              NO PREDICTIONS AVAILABLE
            </div>
            <div className="text-[10px] text-[var(--text-muted)] mt-2 opacity-50">
              予想モデルの実行後にここに表示されます
            </div>
          </div>
        )}

        {/* ── 全レース一覧テーブル ─────────────────────────────── */}
        {hasRaces && races.length > 1 && (
          <div className="neon-card overflow-hidden slide-in">
            <div className="px-4 py-3 border-b border-[rgba(0,200,255,0.12)]">
              <span className="text-xs neon-text tracking-[0.2em] font-semibold">
                ALL RACES — {races.length} RECORDS
              </span>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full race-table">
                <thead>
                  <tr>
                    <th>日付</th>
                    <th>会場</th>
                    <th>レース名</th>
                    <th className="text-center">R</th>
                    <th>距離</th>
                    <th className="text-center">頭数</th>
                    <th>優勝馬</th>
                    <th className="text-right">単勝</th>
                  </tr>
                </thead>
                <tbody>
                  {races.map((r) => {
                    const winner = r.results?.find(h => h.rank === 1)
                    return (
                      <tr key={r.race_id}>
                        <td className="font-mono text-xs text-[var(--text-muted)]">{r.date}</td>
                        <td className="text-xs">{r.venue}</td>
                        <td className="font-semibold text-[var(--text-primary)]">{r.race_name}</td>
                        <td className="text-center text-xs text-[var(--text-muted)]">{r.race_number}</td>
                        <td className="text-xs text-[var(--text-muted)]">
                          {r.surface}{r.distance}m
                        </td>
                        <td className="text-center text-xs">{r.results?.length ?? 0}頭</td>
                        <td className="text-sm">
                          {winner
                            ? <span className="neon-text-gold">{winner.horse_name}</span>
                            : <span className="text-[var(--text-muted)]">—</span>}
                        </td>
                        <td className="text-right font-mono text-xs">
                          {winner?.win_odds != null
                            ? <span className="text-[var(--neon-cyan)]">{winner.win_odds.toFixed(1)}</span>
                            : <span className="text-[var(--text-muted)]">—</span>}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* ── フッター ─────────────────────────────────────────── */}
        <footer className="text-center text-xs text-[var(--text-muted)] tracking-widest pt-4 pb-2 border-t border-[rgba(0,200,255,0.08)]">
          UMALOGI &nbsp;·&nbsp; データソース: netkeiba.com
          {featured && <>&nbsp;·&nbsp; 最終更新: {featured.date}</>}
        </footer>

      </main>
    </div>
  )
}

function SummaryBadge({
  label, value, dim = false,
}: { label: string; value: string; dim?: boolean }) {
  return (
    <div
      className="flex items-center gap-2 px-3 py-1.5 rounded text-xs"
      style={{
        background: 'rgba(0,200,255,0.04)',
        border: `1px solid rgba(0,200,255,${dim ? '0.08' : '0.2'})`,
        opacity: dim ? 0.5 : 1,
      }}
    >
      <span className="text-[var(--text-muted)] tracking-wider">{label}</span>
      <span className={dim ? 'text-[var(--text-muted)]' : 'neon-text font-semibold'}>{value}</span>
    </div>
  )
}
