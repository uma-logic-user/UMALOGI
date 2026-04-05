import racesJson from '@/data/races.json'
import type { RaceData } from '@/types/race'
import NavBar from '@/components/NavBar'
import RaceHeader from '@/components/RaceHeader'
import StatCards from '@/components/StatCards'
import RaceTable from '@/components/RaceTable'
import PedigreeChart from '@/components/PedigreeChart'

const data = racesJson as RaceData

export default function HomePage() {
  const { race, results } = data

  return (
    <div className="relative z-10 min-h-screen flex flex-col">
      <NavBar />

      <main className="flex-1 max-w-[1400px] mx-auto w-full px-4 py-6 space-y-5">

        {/* ── パンくず ────────────────────────────────── */}
        <div className="text-xs text-[var(--text-muted)] tracking-widest flex items-center gap-2">
          <span>HOME</span>
          <span className="text-[rgba(0,200,255,0.3)]">›</span>
          <span>RACES</span>
          <span className="text-[rgba(0,200,255,0.3)]">›</span>
          <span className="neon-text">{race.race_name}</span>
        </div>

        {/* ── レース情報カード ──────────────────────────── */}
        <RaceHeader race={race} />

        {/* ── スタッツカード ────────────────────────────── */}
        <StatCards results={results} />

        {/* ── メインコンテンツ（結果テーブル + 血統チャート） ─ */}
        <div className="grid grid-cols-1 xl:grid-cols-[1fr_280px] gap-5">
          <RaceTable results={results} />
          <PedigreeChart results={results} />
        </div>

        {/* ── フッター ─────────────────────────────────── */}
        <footer className="text-center text-xs text-[var(--text-muted)] tracking-widest pt-4 pb-2 border-t border-[rgba(0,200,255,0.08)]">
          UMALOGI &nbsp;·&nbsp; データソース: netkeiba.com &nbsp;·&nbsp; {race.date}
        </footer>
      </main>
    </div>
  )
}
