'use client'

import { useState, useMemo } from 'react'
import type { RaceEntry, Prediction } from '@/types/race'
import NavBar     from './NavBar'
import RaceTree   from './RaceTree'
import RaceDetail from './RaceDetail'
import HitHistory from './HitHistory'
import TabView    from './TabView'

type View = 'race' | 'hits' | 'dashboard'

interface Props {
  races:       RaceEntry[]
  predictions: Prediction[]
  summary:     { total_races_in_db: number; overall: Record<string, unknown> }
}

export default function AppShell({ races, predictions, summary }: Props) {
  const [view, setView]                     = useState<View>('dashboard')
  const [selectedRaceId, setSelectedRaceId] = useState<string | null>(null)

  const selectedRace = useMemo(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    () => races.find(r => r.race_id === selectedRaceId) as any ?? null,
    [races, selectedRaceId],
  )

  const racePredictions = useMemo(
    () => selectedRaceId
      ? predictions.filter(p => p.race_id === selectedRaceId)
      : [],
    [predictions, selectedRaceId],
  )

  function handleSelectRace(raceId: string) {
    setSelectedRaceId(raceId)
    setView('race')
  }

  const hits = predictions.filter(p => p.is_hit === 1)

  return (
    <div className="app-shell">
      {/* ── NavBar ──────────────────────────────────── */}
      <div className="app-navbar">
        <NavBar />
      </div>

      {/* ── Sidebar ─────────────────────────────────── */}
      <aside className="app-sidebar">
        {/* 特別ボタン群 */}
        <div className="border-b border-[rgba(0,200,255,0.1)] py-1">
          <button
            className={`sidebar-special-btn ${view === 'hits' ? 'active' : ''}`}
            onClick={() => setView('hits')}
          >
            <span style={{
              color: 'var(--neon-gold)',
              textShadow: '0 0 8px rgba(255,215,0,0.7)',
              fontSize: '1rem',
            }}>★</span>
            <span style={{ color: view === 'hits' ? 'var(--neon-gold)' : 'var(--text-primary)' }}>
              的中実績
            </span>
            {hits.length > 0 && (
              <span
                className="ml-auto text-xs font-bold px-1.5 py-0.5 rounded"
                style={{
                  background: 'rgba(255,215,0,0.15)',
                  color: 'var(--neon-gold)',
                  border: '1px solid rgba(255,215,0,0.3)',
                }}
              >
                {hits.length}
              </span>
            )}
          </button>
          <button
            className={`sidebar-special-btn ${view === 'dashboard' ? 'active' : ''}`}
            onClick={() => setView('dashboard')}
          >
            <span style={{ color: 'var(--neon-cyan)', fontSize: '0.9rem' }}>⬡</span>
            <span style={{ color: view === 'dashboard' ? 'var(--neon-cyan)' : 'var(--text-primary)' }}>
              ダッシュボード
            </span>
          </button>
        </div>

        {/* レースツリー */}
        <div className="py-1">
          <div className="px-3 py-2 text-xs text-[var(--text-muted)] tracking-[0.15em] uppercase">
            Race Explorer
          </div>
          <RaceTree
            races={races}
            selectedRaceId={selectedRaceId}
            onSelectRace={handleSelectRace}
          />
        </div>
      </aside>

      {/* ── Main ────────────────────────────────────── */}
      <main className="app-main">
        {view === 'hits' && (
          <HitHistory predictions={predictions} />
        )}
        {view === 'dashboard' && (
          <div className="p-4">
            <TabView races={races} predictions={predictions} summary={summary} />
          </div>
        )}
        {view === 'race' && selectedRace && (
          <RaceDetail race={selectedRace} predictions={racePredictions} />
        )}
        {view === 'race' && !selectedRace && (
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <div className="text-6xl mb-6 opacity-20">🏇</div>
              <div className="neon-text text-lg tracking-[0.3em]">RACE EXPLORER</div>
              <div className="text-sm text-[var(--text-muted)] mt-3 tracking-widest">
                左のツリーからレースを選択してください
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  )
}
