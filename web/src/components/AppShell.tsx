'use client'

import { useState, useMemo, useEffect, useCallback } from 'react'
import type { RaceEntry, Prediction } from '@/types/race'
import NavBar              from './NavBar'
import RaceTree            from './RaceTree'
import RaceDetail          from './RaceDetail'
import HitHistory          from './HitHistory'
import TabView             from './TabView'
import FinancialDashboard  from './FinancialDashboard'
import Win5Panel           from './Win5Panel'
import GachiHits           from './GachiHits'
import ConditionAnalysis   from './ConditionAnalysis'

type View = 'race' | 'hits' | 'dashboard' | 'financial' | 'win5' | 'gachi' | 'condition'

interface Summary {
  total_races_in_db: number
  overall: Record<string, unknown>
}

export default function AppShell() {
  const [view, setView]                     = useState<View>('dashboard')
  const [selectedRaceId, setSelectedRaceId] = useState<string | null>(null)

  // ── データ状態 ────────────────────────────────────────────────────
  const [races,         setRaces]         = useState<RaceEntry[]>([])
  const [predictions,   setPredictions]   = useState<Prediction[]>([])
  const [summary,       setSummary]       = useState<Summary>({ total_races_in_db: 0, overall: {} })
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [financialData, setFinancialData] = useState<Record<string, { daily: any[]; monthly: any[]; yearly: any[] }>>({})
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [win5Data,      setWin5Data]      = useState<any[]>([])
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [gachiHits,     setGachiHits]     = useState<any[]>([])
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [conditionData, setConditionData] = useState<any>(null)
  const [loading,       setLoading]       = useState(true)
  const [error,         setError]         = useState<string | null>(null)

  // ── 初回データ取得 ──────────────────────────────────────────────
  useEffect(() => {
    let cancelled = false

    async function fetchAll() {
      try {
        const [racesRes, predsRes, summaryRes, finRes, gachiRes, win5Res, condRes] = await Promise.all([
          fetch('/api/races'),
          fetch('/api/predictions'),
          fetch('/api/summary'),
          fetch('/api/financial'),
          fetch('/api/gachi'),
          fetch('/api/win5'),
          fetch('/api/condition'),
        ])

        if (cancelled) return

        const [racesData, predsData, summaryData, finData, gachiData, win5RawData, condData] =
          await Promise.all([
            racesRes.json(),
            predsRes.json(),
            summaryRes.json(),
            finRes.json(),
            gachiRes.json(),
            win5Res.json(),
            condRes.json(),
          ])

        if (cancelled) return

        setRaces(racesData)
        setPredictions(predsData)
        setSummary(summaryData)
        setFinancialData(finData)
        setGachiHits(gachiData)
        setWin5Data(win5RawData)
        setConditionData(condData)
      } catch (e) {
        if (!cancelled) setError(String(e))
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    fetchAll()
    return () => { cancelled = true }
  }, [])

  // ── 選択レース ───────────────────────────────────────────────────
  const selectedRace = useMemo(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    () => races.find(r => r.race_id === selectedRaceId) as any ?? null,
    [races, selectedRaceId],
  )

  const racePredictions = useMemo(
    () => selectedRaceId ? predictions.filter(p => p.race_id === selectedRaceId) : [],
    [predictions, selectedRaceId],
  )

  const handleSelectRace = useCallback((raceId: string) => {
    setSelectedRaceId(raceId)
    setView('race')
  }, [])

  const hits = predictions.filter(p => p.is_hit === 1)

  // ── ローディング / エラー ────────────────────────────────────────
  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-center">
          <div className="neon-text text-xl tracking-[0.3em] animate-pulse">UMALOGI</div>
          <div className="text-sm text-[var(--text-muted)] mt-3 tracking-widest">Loading...</div>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-center text-red-400">
          <div className="text-lg mb-2">データ取得エラー</div>
          <div className="text-sm opacity-70">{error}</div>
        </div>
      </div>
    )
  }

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
          <button
            className={`sidebar-special-btn ${view === 'financial' ? 'active' : ''}`}
            onClick={() => setView('financial')}
          >
            <span style={{ color: 'var(--neon-gold)', fontSize: '0.9rem' }}>¥</span>
            <span style={{ color: view === 'financial' ? 'var(--neon-gold)' : 'var(--text-primary)' }}>
              収支管理
            </span>
          </button>
          <button
            className={`sidebar-special-btn ${view === 'win5' ? 'active' : ''}`}
            onClick={() => setView('win5')}
          >
            <span style={{ color: '#a855f7', fontSize: '0.9rem' }}>W5</span>
            <span style={{ color: view === 'win5' ? '#a855f7' : 'var(--text-primary)' }}>
              WIN5 ガチ予想
            </span>
          </button>
          <button
            className={`sidebar-special-btn ${view === 'gachi' ? 'active' : ''}`}
            onClick={() => setView('gachi')}
          >
            <span style={{ color: '#ff4757', fontSize: '0.9rem', textShadow: '0 0 8px rgba(255,71,87,0.8)' }}>🎯</span>
            <span style={{ color: view === 'gachi' ? '#ff4757' : 'var(--text-primary)' }}>
              ガチ予想実績
            </span>
            {gachiHits.filter((h: { is_hit: number }) => h.is_hit === 1).length > 0 && (
              <span className="ml-auto text-xs font-bold px-1.5 py-0.5 rounded"
                    style={{ background: 'rgba(255,71,87,0.15)', color: '#ff4757', border: '1px solid rgba(255,71,87,0.3)' }}>
                {gachiHits.filter((h: { is_hit: number }) => h.is_hit === 1).length}
              </span>
            )}
          </button>
          <button
            className={`sidebar-special-btn ${view === 'condition' ? 'active' : ''}`}
            onClick={() => setView('condition')}
          >
            <span style={{ color: '#38bdf8', fontSize: '0.9rem' }}>📊</span>
            <span style={{ color: view === 'condition' ? '#38bdf8' : 'var(--text-primary)' }}>
              得意条件分析
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
        {view === 'financial' && (
          <div className="p-4">
            <FinancialDashboard
              data={financialData}
              onSelectRace={(raceId) => handleSelectRace(raceId)}
            />
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
        {view === 'win5' && (
          <Win5Panel data={win5Data} />
        )}
        {view === 'gachi' && (
          <GachiHits data={gachiHits} />
        )}
        {view === 'condition' && (
          <ConditionAnalysis data={conditionData} />
        )}
      </main>
    </div>
  )
}
