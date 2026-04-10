'use client'

import { useMemo, useState } from 'react'
import type { RaceEntry } from '@/types/race'

interface Props {
  races:          RaceEntry[]
  selectedRaceId: string | null
  onSelectRace:   (raceId: string) => void
}

// ── ツリーデータ構造を構築 ────────────────────────────────
type TreeNode = {
  [year: string]: {
    [date: string]: {
      [venue: string]: RaceEntry[]
    }
  }
}

function buildTree(races: RaceEntry[]): TreeNode {
  const tree: TreeNode = {}
  for (const race of races) {
    const year  = race.year ?? race.date.slice(0, 4)
    const date  = race.date   // "2024/06/01"
    const venue = race.venue
    if (!tree[year])          tree[year]         = {}
    if (!tree[year][date])    tree[year][date]    = {}
    if (!tree[year][date][venue]) tree[year][date][venue] = []
    tree[year][date][venue].push(race)
  }
  // レース番号順にソート
  for (const year of Object.keys(tree)) {
    for (const date of Object.keys(tree[year])) {
      for (const venue of Object.keys(tree[year][date])) {
        tree[year][date][venue].sort((a, b) => a.race_number - b.race_number)
      }
    }
  }
  return tree
}

// ── 表示用フォーマット ──────────────────────────────────────
function formatDate(dateStr: string): string {
  // "2024/06/01" → "06/01"
  const parts = dateStr.split('/')
  return parts.length >= 3 ? `${parts[1]}/${parts[2]}` : dateStr
}

function surfaceIcon(surface: string): string {
  return surface === '芝' ? '🌿' : surface === 'ダート' ? '🟤' : '•'
}

export default function RaceTree({ races, selectedRaceId, onSelectRace }: Props) {
  const tree = useMemo(() => buildTree(races), [races])
  const years = Object.keys(tree).sort((a, b) => b.localeCompare(a))  // 降順

  // デフォルトで最新年を展開
  const [openYears,  setOpenYears]  = useState<Set<string>>(() => new Set(years.slice(0, 1)))
  const [openDates,  setOpenDates]  = useState<Set<string>>(new Set())

  function toggleYear(year: string) {
    setOpenYears(prev => {
      const next = new Set(prev)
      next.has(year) ? next.delete(year) : next.add(year)
      return next
    })
  }

  function toggleDate(key: string) {
    setOpenDates(prev => {
      const next = new Set(prev)
      next.has(key) ? next.delete(key) : next.add(key)
      return next
    })
  }

  // 選択レースが含まれるノードを自動展開
  useMemo(() => {
    if (!selectedRaceId) return
    const race = races.find(r => r.race_id === selectedRaceId)
    if (!race) return
    const year  = race.year ?? race.date.slice(0, 4)
    const dkey  = `${year}-${race.date}`
    setOpenYears(prev => new Set([...prev, year]))
    setOpenDates(prev => new Set([...prev, dkey]))
  }, [selectedRaceId, races])

  if (races.length === 0) {
    return (
      <div className="px-4 py-6 text-xs text-[var(--text-muted)] text-center">
        レースデータなし
      </div>
    )
  }

  return (
    <div className="pb-6">
      {years.map(year => {
        const yearOpen = openYears.has(year)
        const dates    = Object.keys(tree[year]).sort((a, b) => b.localeCompare(a))
        const raceCount = dates.reduce(
          (sum, d) => sum + Object.values(tree[year][d]).reduce((s, rs) => s + rs.length, 0),
          0,
        )

        return (
          <div key={year}>
            {/* 年ボタン */}
            <button className="tree-year-btn" onClick={() => toggleYear(year)}>
              <span className="text-[var(--text-muted)] text-xs">
                {yearOpen ? '▼' : '▶'}
              </span>
              <span>{year}</span>
              <span className="ml-auto text-[10px] text-[var(--text-muted)]">
                {raceCount}R
              </span>
            </button>

            {/* 日付リスト */}
            {yearOpen && dates.map(date => {
              const dkey      = `${year}-${date}`
              const dateOpen  = openDates.has(dkey)
              const venues    = Object.keys(tree[year][date]).sort()
              const dayCount  = venues.reduce((s, v) => s + tree[year][date][v].length, 0)

              return (
                <div key={date}>
                  {/* 日付ボタン */}
                  <button className="tree-date-btn" onClick={() => toggleDate(dkey)}>
                    <span className="text-[var(--text-muted)] text-[10px]">
                      {dateOpen ? '▼' : '▶'}
                    </span>
                    <span>{formatDate(date)}</span>
                    <span className="ml-auto text-[10px] text-[var(--text-muted)]">
                      {dayCount}R
                    </span>
                  </button>

                  {/* 会場リスト */}
                  {dateOpen && venues.map(venue => (
                    <div key={venue}>
                      <div className="tree-venue-label">{venue}</div>
                      {tree[year][date][venue].map(race => {
                        const isActive = race.race_id === selectedRaceId
                        return (
                          <button
                            key={race.race_id}
                            className={`tree-race-btn ${isActive ? 'active' : ''}`}
                            onClick={() => onSelectRace(race.race_id)}
                            title={`${race.race_number}R ${race.race_name} ${race.surface}${race.distance}m`}
                          >
                            <span className="text-[var(--text-muted)] mr-1.5 font-mono text-[10px]">
                              {String(race.race_number).padStart(2, ' ')}R
                            </span>
                            {surfaceIcon(race.surface)}
                            <span className="ml-1">{race.race_name}</span>
                          </button>
                        )
                      })}
                    </div>
                  ))}
                </div>
              )
            })}
          </div>
        )
      })}
    </div>
  )
}
