'use client'

import { useMemo, useState, useEffect, useRef } from 'react'
import type { Race } from '@/types/race'

// ── 発走時刻ユーティリティ ─────────────────────────────────────
// R1=10:00 JST, 以降30分間隔
function estimatePostTime(dateStr: string, raceNumber: number): Date {
  const normalized = dateStr.replace(/\//g, '-')
  const [y, m, d]  = normalized.split('-').map(Number)
  const totalMin   = 10 * 60 + (raceNumber - 1) * 30  // JST分
  const h = Math.floor(totalMin / 60)
  const min = totalMin % 60
  // JST(UTC+9) → UTC へ変換
  return new Date(Date.UTC(y, m - 1, d, h - 9, min, 0))
}

function formatPostTimeHHMM(dt: Date): string {
  const jst = new Date(dt.getTime() + 9 * 60 * 60 * 1000)
  return `${String(jst.getUTCHours()).padStart(2, '0')}:${String(jst.getUTCMinutes()).padStart(2, '0')}`
}

function formatCountdown(secRemaining: number): string {
  if (secRemaining <= 0) return ''
  const h = Math.floor(secRemaining / 3600)
  const m = Math.floor((secRemaining % 3600) / 60)
  if (h > 0) return `${h}時間${m}分後`
  if (m === 0) return '間もなく'
  return `${m}分後`
}

// JST で今日の日付文字列 "YYYY-MM-DD" を返す
function todayJST(now: Date): string {
  const jst = new Date(now.getTime() + 9 * 60 * 60 * 1000)
  return jst.toISOString().slice(0, 10)
}

interface Props {
  races:          Race[]
  selectedRaceId: string | null
  onSelectRace:   (raceId: string) => void
}

// ── ツリーデータ構造を構築 ────────────────────────────────
type TreeNode = {
  [year: string]: {
    [date: string]: {
      [venue: string]: Race[]
    }
  }
}

function buildTree(races: Race[]): TreeNode {
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
  // "2026-05-17" → "05/17"  or  "2024/06/01" → "06/01"
  const parts = dateStr.split(/[-/]/)
  return parts.length >= 3 ? `${parts[1]}/${parts[2]}` : dateStr
}

function surfaceIcon(surface: string): string {
  return surface === '芝' ? '🌿' : surface === 'ダート' ? '🟤' : '•'
}

export default function RaceTree({ races, selectedRaceId, onSelectRace }: Props) {
  const tree = useMemo(() => buildTree(races), [races])
  const years = Object.keys(tree).sort((a, b) => b.localeCompare(a))  // 降順

  // races=[] で初期化されるため useState の lazy initializer では年が取れない
  // → useState は空 Set で起動し、useEffect でデータロード後に最新年・日付を自動展開する
  const [openYears, setOpenYears] = useState<Set<string>>(new Set())
  const [openDates, setOpenDates] = useState<Set<string>>(new Set())
  const didAutoExpand = useRef(false)

  // カウントダウン用: 60秒ごとに更新
  const [now, setNow] = useState<Date>(() => new Date())
  useEffect(() => {
    const timer = setInterval(() => setNow(new Date()), 60_000)
    return () => clearInterval(timer)
  }, [])

  // データ初回ロード時に最新年と最新日付を自動展開
  useEffect(() => {
    if (didAutoExpand.current || years.length === 0) return
    didAutoExpand.current = true
    const latestYear = years[0]
    const datesInYear = Object.keys(tree[latestYear] ?? {}).sort((a, b) => b.localeCompare(a))
    const latestDate = datesInYear[0]
    if (!latestDate) return
    const dkey = `${latestYear}-${latestDate}`
    setOpenYears(new Set([latestYear]))
    setOpenDates(new Set([dkey]))
  }, [years, tree])

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

  // 選択レースが含まれるノードを自動展開（useMemo → useEffect に変更: 副作用は useEffect で）
  useEffect(() => {
    if (!selectedRaceId) return
    const race = races.find((r: Race) => r.race_id === selectedRaceId)
    if (!race) return
    const year = race.year ?? race.date.slice(0, 4)
    const dkey = `${year}-${race.date}`
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
                  {dateOpen && venues.map(venue => {
                    const todayStr = todayJST(now)
                    return (
                      <div key={venue}>
                        <div className="tree-venue-label">{venue}</div>
                        {tree[year][date][venue].map(race => {
                          const isActive   = race.race_id === selectedRaceId
                          const postTime   = estimatePostTime(race.date, race.race_number)
                          const timeLabel  = formatPostTimeHHMM(postTime)
                          const isToday    = race.date.replace(/\//g, '-') === todayStr
                          const secLeft    = isToday
                            ? Math.floor((postTime.getTime() - now.getTime()) / 1000)
                            : -1
                          const countdown  = secLeft > 0 ? formatCountdown(secLeft) : ''
                          const isPast     = isToday && secLeft <= 0

                          return (
                            <button
                              key={race.race_id}
                              className={`tree-race-btn ${isActive ? 'active' : ''}`}
                              onClick={() => onSelectRace(race.race_id)}
                              title={`${race.race_number}R ${race.race_name || `第${race.race_number}R`} ${race.surface}${race.distance}m  発走 ${timeLabel} JST`}
                            >
                              <span className="text-[var(--text-muted)] mr-1.5 font-mono text-[11px]">
                                {String(race.race_number).padStart(2, ' ')}R
                              </span>
                              {surfaceIcon(race.surface)}
                              <span className="ml-1 truncate flex-1">{race.race_name || `第${race.race_number}R`}</span>
                              <span
                                className="ml-1.5 font-mono text-[12px] shrink-0"
                                style={{ color: isPast ? 'var(--text-muted)' : 'var(--text-primary)' }}
                              >
                                {timeLabel}
                              </span>
                              {countdown && (
                                <span
                                  className="ml-1 font-mono text-[12px] font-semibold shrink-0"
                                  style={{ color: secLeft < 1800 ? '#C44040' : '#5B8A5B' }}
                                >
                                  {countdown}
                                </span>
                              )}
                            </button>
                          )
                        })}
                      </div>
                    )
                  })}
                </div>
              )
            })}
          </div>
        )
      })}
    </div>
  )
}
