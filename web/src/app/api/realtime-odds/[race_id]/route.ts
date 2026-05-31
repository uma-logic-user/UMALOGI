/**
 * GET /api/realtime-odds/[race_id]
 *
 * 指定レースの馬番ごと最新オッズを返す。
 * realtime_odds テーブルから GROUP BY horse_number で最新行を取得する
 * （SQLite の「非集計列は MAX 行の値を返す」拡張に依拠）。
 *
 * レスポンス例:
 * {
 *   "race_id": "202605310511",
 *   "updated_at": "2026-05-31 14:46:03",
 *   "odds": {
 *     "5": { "horse_name": "テスト馬A", "win_odds": 3.5, "place_odds_min": 1.4,
 *            "place_odds_max": 2.1, "popularity": 2 },
 *     ...
 *   }
 * }
 *
 * realtime_odds が空の場合は odds: {} / updated_at: null を返す（404 ではなく 200）。
 */

import { NextRequest, NextResponse } from 'next/server'
import { getDb } from '@/lib/db'

export const dynamic = 'force-dynamic'

interface OddsEntry {
  horse_name:      string
  win_odds:        number | null
  place_odds_min:  number | null
  place_odds_max:  number | null
  popularity:      number | null
}

interface RealtimeOddsResponse {
  race_id:    string
  updated_at: string | null
  odds:       Record<string, OddsEntry>
}

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ race_id: string }> },
) {
  const { race_id } = await params

  const db = getDb()

  const rows = db.prepare(`
    SELECT
      horse_number,
      horse_name,
      win_odds,
      place_odds_min,
      place_odds_max,
      popularity,
      MAX(recorded_at) AS updated_at
    FROM realtime_odds
    WHERE race_id = ?
    GROUP BY horse_number
    ORDER BY horse_number
  `).all(race_id) as Array<{
    horse_number:   number
    horse_name:     string
    win_odds:       number | null
    place_odds_min: number | null
    place_odds_max: number | null
    popularity:     number | null
    updated_at:     string
  }>

  const odds: Record<string, OddsEntry> = {}
  let latestAt: string | null = null

  for (const row of rows) {
    odds[String(row.horse_number)] = {
      horse_name:     row.horse_name ?? '',
      win_odds:       row.win_odds,
      place_odds_min: row.place_odds_min,
      place_odds_max: row.place_odds_max,
      popularity:     row.popularity,
    }
    if (!latestAt || row.updated_at > latestAt) {
      latestAt = row.updated_at
    }
  }

  const body: RealtimeOddsResponse = {
    race_id,
    updated_at: latestAt,
    odds,
  }

  return NextResponse.json(body)
}
