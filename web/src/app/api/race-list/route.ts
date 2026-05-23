import { NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { rowToObj } from '@/lib/dbHelpers'

export const dynamic = 'force-dynamic'

/** レースツリー用の軽量エンドポイント（results/payouts なし、全期間対象）*/
export async function GET() {
  try {
    const db = getDb()
    const races = db.prepare(`
      SELECT race_id, race_name, date, venue, race_number, surface, distance
      FROM races
      ORDER BY date DESC, race_id
      LIMIT 20000
    `).all() as Record<string, unknown>[]

    const output = races.map((race) => {
      const d = rowToObj(race)
      const dateStr = d.date as string | null
      d.year = dateStr ? dateStr.slice(0, 4) : null
      if (!d.race_name) {
        d.race_name = d.race_number != null ? `第${d.race_number}レース` : 'レース'
      }
      return d
    })

    return NextResponse.json(output)
  } catch (err) {
    console.error('[/api/race-list]', err)
    return NextResponse.json([], { status: 500 })
  }
}
