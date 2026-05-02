import { NextRequest, NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { validateResponse } from '@/lib/validateResponse'

const BET_ORDER: Record<string, number> = {
  '単勝': 1, '複勝': 2, '枠連': 3, '馬連': 4,
  'ワイド': 5, '馬単': 6, '三連複': 7, '三連単': 8,
}

function sanitize(v: unknown): unknown {
  return typeof v === 'string' ? v.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/g, '').trim() : v
}

function rowToObj(row: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(row).map(([k, v]) => [k, sanitize(v)]))
}

export async function GET(req: NextRequest) {
  try {
    const db = getDb()
    const { searchParams } = req.nextUrl
    const limit  = Math.min(parseInt(searchParams.get('limit')  ?? '500', 10), 2000)
    const offset = parseInt(searchParams.get('offset') ?? '0', 10)

    const races = db.prepare(`
      SELECT race_id, race_name, date, venue, race_number,
             distance, surface, track_direction, weather, condition
      FROM races
      ORDER BY date DESC, race_id
      LIMIT ? OFFSET ?
    `).all(limit, offset) as Record<string, unknown>[]

    if (races.length === 0) {
      return NextResponse.json([])
    }

    // 全レースIDのプレースホルダを構築して一括取得（N+1 回避）
    const raceIds = races.map(r => r.race_id as string)
    const ph = raceIds.map(() => '?').join(',')

    const allResults = db.prepare(`
      SELECT rr.race_id, rr.rank, rr.gate_number, rr.horse_number,
             rr.horse_name, rr.horse_id, rr.sex_age, rr.weight_carried,
             rr.jockey, rr.trainer, rr.finish_time, rr.margin,
             rr.win_odds, rr.popularity, rr.horse_weight, rr.horse_weight_diff,
             h.sire, h.dam, h.dam_sire
      FROM race_results rr
      LEFT JOIN horses h ON rr.horse_id = h.horse_id
      WHERE rr.race_id IN (${ph})
      ORDER BY rr.race_id, rr.rank NULLS LAST, rr.id
    `).all(...raceIds) as Record<string, unknown>[]

    const allPayouts = db.prepare(`
      SELECT race_id, bet_type, combination, payout, popularity
      FROM race_payouts
      WHERE race_id IN (${ph})
    `).all(...raceIds) as Record<string, unknown>[]

    // race_id でグループ化
    const resultsMap = new Map<string, Record<string, unknown>[]>()
    for (const row of allResults) {
      const rid = row.race_id as string
      if (!resultsMap.has(rid)) resultsMap.set(rid, [])
      resultsMap.get(rid)!.push(rowToObj(row))
    }

    const payoutsMap = new Map<string, Record<string, unknown>[]>()
    for (const row of allPayouts) {
      const rid = row.race_id as string
      if (!payoutsMap.has(rid)) payoutsMap.set(rid, [])
      payoutsMap.get(rid)!.push(rowToObj(row))
    }

    const output = races.map((race) => {
      const d = rowToObj(race)
      const dateStr = d.date as string | null
      d.year = dateStr ? dateStr.slice(0, 4) : null
      // race_name が空の場合は「第○レース」で代替
      if (!d.race_name) {
        d.race_name = d.race_number != null ? `第${d.race_number}レース` : 'レース'
      }
      d.results = resultsMap.get(d.race_id as string) ?? []
      d.payouts = (payoutsMap.get(d.race_id as string) ?? []).sort((a, b) => {
        const ao = BET_ORDER[a.bet_type as string] ?? 99
        const bo = BET_ORDER[b.bet_type as string] ?? 99
        if (ao !== bo) return ao - bo
        return ((a.popularity as number) ?? 999) - ((b.popularity as number) ?? 999)
      })
      return d
    })

    return NextResponse.json(validateResponse(output, '[/api/races]'))
  } catch (err) {
    console.error('[/api/races]', err)
    return NextResponse.json([], { status: 500 })
  }
}
