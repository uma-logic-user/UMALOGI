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

function sortedCombinations(json: unknown): string {
  if (!json || typeof json !== 'string') return '[]'
  try {
    const raw: number[][] = JSON.parse(json)
    const sorted = raw
      .map(c => [...c].sort((a, b) => a - b))
      .sort((a, b) => {
        for (let i = 0; i < Math.min(a.length, b.length); i++) {
          if (a[i] !== b[i]) return a[i] - b[i]
        }
        return a.length - b.length
      })
    return JSON.stringify(sorted)
  } catch {
    return String(json)
  }
}

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ race_id: string }> },
) {
  try {
    const { race_id } = await params
    const db = getDb()

    const race = db.prepare(`
      SELECT race_id, race_name, date, venue, race_number,
             distance, surface, track_direction, weather, condition
      FROM races WHERE race_id = ?
    `).get(race_id) as Record<string, unknown> | undefined

    if (!race) {
      return NextResponse.json(null, { status: 404 })
    }

    const d = rowToObj(race)
    const dateStr = d.date as string | null
    d.year = dateStr ? dateStr.slice(0, 4) : null
    if (!d.race_name) {
      d.race_name = d.race_number != null ? `第${d.race_number}レース` : 'レース'
    }

    // 結果
    const results = (db.prepare(`
      SELECT rr.rank, rr.gate_number, rr.horse_number, rr.horse_name,
             rr.horse_id, rr.sex_age, rr.weight_carried, rr.jockey, rr.trainer,
             rr.finish_time, rr.margin, rr.win_odds, rr.popularity,
             rr.horse_weight, rr.horse_weight_diff, h.sire, h.dam, h.dam_sire
      FROM race_results rr
      LEFT JOIN horses h ON rr.horse_id = h.horse_id
      WHERE rr.race_id = ?
      ORDER BY rr.rank NULLS LAST, rr.id
    `).all(race_id) as Record<string, unknown>[]).map(rowToObj)

    // 払戻
    const payouts = (db.prepare(`
      SELECT bet_type, combination, payout, popularity
      FROM race_payouts WHERE race_id = ?
      ORDER BY popularity NULLS LAST
    `).all(race_id) as Record<string, unknown>[])
      .map(rowToObj)
      .sort((a, b) => {
        const ao = BET_ORDER[a.bet_type as string] ?? 99
        const bo = BET_ORDER[b.bet_type as string] ?? 99
        if (ao !== bo) return ao - bo
        return ((a.popularity as number) ?? 999) - ((b.popularity as number) ?? 999)
      })

    // 予想
    const predRows = db.prepare(`
      SELECT p.id AS prediction_id, p.model_type, p.bet_type,
             p.confidence, p.expected_value, p.recommended_bet,
             p.combination_json, p.notes, p.created_at,
             pr.is_hit, pr.payout, pr.profit, pr.roi
      FROM predictions p
      LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
      WHERE p.race_id = ?
      ORDER BY p.created_at, p.id
    `).all(race_id) as Record<string, unknown>[]

    const getHorses = db.prepare(`
      SELECT horse_name, horse_id, predicted_rank, model_score, ev_score
      FROM prediction_horses WHERE prediction_id = ?
      ORDER BY predicted_rank NULLS LAST, id
    `)

    const predictions = predRows.map(rowToObj).map((pd) => ({
      ...pd,
      combination_json: sortedCombinations(pd.combination_json),
      horses: (getHorses.all(pd.prediction_id) as Record<string, unknown>[]).map(rowToObj),
    }))

    d.results     = results
    d.payouts     = payouts
    d.predictions = predictions

    return NextResponse.json(validateResponse(d, '[/api/races/[race_id]]'))
  } catch (err) {
    console.error('[/api/races/[race_id]]', err)
    return NextResponse.json(null, { status: 500 })
  }
}
