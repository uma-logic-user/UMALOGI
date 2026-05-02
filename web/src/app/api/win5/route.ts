import { NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { validateResponse } from '@/lib/validateResponse'

export const dynamic = 'force-dynamic'

function sanitize(v: unknown): unknown {
  return typeof v === 'string' ? v.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/g, '').trim() : v
}

export async function GET() {
  try {
    const db = getDb()

    const rows = db.prepare(`
      SELECT
        p.race_id, p.combination_json, p.notes,
        COALESCE(pr.payout, 0) AS payout,
        COALESCE(pr.is_hit, 0) AS is_hit,
        r.date
      FROM predictions p
      JOIN races r ON r.race_id = p.race_id
      LEFT JOIN prediction_results pr ON pr.prediction_id = p.id
      WHERE p.model_type = 'WIN5' AND p.bet_type = 'WIN5'
      ORDER BY r.date DESC
      LIMIT 30
    `).all() as {
      race_id: string
      combination_json: string | null
      notes: string | null
      payout: number
      is_hit: number
      date: string
    }[]

    const getBasicInfo = db.prepare(
      'SELECT race_id, race_name, venue, distance, surface FROM races WHERE race_id = ?',
    )

    const output = rows.map((row) => {
      let combo: Record<string, unknown> = {}
      try { combo = JSON.parse(row.combination_json ?? '{}') } catch { /* empty */ }

      const raceIds: string[] = (combo.race_ids as string[]) ?? [row.race_id]

      const raceInfo: Record<string, unknown> = {}
      for (const rid of raceIds) {
        const r = getBasicInfo.get(rid) as Record<string, unknown> | undefined
        if (r) {
          raceInfo[rid] = {
            race_id:   sanitize(r.race_id),
            race_name: sanitize(r.race_name),
            venue:     sanitize(r.venue),
            distance:  r.distance,
            surface:   sanitize(r.surface),
          }
        }
      }

      return {
        date:               sanitize(row.date),
        race_ids:           raceIds,
        races:              raceIds.map((rid) => raceInfo[rid] ?? { race_id: rid }),
        selections:         combo.selections ?? {},
        horse_ranks:        combo.horse_ranks ?? {},
        total_combinations: combo.total_combinations ?? 1,
        is_hit:             row.is_hit,
        payout:             row.payout,
        notes:              sanitize(row.notes ?? ''),
      }
    })

    return NextResponse.json(validateResponse(output, '[/api/win5]'))
  } catch (err) {
    console.error('[/api/win5]', err)
    return NextResponse.json([], { status: 500 })
  }
}
