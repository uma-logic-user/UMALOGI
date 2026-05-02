import { NextRequest, NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { validateResponse } from '@/lib/validateResponse'

function sanitize(v: unknown): unknown {
  return typeof v === 'string' ? v.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/g, '').trim() : v
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

export async function GET(req: NextRequest) {
  try {
    const db = getDb()
    const limit = Math.min(
      parseInt(req.nextUrl.searchParams.get('limit') ?? '200', 10),
      1000,
    )

    const rows = db.prepare(`
      SELECT
        r.race_id,
        r.race_name,
        r.date,
        r.venue,
        r.surface,
        r.distance,
        p.model_type,
        p.bet_type,
        p.combination_json,
        COALESCE(pr.payout, 0)  AS payout,
        COALESCE(pr.is_hit, 0)  AS is_hit,
        p.recommended_bet,
        p.notes
      FROM predictions p
      JOIN races r ON r.race_id = p.race_id
      LEFT JOIN prediction_results pr ON pr.prediction_id = p.id
      WHERE p.model_type LIKE 'Oracle%'
        AND p.bet_type IN ('三連複', '三連単')
      ORDER BY r.date DESC, payout DESC
      LIMIT ?
    `).all(limit) as Record<string, unknown>[]

    const output = rows.map((row) => {
      const payout = (row.payout as number) ?? 0
      const rank =
        payout >= 100_000 ? 'S' :
        payout >= 30_000  ? 'A' :
        payout >= 10_000  ? 'B' : 'C'
      return {
        race_id:          sanitize(row.race_id),
        race_name:        sanitize(row.race_name),
        date:             sanitize(row.date),
        venue:            sanitize(row.venue),
        surface:          sanitize(row.surface),
        distance:         row.distance,
        model_type:       sanitize(row.model_type),
        bet_type:         sanitize(row.bet_type),
        combination_json: sortedCombinations(row.combination_json),
        payout,
        is_hit:           row.is_hit ?? 0,
        rank:             row.is_hit ? rank : null,
        recommended_bet:  row.recommended_bet,
        notes:            sanitize(row.notes ?? ''),
      }
    })

    return NextResponse.json(validateResponse(output, '[/api/gachi]'))
  } catch (err) {
    console.error('[/api/gachi]', err)
    return NextResponse.json([], { status: 500 })
  }
}
