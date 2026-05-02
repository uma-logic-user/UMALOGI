import { NextRequest, NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { validateResponse } from '@/lib/validateResponse'

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

// SQLite の VARIABLE_NUMBER 制限 (999) に収まるよう分割する
function chunkArray<T>(arr: T[], size: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
}

export async function GET(req: NextRequest) {
  try {
    const db = getDb()
    const { searchParams } = req.nextUrl
    const limit = Math.min(parseInt(searchParams.get('limit') ?? '1000', 10), 5000)

    const preds = db.prepare(`
      SELECT
        p.id            AS prediction_id,
        p.race_id,
        r.race_name,
        r.date,
        r.venue,
        r.race_number,
        r.surface,
        r.distance,
        r.weather,
        r.condition,
        p.model_type,
        p.bet_type,
        p.confidence,
        p.expected_value,
        p.recommended_bet,
        p.combination_json,
        p.notes,
        p.created_at,
        pr.is_hit,
        pr.payout,
        pr.profit,
        pr.roi
      FROM predictions p
      JOIN  races r             ON p.race_id = r.race_id
      LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
      ORDER BY p.created_at DESC
      LIMIT ?
    `).all(limit) as Record<string, unknown>[]

    // N+1 → バルク IN 句: prediction_horses を全件まとめて取得してメモリで結合
    const predIds = preds.map(p => p.prediction_id as number)
    const allHorses: Record<string, unknown>[] = []
    for (const chunk of chunkArray(predIds, 500)) {
      if (chunk.length === 0) continue
      const rows = (db.prepare(`
        SELECT ph.prediction_id,
               ph.horse_name, ph.horse_id, ph.predicted_rank, ph.model_score, ph.ev_score,
               COALESCE(e.horse_number, 99) AS horse_number
        FROM prediction_horses ph
        LEFT JOIN predictions p2 ON ph.prediction_id = p2.id
        LEFT JOIN entries e ON e.horse_id = ph.horse_id AND e.race_id = p2.race_id
        WHERE ph.prediction_id IN (${chunk.map(() => '?').join(',')})
        ORDER BY ph.prediction_id, COALESCE(e.horse_number, 99), ph.id
      `).all(...chunk) as Record<string, unknown>[]).map(rowToObj)
      allHorses.push(...rows)
    }

    // prediction_id でグループ化
    const horsesByPred = new Map<number, Record<string, unknown>[]>()
    for (const h of allHorses) {
      const pid = h.prediction_id as number
      if (!horsesByPred.has(pid)) horsesByPred.set(pid, [])
      horsesByPred.get(pid)!.push(h)
    }

    const output = preds.map(rowToObj).map((pd) => {
      const dateStr = pd.date as string | null
      if (!pd.race_name) {
        pd.race_name = pd.race_number != null ? `第${pd.race_number}レース` : 'レース'
      }
      return {
        ...pd,
        combination_json: sortedCombinations(pd.combination_json),
        year:   dateStr ? dateStr.slice(0, 4) : null,
        horses: horsesByPred.get(pd.prediction_id as number) ?? [],
      }
    })

    return NextResponse.json(validateResponse(output, '[/api/predictions]'))
  } catch (err) {
    console.error('[/api/predictions]', err)
    return NextResponse.json([], { status: 500 })
  }
}
