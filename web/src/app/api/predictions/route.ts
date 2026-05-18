import { NextRequest, NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { validateResponse } from '@/lib/validateResponse'
import { sanitize, rowToObj, sortedCombinations, chunkArray, identifyBetForm } from '@/lib/dbHelpers'

export const dynamic = 'force-dynamic'

export async function GET(req: NextRequest) {
  try {
    const db = getDb()
    const { searchParams } = req.nextUrl
    const limit = Math.min(parseInt(searchParams.get('limit') ?? '50000', 10), 50000)
    const dateFilter = searchParams.get('date') ?? null

    const preds = dateFilter
      ? (db.prepare(`
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
          WHERE r.date = ?
          ORDER BY p.created_at DESC
          LIMIT ?
        `).all(dateFilter, limit) as Record<string, unknown>[])
      : (db.prepare(`
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
        `).all(limit) as Record<string, unknown>[])

    // N+1 → バルク IN 句: prediction_horses を全件まとめて取得してメモリで結合
    const predIds = preds.map(p => p.prediction_id as number)
    const allHorses: Record<string, unknown>[] = []
    for (const chunk of chunkArray(predIds, 500)) {
      if (chunk.length === 0) continue
      const rows = (db.prepare(`
        SELECT ph.prediction_id,
               ph.horse_name, ph.horse_id, ph.predicted_rank, ph.model_score, ph.ev_score,
               COALESCE(rr.horse_number, en.horse_number) AS horse_number
        FROM prediction_horses ph
        LEFT JOIN predictions p2 ON ph.prediction_id = p2.id
        LEFT JOIN race_results rr ON rr.horse_name = ph.horse_name AND rr.race_id = p2.race_id
        LEFT JOIN entries     en ON en.horse_name = ph.horse_name AND en.race_id = p2.race_id
        WHERE ph.prediction_id IN (${chunk.map(() => '?').join(',')})
        ORDER BY ph.prediction_id, COALESCE(rr.horse_number, en.horse_number, 99), ph.id
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

    // 馬番→馬名マップをレースIDごとに一括取得
    const raceIds = [...new Set(preds.map(p => p.race_id as string))]
    const horseNumToNameByRace = new Map<string, Record<string, string>>()
    for (const chunk of chunkArray(raceIds, 500)) {
      if (chunk.length === 0) continue
      const rows = db.prepare(`
        SELECT race_id, horse_number, horse_name
        FROM race_results
        WHERE horse_number IS NOT NULL
          AND race_id IN (${chunk.map(() => '?').join(',')})
      `).all(...chunk) as { race_id: string; horse_number: number; horse_name: string }[]
      for (const row of rows) {
        if (!horseNumToNameByRace.has(row.race_id)) {
          horseNumToNameByRace.set(row.race_id, {})
        }
        horseNumToNameByRace.get(row.race_id)![String(row.horse_number)] = row.horse_name
      }
    }

    const output = preds.map(rowToObj).map((pd) => {
      const dateStr   = pd.date as string | null
      const modelType = (pd.model_type as string) ?? ''
      if (!pd.race_name) {
        pd.race_name = pd.race_number != null ? `第${pd.race_number}レース` : 'レース'
      }
      const comboJson = pd.combination_json as string | null
      const betType   = (pd.bet_type as string) ?? ''
      const [betForm, nTickets] = identifyBetForm(comboJson, betType)
      return {
        ...pd,
        combination_json:  sortedCombinations(comboJson, betType),
        bet_form:          betForm,
        n_tickets:         nTickets,
        invested:          nTickets * 100,
        year:              dateStr ? dateStr.slice(0, 4) : null,
        horses:            horsesByPred.get(pd.prediction_id as number) ?? [],
        horse_num_to_name: horseNumToNameByRace.get(pd.race_id as string) ?? {},
        is_provisional:    modelType.includes('(暫定)'),
      }
    })

    return NextResponse.json(validateResponse(output, '[/api/predictions]'))
  } catch (err) {
    console.error('[/api/predictions]', err)
    return NextResponse.json([], { status: 500 })
  }
}
