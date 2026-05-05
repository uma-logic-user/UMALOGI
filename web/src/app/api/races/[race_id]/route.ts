import { NextRequest, NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { validateResponse } from '@/lib/validateResponse'
import { BET_ORDER, sanitize, rowToObj, sortedCombinations, identifyBetForm } from '@/lib/dbHelpers'

export const dynamic = 'force-dynamic'

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
      SELECT ph.horse_name, ph.horse_id, ph.predicted_rank, ph.model_score, ph.ev_score,
             COALESCE(rr.horse_number, en.horse_number) AS horse_number
      FROM prediction_horses ph
      LEFT JOIN race_results rr ON rr.horse_name = ph.horse_name AND rr.race_id = ?
      LEFT JOIN entries     en ON en.horse_name = ph.horse_name AND en.race_id = ?
      WHERE ph.prediction_id = ?
      ORDER BY ph.predicted_rank NULLS LAST, ph.id
    `)

    // 馬番→馬名マップ（このレース用）
    const horseNumToName: Record<string, string> = {}
    for (const row of results) {
      const num = row.horse_number
      const name = row.horse_name
      if (num != null && name != null) {
        horseNumToName[String(num)] = name as string
      }
    }

    const predictions = predRows.map(rowToObj).map((pd) => {
      const comboJson = pd.combination_json as string | null
      const betType   = (pd.bet_type as string) ?? ''
      const [betForm, nTickets] = identifyBetForm(comboJson, betType)
      return {
        ...pd,
        combination_json:  sortedCombinations(comboJson, betType),
        bet_form:          betForm,
        n_tickets:         nTickets,
        horses:            (getHorses.all(race_id, race_id, pd.prediction_id) as Record<string, unknown>[]).map(rowToObj),
        horse_num_to_name: horseNumToName,
      }
    })

    d.results     = results
    d.payouts     = payouts
    d.predictions = predictions

    return NextResponse.json(validateResponse(d, '[/api/races/[race_id]]'))
  } catch (err) {
    console.error('[/api/races/[race_id]]', err)
    return NextResponse.json(null, { status: 500 })
  }
}
