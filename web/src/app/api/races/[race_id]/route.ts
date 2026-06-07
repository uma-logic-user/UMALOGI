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

    // 結果（race_results）
    const rrRows = (db.prepare(`
      SELECT rr.rank, rr.gate_number, rr.horse_number, rr.horse_name,
             rr.horse_id, rr.sex_age, rr.weight_carried, rr.jockey, rr.trainer,
             rr.finish_time, rr.margin, rr.win_odds, rr.popularity,
             rr.horse_weight, rr.horse_weight_diff, h.sire, h.dam, h.dam_sire
      FROM race_results rr
      LEFT JOIN horses h ON rr.horse_id = h.horse_id
      WHERE rr.race_id = ?
      ORDER BY rr.rank NULLS LAST, rr.id
    `).all(race_id) as Record<string, unknown>[]).map(rowToObj)

    // entries（出馬表）: race_results に馬名がない行を entries で補完する
    // 本日の未確定レース等で race_results が空または馬名欠損の場合に使用
    // 文字化けパターン: ?X 系 / 半角カタカナ / U+FFFD
    const _garbledRe = /\?[A-Za-z\[\]＝]|[｡-ﾟ]|�/
    const _isValidHorseName = (n: string) => n.trim() !== '' && !_garbledRe.test(n)
    const validRrCount = rrRows.filter(r => r.horse_name && _isValidHorseName(r.horse_name as string)).length
    let results: Record<string, unknown>[]
    if (validRrCount === 0) {
      // entries テーブルから出馬表を構築（race_results 互換フォーマット）
      results = (db.prepare(`
        SELECT en.horse_number, en.gate_number, en.horse_name, en.horse_id,
               en.sex_age, en.weight_carried, en.jockey, en.trainer,
               en.horse_weight, en.horse_weight_diff,
               NULL AS rank, NULL AS finish_time, NULL AS margin,
               NULL AS win_odds, NULL AS popularity,
               NULL AS sire, NULL AS dam, NULL AS dam_sire
        FROM entries en
        WHERE en.race_id = ?
        ORDER BY en.horse_number
      `).all(race_id) as Record<string, unknown>[]).map(rowToObj)
    } else {
      // race_results に馬名のない行は entries で horse_name/jockey を補完
      const entMap: Record<number, Record<string, unknown>> = {}
      for (const en of (db.prepare(`SELECT horse_number, horse_name, jockey, trainer, sex_age, weight_carried, horse_weight, horse_weight_diff FROM entries WHERE race_id = ?`).all(race_id) as Record<string, unknown>[])) {
        entMap[en.horse_number as number] = en
      }
      results = rrRows.map(r => {
        if (!r.horse_name || (r.horse_name as string).trim() === '') {
          const en = entMap[r.horse_number as number]
          if (en) {
            return { ...r, horse_name: en.horse_name, jockey: en.jockey ?? r.jockey, trainer: en.trainer ?? r.trainer }
          }
        }
        return r
      })
    }

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

    // マルチ券種オッズ（直近スナップショット）
    const multiOddsRows = (db.prepare(`
      SELECT bet_type, combination, odds, odds_max, popularity, recorded_at
      FROM multi_odds
      WHERE race_id = ?
        AND recorded_at = (
          SELECT MAX(recorded_at) FROM multi_odds WHERE race_id = ?
        )
      ORDER BY bet_type, popularity NULLS LAST
    `).all(race_id, race_id) as Record<string, unknown>[]).map(rowToObj)

    const multiOdds: Record<string, unknown[]> = {}
    for (const row of multiOddsRows) {
      const bt = row.bet_type as string
      if (!multiOdds[bt]) multiOdds[bt] = []
      multiOdds[bt].push({
        combination: row.combination,
        odds:        row.odds,
        odds_max:    row.odds_max,
        popularity:  row.popularity,
        recorded_at: row.recorded_at,
      })
    }

    d.results     = results
    d.payouts     = payouts
    d.predictions = predictions
    d.multi_odds  = Object.keys(multiOdds).length > 0 ? multiOdds : undefined

    return NextResponse.json(validateResponse(d, '[/api/races/[race_id]]'))
  } catch (err) {
    console.error('[/api/races/[race_id]]', err)
    return NextResponse.json(null, { status: 500 })
  }
}
