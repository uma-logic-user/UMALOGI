import { NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { validateResponse } from '@/lib/validateResponse'

export const dynamic = 'force-dynamic'

function rowToObj(row: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(row).map(([k, v]) => [
      k,
      typeof v === 'string' ? v.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/g, '').trim() : v,
    ]),
  )
}

export async function GET() {
  try {
    const db = getDb()

    const annual = (db.prepare(`
      SELECT model_type, year, bet_type, venue,
             total_bets, hits, hit_rate,
             total_invested, total_payout, roi, updated_at
      FROM model_performance
      WHERE month = 0
      ORDER BY year DESC, model_type, bet_type
    `).all() as Record<string, unknown>[]).map(rowToObj)

    const overall = db.prepare(`
      SELECT
        COUNT(pr.id)           AS total_bets,
        SUM(pr.is_hit)         AS total_hits,
        SUM(p.recommended_bet) AS total_invested,
        SUM(pr.payout)         AS total_payout
      FROM predictions p
      LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
    `).get() as Record<string, unknown> | undefined

    const byBetType = (db.prepare(`
      SELECT
        p.bet_type,
        COUNT(pr.id)                                           AS total_bets,
        COALESCE(SUM(pr.is_hit), 0)                           AS hits,
        ROUND(CAST(SUM(pr.is_hit) AS REAL)
              / NULLIF(COUNT(pr.id), 0) * 100, 2)             AS hit_rate,
        COALESCE(SUM(p.recommended_bet), 0)                   AS total_invested,
        COALESCE(SUM(pr.payout), 0)                           AS total_payout,
        ROUND(COALESCE(SUM(pr.payout), 0)
              / NULLIF(SUM(p.recommended_bet), 0) * 100, 2)   AS roi
      FROM predictions p
      LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
      WHERE pr.id IS NOT NULL
      GROUP BY p.bet_type
      ORDER BY total_bets DESC
    `).all() as Record<string, unknown>[]).map(rowToObj)

    const byYear = (db.prepare(`
      SELECT
        substr(r.date, 1, 4)                                   AS year,
        p.model_type,
        COUNT(pr.id)                                           AS total_bets,
        COALESCE(SUM(pr.is_hit), 0)                           AS hits,
        ROUND(CAST(SUM(pr.is_hit) AS REAL)
              / NULLIF(COUNT(pr.id), 0) * 100, 2)             AS hit_rate,
        COALESCE(SUM(p.recommended_bet), 0)                   AS total_invested,
        COALESCE(SUM(pr.payout), 0)                           AS total_payout,
        ROUND(COALESCE(SUM(pr.payout), 0)
              / NULLIF(SUM(p.recommended_bet), 0) * 100, 2)   AS roi
      FROM predictions p
      JOIN races r ON p.race_id = r.race_id
      LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
      WHERE pr.id IS NOT NULL
      GROUP BY year, p.model_type
      ORDER BY year DESC, p.model_type
    `).all() as Record<string, unknown>[]).map(rowToObj)

    const totalRaces = db.prepare('SELECT COUNT(*) AS cnt FROM races').get() as
      | { cnt: number }
      | undefined

    return NextResponse.json(validateResponse({
      total_races_in_db:  totalRaces?.cnt ?? 0,
      annual_performance: annual,
      by_bet_type:        byBetType,
      by_year:            byYear,
      overall:            overall ? rowToObj(overall) : {},
    }, '[/api/summary]'))
  } catch (err) {
    console.error('[/api/summary]', err)
    return NextResponse.json(
      { total_races_in_db: 0, annual_performance: [], by_bet_type: [], by_year: [], overall: {} },
      { status: 500 },
    )
  }
}
