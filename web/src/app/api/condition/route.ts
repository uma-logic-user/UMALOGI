import { NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { validateResponse } from '@/lib/validateResponse'

export const dynamic = 'force-dynamic'

const DIST_CASE = `
  CASE
    WHEN r.distance IS NULL OR r.distance = 0 THEN '不明'
    WHEN r.distance < 1400  THEN '短距離(<1400m)'
    WHEN r.distance <= 1800 THEN 'マイル(1400-1800m)'
    WHEN r.distance <= 2200 THEN '中距離(1801-2200m)'
    ELSE '長距離(>2200m)'
  END
`

const BASE_FROM = `
  FROM predictions p
  JOIN races r ON p.race_id = r.race_id
  JOIN prediction_results pr ON p.id = pr.prediction_id
  WHERE r.date >= date('now', '-2 years')
    AND pr.id IS NOT NULL
`

const AGG_COLS = `
  COUNT(pr.id)  AS total_bets,
  COALESCE(SUM(pr.is_hit), 0) AS hits,
  ROUND(CAST(SUM(pr.is_hit) AS REAL)
        / NULLIF(COUNT(pr.id), 0) * 100, 1) AS hit_rate,
  COALESCE(SUM(COALESCE(pr.payout, 0) - COALESCE(pr.profit, 0)), 0) AS total_invested,
  COALESCE(SUM(pr.payout), 0)          AS total_payout,
  ROUND(COALESCE(SUM(pr.payout), 0)
        / NULLIF(SUM(COALESCE(pr.payout, 0) - COALESCE(pr.profit, 0)), 0) * 100, 1) AS roi
`

export async function GET() {
  try {
    const db = getDb()

    const byVenue = db.prepare(`
      SELECT r.venue AS venue, p.model_type, ${AGG_COLS}
      ${BASE_FROM}
      GROUP BY r.venue, p.model_type
      HAVING COUNT(pr.id) >= 3
      ORDER BY roi DESC
    `).all() as Record<string, unknown>[]

    const byDistance = db.prepare(`
      SELECT (${DIST_CASE}) AS distance_cat, p.model_type, ${AGG_COLS}
      ${BASE_FROM}
      GROUP BY (${DIST_CASE}), p.model_type
      HAVING COUNT(pr.id) >= 3
      ORDER BY roi DESC
    `).all() as Record<string, unknown>[]

    const bySurface = db.prepare(`
      SELECT COALESCE(r.surface, '不明') AS surface, p.model_type, ${AGG_COLS}
      ${BASE_FROM}
      GROUP BY COALESCE(r.surface, '不明'), p.model_type
      HAVING COUNT(pr.id) >= 3
      ORDER BY roi DESC
    `).all() as Record<string, unknown>[]

    const byCondition = db.prepare(`
      SELECT COALESCE(r.condition, '不明') AS track_condition, p.model_type, ${AGG_COLS}
      ${BASE_FROM}
      GROUP BY COALESCE(r.condition, '不明'), p.model_type
      HAVING COUNT(pr.id) >= 3
      ORDER BY roi DESC
    `).all() as Record<string, unknown>[]

    const combined = db.prepare(`
      SELECT
        r.venue,
        (${DIST_CASE})                        AS distance_cat,
        COALESCE(r.surface, '不明')          AS surface,
        COALESCE(r.condition, '不明')        AS track_condition,
        p.model_type,
        ${AGG_COLS}
      ${BASE_FROM}
      GROUP BY r.venue, (${DIST_CASE}), COALESCE(r.surface,'不明'), COALESCE(r.condition,'不明'), p.model_type
      HAVING COUNT(pr.id) >= 3
      ORDER BY roi DESC
      LIMIT 200
    `).all() as Record<string, unknown>[]

    const output = {
      generated_at: new Date().toISOString(),
      by_venue:     byVenue,
      by_distance:  byDistance,
      by_surface:   bySurface,
      by_condition: byCondition,
      combined,
    }

    return NextResponse.json(validateResponse(output, '[/api/condition]'))
  } catch (err) {
    console.error('[/api/condition]', err)
    return NextResponse.json(null, { status: 500 })
  }
}
