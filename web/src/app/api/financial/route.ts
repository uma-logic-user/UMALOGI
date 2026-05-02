import { NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { validateResponse } from '@/lib/validateResponse'

export const dynamic = 'force-dynamic'

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

interface PeriodRow {
  period: string
  model_type: string
  bet_type: string
  invested: number
  payout: number
  total_bets: number
  hits: number
}

interface RaceRow {
  date: string
  model_type: string
  bet_type: string
  race_id: string
  race_name: string
  venue: string
  race_number: number
  invested: number
  payout: number
  is_hit: number | null
}

function buildPeriodAggregates(
  rows: PeriodRow[],
  labelFn: (period: string) => string,
): Record<string, unknown[]> {
  const periodMap = new Map<string, {
    period: string; label: string; model_type: string
    invested: number; payout: number; total_bets: number; hits: number
    by_bet_type: unknown[]
  }>()

  for (const r of rows) {
    const key = `${r.period}|${r.model_type}`
    if (!periodMap.has(key)) {
      periodMap.set(key, {
        period: r.period, label: labelFn(r.period), model_type: r.model_type,
        invested: 0, payout: 0, total_bets: 0, hits: 0, by_bet_type: [],
      })
    }
    const entry = periodMap.get(key)!
    const inv = r.invested ?? 0
    const pay = r.payout  ?? 0
    entry.invested   += inv
    entry.payout     += pay
    entry.total_bets += r.total_bets
    entry.hits       += r.hits
    entry.by_bet_type.push({
      bet_type: r.bet_type,
      invested: Math.round(inv * 10) / 10,
      payout:   Math.round(pay * 10) / 10,
      profit:   Math.round((pay - inv) * 10) / 10,
      roi:      inv > 0 ? Math.round(pay / inv * 10000) / 100 : 0,
      total_bets: r.total_bets,
      hits:       r.hits,
    })
  }

  for (const v of periodMap.values()) {
    (v.by_bet_type as { bet_type: string }[]).sort(
      (a, b) => (BET_ORDER[a.bet_type] ?? 99) - (BET_ORDER[b.bet_type] ?? 99),
    )
  }

  const result: Record<string, unknown[]> = {}
  for (const [, v] of [...periodMap.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    const inv  = v.invested
    const pay  = v.payout
    const prof = pay - inv
    const roi  = inv > 0 ? Math.round(pay / inv * 10000) / 100 : 0
    if (!result[v.model_type]) result[v.model_type] = []
    result[v.model_type].push({
      period:     v.period,
      label:      v.label,
      invested:   Math.round(inv  * 10) / 10,
      payout:     Math.round(pay  * 10) / 10,
      profit:     Math.round(prof * 10) / 10,
      roi,
      total_bets: v.total_bets,
      hits:       v.hits,
      by_bet_type: v.by_bet_type,
    })
  }

  for (const arr of Object.values(result) as { profit: number; cumulative_profit?: number }[][]) {
    let cum = 0
    for (const p of arr) {
      cum += p.profit
      p.cumulative_profit = Math.round(cum * 10) / 10
    }
  }

  return result
}

export async function GET() {
  try {
    const db = getDb()

    // 日次×券種
    const betRows = (db.prepare(`
      SELECT
        substr(r.date, 1, 10)               AS period,
        p.model_type,
        p.bet_type,
        COALESCE(SUM(p.recommended_bet), 0) AS invested,
        COALESCE(SUM(pr.payout), 0)         AS payout,
        COUNT(pr.id)                        AS total_bets,
        COALESCE(SUM(pr.is_hit), 0)         AS hits
      FROM predictions p
      JOIN  races r              ON p.race_id = r.race_id
      LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
      WHERE pr.id IS NOT NULL
      GROUP BY substr(r.date, 1, 10), p.model_type, p.bet_type
      ORDER BY period, p.model_type, p.bet_type
    `).all() as Record<string, unknown>[]).map(rowToObj) as unknown as PeriodRow[]

    // レース粒度
    const raceRows = (db.prepare(`
      SELECT
        substr(r.date, 1, 10) AS date,
        p.model_type,
        p.bet_type,
        p.race_id,
        r.race_name,
        r.venue,
        r.race_number,
        COALESCE(p.recommended_bet, 0) AS invested,
        COALESCE(pr.payout, 0)         AS payout,
        pr.is_hit
      FROM predictions p
      JOIN  races r              ON p.race_id = r.race_id
      LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
      WHERE pr.id IS NOT NULL
      ORDER BY date, p.model_type, p.bet_type, p.race_id
    `).all() as Record<string, unknown>[]).map(rowToObj) as unknown as RaceRow[]

    // レース粒度マップ: (date|model|bet) → races[]
    const raceMap = new Map<string, unknown[]>()
    for (const r of raceRows) {
      const key = `${r.date}|${r.model_type}|${r.bet_type}`
      if (!raceMap.has(key)) raceMap.set(key, [])
      raceMap.get(key)!.push({
        race_id:     r.race_id,
        race_name:   r.race_name,
        venue:       r.venue,
        race_number: r.race_number,
        invested:    Math.round((r.invested ?? 0) * 10) / 10,
        payout:      Math.round((r.payout   ?? 0) * 10) / 10,
        is_hit:      r.is_hit ?? 0,
      })
    }

    // 日次サマリー: (date|model) → day entry
    const dayMap = new Map<string, {
      date: string; model_type: string
      invested: number; payout: number; total_bets: number; hits: number
      by_bet_type: unknown[]
    }>()

    for (const r of betRows) {
      const key = `${r.period}|${r.model_type}`
      if (!dayMap.has(key)) {
        dayMap.set(key, {
          date: r.period, model_type: r.model_type,
          invested: 0, payout: 0, total_bets: 0, hits: 0,
          by_bet_type: [],
        })
      }
      const d   = dayMap.get(key)!
      const inv = r.invested ?? 0
      const pay = r.payout  ?? 0
      d.invested   += inv
      d.payout     += pay
      d.total_bets += r.total_bets
      d.hits       += r.hits
      d.by_bet_type.push({
        bet_type: r.bet_type,
        invested: Math.round(inv * 10) / 10,
        payout:   Math.round(pay * 10) / 10,
        profit:   Math.round((pay - inv) * 10) / 10,
        roi:      inv > 0 ? Math.round(pay / inv * 10000) / 100 : 0,
        total_bets: r.total_bets,
        hits:       r.hits,
        races:      raceMap.get(`${r.period}|${r.model_type}|${r.bet_type}`) ?? [],
      })
    }

    for (const v of dayMap.values()) {
      (v.by_bet_type as { bet_type: string }[]).sort(
        (a, b) => (BET_ORDER[a.bet_type] ?? 99) - (BET_ORDER[b.bet_type] ?? 99),
      )
    }

    const dailyByModel: Record<string, unknown[]> = {}
    for (const [, v] of [...dayMap.entries()].sort(([a], [b]) => a.localeCompare(b))) {
      const inv  = v.invested
      const pay  = v.payout
      const prof = pay - inv
      const roi  = inv > 0 ? Math.round(pay / inv * 10000) / 100 : 0
      if (!dailyByModel[v.model_type]) dailyByModel[v.model_type] = []
      dailyByModel[v.model_type].push({
        date: v.date,
        invested:   Math.round(inv  * 10) / 10,
        payout:     Math.round(pay  * 10) / 10,
        profit:     Math.round(prof * 10) / 10,
        roi,
        total_bets: v.total_bets,
        hits:       v.hits,
        by_bet_type: v.by_bet_type,
      })
    }
    for (const arr of Object.values(dailyByModel) as { profit: number; cumulative_profit?: number }[][]) {
      let cum = 0
      for (const p of arr) { cum += p.profit; p.cumulative_profit = Math.round(cum * 10) / 10 }
    }

    // 月別・年別
    const monthRows = (db.prepare(`
      SELECT substr(r.date, 1, 7) AS period, p.model_type, p.bet_type,
             COALESCE(SUM(p.recommended_bet), 0) AS invested,
             COALESCE(SUM(pr.payout), 0)         AS payout,
             COUNT(pr.id)                        AS total_bets,
             COALESCE(SUM(pr.is_hit), 0)         AS hits
      FROM predictions p
      JOIN races r ON p.race_id = r.race_id
      LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
      WHERE pr.id IS NOT NULL
      GROUP BY substr(r.date, 1, 7), p.model_type, p.bet_type
      ORDER BY period, p.model_type, p.bet_type
    `).all() as Record<string, unknown>[]).map(rowToObj) as unknown as PeriodRow[]

    const yearRows = (db.prepare(`
      SELECT substr(r.date, 1, 4) AS period, p.model_type, p.bet_type,
             COALESCE(SUM(p.recommended_bet), 0) AS invested,
             COALESCE(SUM(pr.payout), 0)         AS payout,
             COUNT(pr.id)                        AS total_bets,
             COALESCE(SUM(pr.is_hit), 0)         AS hits
      FROM predictions p
      JOIN races r ON p.race_id = r.race_id
      LEFT JOIN prediction_results pr ON p.id = pr.prediction_id
      WHERE pr.id IS NOT NULL
      GROUP BY substr(r.date, 1, 4), p.model_type, p.bet_type
      ORDER BY period, p.model_type, p.bet_type
    `).all() as Record<string, unknown>[]).map(rowToObj) as unknown as PeriodRow[]

    const monthLabel = (p: string) => { const [y, m] = p.split('-'); return `${y}年${parseInt(m)}月` }
    const yearLabel  = (p: string) => `${p}年`

    const monthly = buildPeriodAggregates(monthRows, monthLabel)
    const yearly  = buildPeriodAggregates(yearRows,  yearLabel)

    const allModels = new Set([
      ...Object.keys(dailyByModel),
      ...Object.keys(monthly),
      ...Object.keys(yearly),
    ])

    const output: Record<string, unknown> = {}
    for (const model of [...allModels].sort()) {
      output[model] = {
        daily:   dailyByModel[model] ?? [],
        monthly: monthly[model]      ?? [],
        yearly:  yearly[model]       ?? [],
      }
    }

    return NextResponse.json(validateResponse(output, '[/api/financial]'))
  } catch (err) {
    console.error('[/api/financial]', err)
    return NextResponse.json({}, { status: 500 })
  }
}
