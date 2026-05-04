import { NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { validateResponse } from '@/lib/validateResponse'
import { BET_ORDER, sanitize } from '@/lib/dbHelpers'

export const dynamic = 'force-dynamic'

interface BaseRow {
  year: string
  month: string
  day: string
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

// ── 集計ヘルパー ─────────────────────────────────────────────────────────────

function buildDailyByModel(rows: BaseRow[]): Record<string, unknown[]> {
  // (day|model|bettype) → bet entry
  type BetEntry = {
    bet_type: string; invested: number; payout: number
    total_bets: number; hits: number
    races: unknown[]
  }
  type DayEntry = {
    date: string; model_type: string
    invested: number; payout: number; total_bets: number; hits: number
    by_bet_type: Map<string, BetEntry>
  }
  const dayMap = new Map<string, DayEntry>()

  for (const r of rows) {
    const dayKey = `${r.day}|${r.model_type}`
    if (!dayMap.has(dayKey)) {
      dayMap.set(dayKey, {
        date: r.day, model_type: r.model_type,
        invested: 0, payout: 0, total_bets: 0, hits: 0,
        by_bet_type: new Map(),
      })
    }
    const d = dayMap.get(dayKey)!
    const inv = r.invested ?? 0
    const pay = r.payout  ?? 0
    d.invested   += inv
    d.payout     += pay
    d.total_bets += 1
    d.hits       += r.is_hit ?? 0

    const betKey = r.bet_type
    if (!d.by_bet_type.has(betKey)) {
      d.by_bet_type.set(betKey, {
        bet_type: r.bet_type, invested: 0, payout: 0,
        total_bets: 0, hits: 0, races: [],
      })
    }
    const b = d.by_bet_type.get(betKey)!
    b.invested   += inv
    b.payout     += pay
    b.total_bets += 1
    b.hits       += r.is_hit ?? 0
    b.races.push({
      race_id:     sanitize(r.race_id),
      race_name:   sanitize(r.race_name),
      venue:       sanitize(r.venue),
      race_number: r.race_number,
      invested:    Math.round(inv * 10) / 10,
      payout:      Math.round(pay * 10) / 10,
      is_hit:      r.is_hit ?? 0,
    })
  }

  const result: Record<string, unknown[]> = {}
  for (const [, d] of [...dayMap.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    const inv  = d.invested
    const pay  = d.payout
    const prof = pay - inv
    const roi  = inv > 0 ? Math.round(pay / inv * 10000) / 100 : 0

    const by_bet_type = [...d.by_bet_type.values()]
      .sort((a, b) => (BET_ORDER[a.bet_type] ?? 99) - (BET_ORDER[b.bet_type] ?? 99))
      .map(b => ({
        bet_type:   b.bet_type,
        invested:   Math.round(b.invested   * 10) / 10,
        payout:     Math.round(b.payout     * 10) / 10,
        profit:     Math.round((b.payout - b.invested) * 10) / 10,
        roi:        b.invested > 0 ? Math.round(b.payout / b.invested * 10000) / 100 : 0,
        total_bets: b.total_bets,
        hits:       b.hits,
        races:      b.races,
      }))

    if (!result[d.model_type]) result[d.model_type] = []
    result[d.model_type].push({
      date: d.date,
      invested:   Math.round(inv  * 10) / 10,
      payout:     Math.round(pay  * 10) / 10,
      profit:     Math.round(prof * 10) / 10,
      roi,
      total_bets: d.total_bets,
      hits:       d.hits,
      by_bet_type,
    })
  }

  for (const arr of Object.values(result) as { profit: number; cumulative_profit?: number }[][]) {
    let cum = 0
    for (const p of arr) { cum += p.profit; p.cumulative_profit = Math.round(cum * 10) / 10 }
  }

  return result
}

function buildPeriodByModel(
  rows: BaseRow[],
  keyFn: (r: BaseRow) => string,
  labelFn: (key: string) => string,
): Record<string, unknown[]> {
  type PEntry = {
    period: string; model_type: string
    invested: number; payout: number; total_bets: number; hits: number
    by_bet_type: Map<string, { bet_type: string; invested: number; payout: number; total_bets: number; hits: number }>
  }
  const periodMap = new Map<string, PEntry>()

  for (const r of rows) {
    const period = keyFn(r)
    const pKey = `${period}|${r.model_type}`
    if (!periodMap.has(pKey)) {
      periodMap.set(pKey, {
        period, model_type: r.model_type,
        invested: 0, payout: 0, total_bets: 0, hits: 0,
        by_bet_type: new Map(),
      })
    }
    const p   = periodMap.get(pKey)!
    const inv = r.invested ?? 0
    const pay = r.payout   ?? 0
    p.invested   += inv
    p.payout     += pay
    p.total_bets += 1
    p.hits       += r.is_hit ?? 0

    if (!p.by_bet_type.has(r.bet_type)) {
      p.by_bet_type.set(r.bet_type, { bet_type: r.bet_type, invested: 0, payout: 0, total_bets: 0, hits: 0 })
    }
    const b = p.by_bet_type.get(r.bet_type)!
    b.invested   += inv
    b.payout     += pay
    b.total_bets += 1
    b.hits       += r.is_hit ?? 0
  }

  const result: Record<string, unknown[]> = {}
  for (const [, p] of [...periodMap.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    const inv  = p.invested
    const pay  = p.payout
    const prof = pay - inv
    const roi  = inv > 0 ? Math.round(pay / inv * 10000) / 100 : 0

    const by_bet_type = [...p.by_bet_type.values()]
      .sort((a, b) => (BET_ORDER[a.bet_type] ?? 99) - (BET_ORDER[b.bet_type] ?? 99))
      .map(b => ({
        bet_type:   b.bet_type,
        invested:   Math.round(b.invested   * 10) / 10,
        payout:     Math.round(b.payout     * 10) / 10,
        profit:     Math.round((b.payout - b.invested) * 10) / 10,
        roi:        b.invested > 0 ? Math.round(b.payout / b.invested * 10000) / 100 : 0,
        total_bets: b.total_bets,
        hits:       b.hits,
      }))

    if (!result[p.model_type]) result[p.model_type] = []
    result[p.model_type].push({
      period:     p.period,
      label:      labelFn(p.period),
      invested:   Math.round(inv  * 10) / 10,
      payout:     Math.round(pay  * 10) / 10,
      profit:     Math.round(prof * 10) / 10,
      roi,
      total_bets: p.total_bets,
      hits:       p.hits,
      by_bet_type,
    })
  }

  for (const arr of Object.values(result) as { profit: number; cumulative_profit?: number }[][]) {
    let cum = 0
    for (const p of arr) { cum += p.profit; p.cumulative_profit = Math.round(cum * 10) / 10 }
  }

  return result
}

export async function GET() {
  try {
    const db = getDb()

    // 単一スキャンで全集計に必要な行レベルデータを取得する
    const baseRows = (db.prepare(`
      SELECT
        substr(r.date, 1, 4)  AS year,
        substr(r.date, 1, 7)  AS month,
        substr(r.date, 1, 10) AS day,
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
      ORDER BY day, p.model_type, p.bet_type, p.race_id
    `).all() as Record<string, unknown>[]).map(r =>
      Object.fromEntries(Object.entries(r).map(([k, v]) => [k, sanitize(v)])),
    ) as unknown as BaseRow[]

    const monthLabel = (p: string) => { const [y, m] = p.split('-'); return `${y}年${parseInt(m)}月` }
    const yearLabel  = (p: string) => `${p}年`

    const dailyByModel   = buildDailyByModel(baseRows)
    const monthlyByModel = buildPeriodByModel(baseRows, r => r.month, monthLabel)
    const yearlyByModel  = buildPeriodByModel(baseRows, r => r.year,  yearLabel)

    const allModels = new Set([
      ...Object.keys(dailyByModel),
      ...Object.keys(monthlyByModel),
      ...Object.keys(yearlyByModel),
    ])

    const output: Record<string, unknown> = {}
    for (const model of [...allModels].sort()) {
      output[model] = {
        daily:   dailyByModel[model]   ?? [],
        monthly: monthlyByModel[model] ?? [],
        yearly:  yearlyByModel[model]  ?? [],
      }
    }

    return NextResponse.json(validateResponse(output, '[/api/financial]'))
  } catch (err) {
    console.error('[/api/financial]', err)
    return NextResponse.json({}, { status: 500 })
  }
}
