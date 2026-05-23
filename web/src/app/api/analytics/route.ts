import { NextResponse } from 'next/server'
import { getDb } from '@/lib/db'

export const dynamic = 'force-dynamic'

interface AnalyticsRow {
  date:         string
  year:         string
  month:        string
  day:          string
  venue:        string
  surface:      string
  model_type:   string
  bet_type:     string
  total_bets:   number
  hits:         number
  total_payout: number
  total_profit: number
}

function roi(payout: number, profit: number): number {
  const invested = payout - profit
  if (invested <= 0) return 0
  return Math.round((payout / invested) * 1000) / 10
}

export async function GET() {
  try {
    const db = getDb()

    // v_analytics ビューから全データ取得
    const rows = db.prepare(`
      SELECT date, year, month, day, venue, surface,
             model_type, bet_type,
             total_bets, hits, total_payout, total_profit
      FROM v_analytics
      ORDER BY date DESC, model_type, bet_type
    `).all() as AnalyticsRow[]

    // ── 階層集計: Year > Month > Day > Model > BetType ─────────────────
    type BetNode = {
      bet_type: string; total_bets: number; hits: number
      total_payout: number; total_profit: number; roi: number
    }
    type ModelNode = {
      model_type: string; total_bets: number; hits: number
      total_payout: number; total_profit: number; roi: number
      bet_types: BetNode[]
    }
    type DayNode = {
      date: string; day: string; total_bets: number; hits: number
      total_payout: number; total_profit: number; roi: number
      models: ModelNode[]
    }
    type MonthNode = {
      month: string; total_bets: number; hits: number
      total_payout: number; total_profit: number; roi: number
      days: DayNode[]
    }
    type YearNode = {
      year: string; total_bets: number; hits: number
      total_payout: number; total_profit: number; roi: number
      months: MonthNode[]
    }

    const yearMap = new Map<string, Map<string, Map<string, Map<string, Map<string, BetNode>>>>>()

    for (const r of rows) {
      if (!yearMap.has(r.year)) yearMap.set(r.year, new Map())
      const mMap = yearMap.get(r.year)!
      if (!mMap.has(r.month)) mMap.set(r.month, new Map())
      const dMap = mMap.get(r.month)!
      if (!dMap.has(r.date)) dMap.set(r.date, new Map())
      const moMap = dMap.get(r.date)!
      if (!moMap.has(r.model_type)) moMap.set(r.model_type, new Map())
      const btMap = moMap.get(r.model_type)!

      btMap.set(r.bet_type, {
        bet_type:     r.bet_type,
        total_bets:   r.total_bets,
        hits:         r.hits,
        total_payout: r.total_payout,
        total_profit: r.total_profit,
        roi:          roi(r.total_payout, r.total_profit),
      })
    }

    const years: YearNode[] = []

    for (const [year, mMap] of [...yearMap.entries()].sort((a, b) => b[0].localeCompare(a[0]))) {
      const months: MonthNode[] = []
      let yBets = 0, yHits = 0, yPayout = 0, yProfit = 0

      for (const [month, dMap] of [...mMap.entries()].sort((a, b) => b[0].localeCompare(a[0]))) {
        const days: DayNode[] = []
        let mBets = 0, mHits = 0, mPayout = 0, mProfit = 0

        for (const [date, moMap] of [...dMap.entries()].sort((a, b) => b[0].localeCompare(a[0]))) {
          const models: ModelNode[] = []
          let dBets = 0, dHits = 0, dPayout = 0, dProfit = 0

          for (const [model, btMap] of [...moMap.entries()].sort()) {
            const betNodes = [...btMap.values()].sort((a, b) => a.bet_type.localeCompare(b.bet_type))
            const moBets   = betNodes.reduce((s, n) => s + n.total_bets, 0)
            const moHits   = betNodes.reduce((s, n) => s + n.hits, 0)
            const moPayout = betNodes.reduce((s, n) => s + n.total_payout, 0)
            const moProfit = betNodes.reduce((s, n) => s + n.total_profit, 0)
            models.push({
              model_type: model, total_bets: moBets, hits: moHits,
              total_payout: moPayout, total_profit: moProfit,
              roi: roi(moPayout, moProfit),
              bet_types: betNodes,
            })
            dBets += moBets; dHits += moHits; dPayout += moPayout; dProfit += moProfit
          }

          days.push({
            date, day: date.slice(8), total_bets: dBets, hits: dHits,
            total_payout: dPayout, total_profit: dProfit,
            roi: roi(dPayout, dProfit), models,
          })
          mBets += dBets; mHits += dHits; mPayout += dPayout; mProfit += dProfit
        }

        months.push({
          month, total_bets: mBets, hits: mHits,
          total_payout: mPayout, total_profit: mProfit,
          roi: roi(mPayout, mProfit), days,
        })
        yBets += mBets; yHits += mHits; yPayout += mPayout; yProfit += mProfit
      }

      years.push({
        year, total_bets: yBets, hits: yHits,
        total_payout: yPayout, total_profit: yProfit,
        roi: roi(yPayout, yProfit), months,
      })
    }

    // 全体サマリー
    const overall = {
      total_bets:   years.reduce((s, y) => s + y.total_bets, 0),
      hits:         years.reduce((s, y) => s + y.hits, 0),
      total_payout: years.reduce((s, y) => s + y.total_payout, 0),
      total_profit: years.reduce((s, y) => s + y.total_profit, 0),
    }

    return NextResponse.json({
      overall: { ...overall, roi: roi(overall.total_payout, overall.total_profit) },
      years,
    })
  } catch (err) {
    console.error('[/api/analytics]', err)
    return NextResponse.json({ overall: {}, years: [] }, { status: 500 })
  }
}
