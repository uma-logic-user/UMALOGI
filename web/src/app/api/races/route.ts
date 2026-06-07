import { NextRequest, NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { validateResponse } from '@/lib/validateResponse'
import { BET_ORDER, sanitize, rowToObj } from '@/lib/dbHelpers'

export const dynamic = 'force-dynamic'

export async function GET(req: NextRequest) {
  try {
    const db = getDb()
    const { searchParams } = req.nextUrl
    const limit  = Math.min(parseInt(searchParams.get('limit')  ?? '2000', 10), 5000)
    const offset = parseInt(searchParams.get('offset') ?? '0', 10)
    const dateFilter = searchParams.get('date') ?? null

    const races = dateFilter
      ? (db.prepare(`
          SELECT race_id, race_name, date, venue, race_number,
                 distance, surface, track_direction, weather, condition
          FROM races
          WHERE date = ?
          ORDER BY race_id
          LIMIT ? OFFSET ?
        `).all(dateFilter, limit, offset) as Record<string, unknown>[])
      : (db.prepare(`
          SELECT race_id, race_name, date, venue, race_number,
                 distance, surface, track_direction, weather, condition
          FROM races
          ORDER BY date DESC, race_id
          LIMIT ? OFFSET ?
        `).all(limit, offset) as Record<string, unknown>[])

    if (races.length === 0) {
      return NextResponse.json([])
    }

    // 全レースIDのプレースホルダを構築して一括取得（N+1 回避）
    const raceIds = races.map(r => r.race_id as string)
    const ph = raceIds.map(() => '?').join(',')

    const allResults = db.prepare(`
      SELECT rr.race_id, rr.rank, rr.gate_number, rr.horse_number,
             rr.horse_name, rr.horse_id, rr.sex_age, rr.weight_carried,
             rr.jockey, rr.trainer, rr.finish_time, rr.margin,
             rr.win_odds, rr.popularity, rr.horse_weight, rr.horse_weight_diff,
             h.sire, h.dam, h.dam_sire
      FROM race_results rr
      LEFT JOIN horses h ON rr.horse_id = h.horse_id
      WHERE rr.race_id IN (${ph})
      ORDER BY rr.race_id, rr.rank NULLS LAST, rr.id
    `).all(...raceIds) as Record<string, unknown>[]

    // entries テーブルから出馬表データを一括取得（race_results 補完用）
    const allEntries = db.prepare(`
      SELECT en.race_id, en.horse_number, en.gate_number, en.horse_name,
             en.horse_id, en.sex_age, en.weight_carried, en.jockey, en.trainer,
             en.horse_weight, en.horse_weight_diff,
             NULL AS rank, NULL AS finish_time, NULL AS margin,
             NULL AS win_odds, NULL AS popularity,
             NULL AS sire, NULL AS dam, NULL AS dam_sire
      FROM entries en
      WHERE en.race_id IN (${ph})
      ORDER BY en.race_id, en.horse_number
    `).all(...raceIds) as Record<string, unknown>[]

    const entriesMap = new Map<string, Record<string, unknown>[]>()
    for (const row of allEntries) {
      const rid = row.race_id as string
      if (!entriesMap.has(rid)) entriesMap.set(rid, [])
      entriesMap.get(rid)!.push(rowToObj(row))
    }

    const allPayouts = db.prepare(`
      SELECT race_id, bet_type, combination, payout, popularity
      FROM race_payouts
      WHERE race_id IN (${ph})
    `).all(...raceIds) as Record<string, unknown>[]

    // race_id でグループ化
    const resultsMap = new Map<string, Record<string, unknown>[]>()
    for (const row of allResults) {
      const rid = row.race_id as string
      if (!resultsMap.has(rid)) resultsMap.set(rid, [])
      resultsMap.get(rid)!.push(rowToObj(row))
    }

    const payoutsMap = new Map<string, Record<string, unknown>[]>()
    for (const row of allPayouts) {
      const rid = row.race_id as string
      if (!payoutsMap.has(rid)) payoutsMap.set(rid, [])
      payoutsMap.get(rid)!.push(rowToObj(row))
    }

    const output = races.map((race) => {
      const d = rowToObj(race)
      const dateStr = d.date as string | null
      d.year = dateStr ? dateStr.slice(0, 4) : null
      // race_name が空の場合は「第○レース」で代替
      if (!d.race_name) {
        d.race_name = d.race_number != null ? `第${d.race_number}レース` : 'レース'
      }
      // race_results に馬名がなければ entries で代替
      const rrList = resultsMap.get(d.race_id as string) ?? []
      const _garbledReR = /\?[A-Za-z\[\]＝]|[｡-ﾟ]|�/
      const _isValidR = (n: string) => n.trim() !== '' && !_garbledReR.test(n)
      const validRr = rrList.filter(r => r.horse_name && _isValidR(r.horse_name as string))
      if (validRr.length === 0) {
        d.results = entriesMap.get(d.race_id as string) ?? []
      } else {
        const entByNum: Record<number, Record<string, unknown>> = {}
        for (const en of (entriesMap.get(d.race_id as string) ?? [])) {
          entByNum[en.horse_number as number] = en
        }
        d.results = rrList.map(r => {
          if (!r.horse_name || (r.horse_name as string).trim() === '') {
            const en = entByNum[r.horse_number as number]
            if (en) return { ...r, horse_name: en.horse_name, jockey: en.jockey ?? r.jockey, trainer: en.trainer ?? r.trainer }
          }
          return r
        })
      }
      d.payouts = (payoutsMap.get(d.race_id as string) ?? []).sort((a, b) => {
        const ao = BET_ORDER[a.bet_type as string] ?? 99
        const bo = BET_ORDER[b.bet_type as string] ?? 99
        if (ao !== bo) return ao - bo
        return ((a.popularity as number) ?? 999) - ((b.popularity as number) ?? 999)
      })
      return d
    })

    return NextResponse.json(validateResponse(output, '[/api/races]'))
  } catch (err) {
    console.error('[/api/races]', err)
    return NextResponse.json([], { status: 500 })
  }
}
