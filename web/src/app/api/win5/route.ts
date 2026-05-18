import { NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { validateResponse } from '@/lib/validateResponse'

export const dynamic = 'force-dynamic'

function sanitize(v: unknown): unknown {
  return typeof v === 'string' ? v.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/g, '').trim() : v
}

export async function GET() {
  try {
    const db = getDb()

    // win5_results テーブルが存在するか確認（マイグレーション前の互換性）
    const hasWin5Results = db.prepare(
      "SELECT COUNT(*) AS cnt FROM sqlite_master WHERE type='table' AND name='win5_results'"
    ).get() as { cnt: number }

    const rows = db.prepare(`
      SELECT
        p.race_id, p.combination_json, p.notes,
        COALESCE(pr.payout, 0) AS payout,
        COALESCE(pr.is_hit, 0) AS is_hit,
        r.date,
        ${hasWin5Results.cnt > 0 ? 'wr.winning_numbers, COALESCE(wr.payout, 0) AS actual_payout' : 'NULL AS winning_numbers, 0 AS actual_payout'}
      FROM predictions p
      JOIN races r ON r.race_id = p.race_id
      LEFT JOIN prediction_results pr ON pr.prediction_id = p.id
      ${hasWin5Results.cnt > 0 ? 'LEFT JOIN win5_results wr ON wr.race_date = r.date' : ''}
      WHERE p.model_type = 'WIN5' AND p.bet_type = 'WIN5'
      ORDER BY r.date DESC
      LIMIT 30
    `).all() as {
      race_id: string
      combination_json: string | null
      notes: string | null
      payout: number
      is_hit: number
      date: string
      winning_numbers: string | null
      actual_payout: number
    }[]

    const getBasicInfo = db.prepare(
      'SELECT race_id, race_name, venue, distance, surface FROM races WHERE race_id = ?',
    )

    const output = rows.map((row) => {
      let combo: Record<string, unknown> = {}
      try { combo = JSON.parse(row.combination_json ?? '{}') } catch { /* empty */ }

      const raceIds: string[] = (combo.race_ids as string[]) ?? [row.race_id]

      const raceInfo: Record<string, unknown> = {}
      for (const rid of raceIds) {
        const r = getBasicInfo.get(rid) as Record<string, unknown> | undefined
        if (r) {
          raceInfo[rid] = {
            race_id:   sanitize(r.race_id),
            race_name: sanitize(r.race_name),
            venue:     sanitize(r.venue),
            distance:  r.distance,
            surface:   sanitize(r.surface),
          }
        }
      }

      // 実結果の的中馬番を parse
      let actualNumbers: number[] = []
      try {
        actualNumbers = row.winning_numbers ? JSON.parse(row.winning_numbers) : []
      } catch { /* empty */ }

      // AI予想の選択馬番（各レースの最上位選択）
      const selections = combo.selections ?? {} as Record<string, number[]>

      // 予実比較: 各レースでAI選択馬番が実際の1着馬番を含むか
      const perRaceHit: Record<string, boolean> = {}
      if (actualNumbers.length === 5) {
        raceIds.forEach((rid, idx) => {
          const sel: number[] = (selections as Record<string, number[]>)[rid] ?? []
          perRaceHit[rid] = sel.includes(actualNumbers[idx])
        })
      }

      // 的中判定: actualNumbers が揃っている場合は予実比較で上書き
      const isHit = actualNumbers.length === 5
        ? Object.values(perRaceHit).every(Boolean) ? 1 : 0
        : row.is_hit

      return {
        date:               sanitize(row.date),
        race_ids:           raceIds,
        races:              raceIds.map((rid) => raceInfo[rid] ?? { race_id: rid }),
        selections,
        horse_ranks:        combo.horse_ranks ?? {},
        total_combinations: combo.total_combinations ?? 1,
        is_hit:             isHit,
        payout:             row.actual_payout > 0 ? row.actual_payout : row.payout,
        actual_numbers:     actualNumbers,
        per_race_hit:       perRaceHit,
        notes:              sanitize(row.notes ?? ''),
      }
    })

    return NextResponse.json(validateResponse(output, '[/api/win5]'))
  } catch (err) {
    console.error('[/api/win5]', err)
    return NextResponse.json([], { status: 500 })
  }
}
