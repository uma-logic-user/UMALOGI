/**
 * GET /api/shap/[prediction_id]
 *
 * 指定 prediction の prediction_horses.shap_json を返す。
 * shap_json が NULL の行は shap: null を返す（SHAP 未計算の予測はフロントで非表示）。
 *
 * レスポンス例:
 * {
 *   "prediction_id": 123,
 *   "horses": [
 *     {
 *       "horse_name": "テスト馬A",
 *       "predicted_rank": 1,
 *       "shap": [
 *         { "feature": "jockey_win_rate_90d", "label": "騎手直近90日勝率", "value": 0.45 },
 *         { "feature": "tc_4f",               "label": "ウッド4Fタイム",   "value": -0.12 },
 *         ...
 *       ]
 *     }
 *   ]
 * }
 */

import { NextRequest, NextResponse } from 'next/server'
import { getDb } from '@/lib/db'
import { shapLabel } from '@/lib/shapLabels'

export const dynamic = 'force-dynamic'

interface ShapEntry {
  feature: string
  label:   string
  value:   number
}

interface HorseShap {
  horse_name:     string
  predicted_rank: number | null
  shap:           ShapEntry[] | null
}

interface ShapResponse {
  prediction_id: number
  horses:        HorseShap[]
}

function parseShapJson(raw: string | null): ShapEntry[] | null {
  if (!raw) return null
  try {
    const obj = JSON.parse(raw) as Record<string, number>
    return Object.entries(obj).map(([feature, value]) => ({
      feature,
      label: shapLabel(feature),
      value,
    }))
  } catch {
    return null
  }
}

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ prediction_id: string }> },
) {
  const { prediction_id } = await params
  const pid = parseInt(prediction_id, 10)
  if (isNaN(pid)) {
    return NextResponse.json({ error: 'invalid prediction_id' }, { status: 400 })
  }

  const db = getDb()

  const rows = db.prepare(`
    SELECT horse_name, predicted_rank, shap_json
    FROM   prediction_horses
    WHERE  prediction_id = ?
    ORDER  BY predicted_rank ASC
  `).all(pid) as Array<{
    horse_name:     string
    predicted_rank: number | null
    shap_json:      string | null
  }>

  const horses: HorseShap[] = rows.map(r => ({
    horse_name:     r.horse_name,
    predicted_rank: r.predicted_rank,
    shap:           parseShapJson(r.shap_json),
  }))

  const body: ShapResponse = { prediction_id: pid, horses }
  return NextResponse.json(body)
}
