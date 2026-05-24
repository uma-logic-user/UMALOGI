/**
 * GET /api/compare/<race_id>
 *
 * legacy_logic と umasugi_engine の予想を並列比較する。
 *
 * umasugi_engine (TypeScript 実装):
 *   - track_style_score : 小回りコース適性（会場ベース）
 *   - turf_type_score   : 野芝/洋芝適性（札幌・函館 = 洋芝）
 *   - umasugi_score     = 0.70 × 0.5 + 0.10 × track + 0.10 × turf + 0.10 × 0.5
 *   - crowd_ev_penalty  : 世論圧力ペナルティ（x_consensus × odds_steam）
 *   - umasugi_ev        = legacy_ev × (1 − crowd_ev_penalty)
 */

import { NextRequest, NextResponse } from 'next/server'
import { getDb } from '@/lib/db'

export const dynamic = 'force-dynamic'

// ── コース分類定数 ────────────────────────────────────────────────────────
const KOMAWARI_VENUES = new Set(['小倉', '函館', '札幌', '福島'])
const OOMAWARI_VENUES = new Set(['東京', '京都', '中京', '新潟'])
const YOSHIBA_VENUES  = new Set(['札幌', '函館'])
const MIN_SAMPLES = 3
const OPINION_THRESHOLD = 0.4
const MAX_PENALTY = 0.35

// ── 小回り判定 ────────────────────────────────────────────────────────────
function isKomawari(venue: string, surface: string, distance: number): boolean | null {
  if (KOMAWARI_VENUES.has(venue)) return true
  if (OOMAWARI_VENUES.has(venue)) return false
  if (venue === '中山' && distance <= 1800) return true
  if (venue === '阪神' && surface === '芝' && distance <= 1400) return true
  return null
}

// ── [0,1] 正規化（比率 0.3〜3.0 を線形変換） ────────────────────────────
function clipNorm(ratio: number): number {
  const clipped = Math.max(0.3, Math.min(3.0, ratio))
  return (clipped - 0.3) / (3.0 - 0.3)
}

// ── sigmoid ──────────────────────────────────────────────────────────────
function sigmoid(x: number): number {
  return 1 / (1 + Math.exp(-x))
}

// ── 世論 EV ペナルティ ───────────────────────────────────────────────────
function calcCrowdPenalty(
  xConsensus: number,
  steamFlag: number,
  crowdBiasRatio: number,
): number {
  const oddsCompression = steamFlag * 0.6 + (1 - steamFlag) * 0.1
  const biasFactor = Math.max(0.5, Math.min(2.0, 2.0 - crowdBiasRatio))
  const rawPressure = Math.min(1.0, xConsensus * oddsCompression * biasFactor)
  return sigmoid(10 * (rawPressure - OPINION_THRESHOLD)) * MAX_PENALTY
}

// ── 型定義 ───────────────────────────────────────────────────────────────
interface HorseResult {
  horse_id: string
  venue: string
  surface: string
  distance: number
  rank: number
}

interface HorseStats {
  k_n: number; k_w: number  // 小回り
  o_n: number; o_w: number  // 大回り
  y_n: number; y_w: number  // 洋芝
  n_n: number; n_w: number  // 野芝
}

interface CompareHorse {
  horse_number: number
  horse_name: string
  legacy_ev: number
  umasugi_ev: number
  ev_delta: number
  track_style_score: number
  turf_type_score: number
  opinion_pressure_score: number
  crowd_ev_penalty: number
  umasugi_score: number
  recommendation: 'umasugi_better' | 'legacy_better' | 'agree'
}

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ race_id: string }> }
) {
  try {
    const { race_id } = await params
    const db = getDb()

    // ── 対象レース情報 ────────────────────────────────────────────────────
    const race = db.prepare(`
      SELECT race_id, race_name, date, venue, surface, distance
      FROM races WHERE race_id = ?
    `).get(race_id) as { race_id: string; race_name: string; date: string; venue: string; surface: string; distance: number } | undefined

    if (!race) {
      return NextResponse.json({ error: 'race_id not found', race_id }, { status: 404 })
    }

    // ── legacy 予想を取得（EV スコアが存在するもの） ──────────────────────
    const predictions = db.prepare(`
      SELECT
        p.id, p.bet_type, p.combination_json, p.expected_value,
        p.recommended_bet, p.notes
      FROM predictions p
      WHERE p.race_id = ?
        AND p.expected_value IS NOT NULL
      ORDER BY p.expected_value DESC
    `).all(race_id) as {
      id: number; bet_type: string; combination_json: string; expected_value: number
      recommended_bet: number; notes: string
    }[]

    if (predictions.length === 0) {
      return NextResponse.json({
        race_id,
        race_name: race.race_name,
        date: race.date,
        message: 'この race_id に対する予想が存在しません',
        horses: [],
        summary: { total: 0, umasugi_better: 0, legacy_better: 0, agree: 0 },
      })
    }

    // ── レースの馬情報（entries or race_results） ─────────────────────────
    type EntryRow = { horse_number: number; horse_id: string; horse_name: string }
    const entryRows = db.prepare(`
      SELECT horse_number, horse_id, horse_name
      FROM entries WHERE race_id = ?
      ORDER BY horse_number
    `).all(race_id) as EntryRow[]

    const raceResultRows = entryRows.length === 0
      ? (db.prepare(`
          SELECT horse_number, horse_id, horse_name
          FROM race_results WHERE race_id = ?
          ORDER BY horse_number
        `).all(race_id) as EntryRow[])
      : entryRows

    const horseMap = new Map<number, { horse_id: string; horse_name: string }>()
    for (const r of raceResultRows) {
      horseMap.set(r.horse_number, { horse_id: r.horse_id, horse_name: r.horse_name })
    }

    const horseIds = [...new Set(raceResultRows.map(r => r.horse_id))]
    if (horseIds.length === 0) {
      return NextResponse.json({ race_id, message: '出馬表データが未登録です', horses: [] })
    }

    // ── 過去成績バルク取得（基準日 = race の date より前） ──────────────
    const baseDate = race.date
    const ph = horseIds.map(() => '?').join(',')
    const histRows = db.prepare(`
      SELECT
        rr.horse_id,
        r.venue,
        r.surface,
        COALESCE(r.distance, 0) AS distance,
        rr.rank
      FROM race_results rr
      JOIN races r ON r.race_id = rr.race_id
      WHERE rr.horse_id IN (${ph})
        AND rr.rank IS NOT NULL AND rr.rank > 0
        AND r.venue IS NOT NULL AND r.venue != ''
        AND r.date < ?
    `).all(...horseIds, baseDate) as HorseResult[]

    // ── horse_id → stats を構築 ────────────────────────────────────────
    const statsMap = new Map<string, HorseStats>()
    for (const row of histRows) {
      if (!statsMap.has(row.horse_id)) {
        statsMap.set(row.horse_id, { k_n:0, k_w:0, o_n:0, o_w:0, y_n:0, y_w:0, n_n:0, n_w:0 })
      }
      const s = statsMap.get(row.horse_id)!
      const isWin = row.rank === 1 ? 1 : 0

      // track_style
      const ko = isKomawari(row.venue, row.surface, row.distance)
      if (ko === true)       { s.k_n++; s.k_w += isWin }
      else if (ko === false) { s.o_n++; s.o_w += isWin }

      // turf_type (ダート・障害レースを除外)
      const isNotDirt = row.surface !== 'ダート' && row.surface !== '障害'
      if (isNotDirt) {
        if (YOSHIBA_VENUES.has(row.venue)) { s.y_n++; s.y_w += isWin }
        else                               { s.n_n++; s.n_w += isWin }
      }
    }

    // ── 各馬スコア算出関数 ────────────────────────────────────────────────
    const raceIsKo = isKomawari(race.venue, race.surface, race.distance)
    const raceIsYo = YOSHIBA_VENUES.has(race.venue)

    function calcTrackStyle(horseId: string): number {
      const s = statsMap.get(horseId)
      if (!s) return 0.5
      const totalN = s.k_n + s.o_n
      if (totalN < MIN_SAMPLES) return 0.5
      const totalWR = (s.k_w + s.o_w) / totalN
      if (totalWR === 0) return 0.5
      if (raceIsKo === null) return 0.5
      const targetN = raceIsKo ? s.k_n : s.o_n
      const targetW = raceIsKo ? s.k_w : s.o_w
      if (targetN < MIN_SAMPLES) return 0.5
      return clipNorm(targetW / targetN / totalWR)
    }

    function calcTurfType(horseId: string): number {
      const s = statsMap.get(horseId)
      if (!s) return 0.5
      const totalN = s.y_n + s.n_n
      if (totalN < MIN_SAMPLES) return 0.5
      const totalWR = (s.y_w + s.n_w) / totalN
      if (totalWR === 0) return 0.5
      const targetN = raceIsYo ? s.y_n : s.n_n
      const targetW = raceIsYo ? s.y_w : s.n_w
      if (targetN < MIN_SAMPLES) return 0.5
      return clipNorm(targetW / targetN / totalWR)
    }

    // ── 予想ごとに umasugi_score を算出 ──────────────────────────────────
    const horses: CompareHorse[] = []

    for (const pred of predictions) {
      // combination_json から代表馬番を取得
      let primaryHorseNumber: number | null = null
      try {
        const combos = JSON.parse(pred.combination_json || '[]')
        for (const c of combos) {
          const n = Array.isArray(c) ? c[0] : c
          if (typeof n === 'number') { primaryHorseNumber = n; break }
        }
      } catch { /* ignore */ }

      const entry = primaryHorseNumber !== null ? horseMap.get(primaryHorseNumber) : undefined
      const horseId = entry?.horse_id ?? ''
      const horseName = entry?.horse_name ?? `馬番${primaryHorseNumber ?? '?'}`

      const trackStyle   = calcTrackStyle(horseId)
      const turfType     = calcTurfType(horseId)
      const umasugiScore = Math.min(1.0,
        0.70 * 0.5 + 0.10 * trackStyle + 0.10 * turfType + 0.10 * 0.5
      )

      // legacy EV = expected_value（モデルが算出した EV スコア）
      const legacyEv = pred.expected_value ?? 0

      // crowd_ev_penalty: notes に crowd_bias_ratio が含まれる場合に利用
      // (現状は x_consensus_score が未格納のため 0 扱い)
      const crowdPenalty = calcCrowdPenalty(0, 0, 1.0)
      const umasugiEv    = Math.max(0, legacyEv * (1 - crowdPenalty))
      const evDelta      = umasugiEv - legacyEv

      const recommendation: CompareHorse['recommendation'] =
        Math.abs(evDelta) < 0.05 ? 'agree'
        : evDelta > 0 ? 'umasugi_better'
        : 'legacy_better'

      horses.push({
        horse_number: primaryHorseNumber ?? -1,
        horse_name:   horseName,
        legacy_ev:    Math.round(legacyEv   * 1000) / 1000,
        umasugi_ev:   Math.round(umasugiEv  * 1000) / 1000,
        ev_delta:     Math.round(evDelta     * 1000) / 1000,
        track_style_score: Math.round(trackStyle  * 1000) / 1000,
        turf_type_score:   Math.round(turfType    * 1000) / 1000,
        opinion_pressure_score: 0,
        crowd_ev_penalty: Math.round(crowdPenalty * 1000) / 1000,
        umasugi_score:    Math.round(umasugiScore * 1000) / 1000,
        recommendation,
      })
    }

    // 重複 horse_number を除去して馬別に集約（最高 EV の予想を代表）
    const seen = new Set<number>()
    const deduped = horses.filter(h => {
      if (seen.has(h.horse_number)) return false
      seen.add(h.horse_number)
      return true
    })

    const summary = {
      total:           deduped.length,
      umasugi_better:  deduped.filter(h => h.recommendation === 'umasugi_better').length,
      legacy_better:   deduped.filter(h => h.recommendation === 'legacy_better').length,
      agree:           deduped.filter(h => h.recommendation === 'agree').length,
    }

    return NextResponse.json({
      race_id,
      race_name:    race.race_name,
      date:         race.date,
      venue:        race.venue,
      surface:      race.surface,
      distance:     race.distance,
      generated_at: new Date().toISOString(),
      horses:       deduped,
      summary,
    })

  } catch (err) {
    console.error('[/api/compare] error:', err)
    return NextResponse.json(
      { error: 'internal server error', detail: String(err) },
      { status: 500 },
    )
  }
}
