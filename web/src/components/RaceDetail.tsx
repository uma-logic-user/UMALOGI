'use client'

import { useState } from 'react'
import type { RaceEntry, Prediction, RacePayout, RaceResult, TrainingEval, RaceBias, EvRecommend } from '@/types/race'
import PredictionsPanel from './PredictionsPanel'
import BiasPanel from './BiasPanel'
import { TodayBuyPanel } from './TodayBuyPanel'

type Tab = 'race_card' | 'results' | 'predictions' | 'payouts' | 'today_buy'

const MEDAL: Record<number, string> = { 1: '🥇', 2: '🥈', 3: '🥉' }

const BET_ORDER: Record<string, number> = {
  '単勝': 1, '複勝': 2, '枠連': 3, '馬連': 4,
  'ワイド': 5, '馬単': 6, '三連複': 7, '三連単': 8,
}

// 払戻バリデーション: 不正な組み合わせをフィルタする
const BET_COMBO_SIZES: Record<string, [number, number]> = {
  '単勝': [1,1], '複勝': [1,1], '枠連': [2,2],
  '馬連': [2,2], 'ワイド': [2,2], '馬単': [2,2],
  '三連複': [3,3], '三連単': [3,3],
}
const BET_MAX_NUM: Record<string, number> = {
  '単勝': 18, '複勝': 18, '枠連': 8,
  '馬連': 18, 'ワイド': 18, '馬単': 18,
  '三連複': 18, '三連単': 18,
}

function isValidPayout(p: RacePayout): boolean {
  if (!p.combination || p.payout < 100) return false
  const sizes = BET_COMBO_SIZES[p.bet_type]
  const maxNum = BET_MAX_NUM[p.bet_type] ?? 18
  if (!sizes) return true // WIN5 等は通す
  const parts = p.combination.replace(/→/g, '-').split('-')
  if (parts.length < sizes[0] || parts.length > sizes[1]) {
    console.warn(`[PayoutGuard] invalid combo: ${p.bet_type} "${p.combination}" (${parts.length} parts, expected ${sizes[0]}-${sizes[1]})`)
    return false
  }
  return parts.every(pt => /^\d+$/.test(pt) && Number(pt) >= 1 && Number(pt) <= maxNum)
}

// odds_velocity がこの値以上で🔥シグナル
const VELOCITY_THRESHOLD = 0.05

// ── 的中結果マッチング用ヘルパー ─────────────────────────────
function parsePayoutCombo(combination: string): number[] {
  return combination.replace(/→/g, '-').split('-').map(Number)
}

function parsePredComboJson(combinationJson: string | null): number[][] {
  if (!combinationJson) return []
  try {
    const parsed = JSON.parse(combinationJson)
    if (!Array.isArray(parsed) || parsed.length === 0) return []
    return Array.isArray(parsed[0]) ? (parsed as number[][]) : [(parsed as number[])]
  } catch { return [] }
}

function comboMatches(payoutNums: number[], predNums: number[], betType: string): boolean {
  if (payoutNums.length !== predNums.length) return false
  const ordered = betType === '三連単' || betType === '馬単'
  if (ordered) return payoutNums.every((n, i) => n === predNums[i])
  const sp = [...payoutNums].sort((a, b) => a - b)
  const sd = [...predNums].sort((a, b) => a - b)
  return sp.every((n, i) => n === sd[i])
}

function getHitPayouts(predictions: Prediction[], allPayouts: RacePayout[]): RacePayout[] {
  const hitPreds = predictions.filter(p => p.is_hit === 1)
  if (hitPreds.length === 0) return []
  const result: RacePayout[] = []
  const seen = new Set<string>()
  for (const pred of hitPreds) {
    const predCombos = parsePredComboJson(pred.combination_json)
    for (const payout of allPayouts) {
      if (payout.bet_type !== pred.bet_type) continue
      if (!isValidPayout(payout)) continue
      const payoutNums = parsePayoutCombo(payout.combination)
      for (const predNums of predCombos) {
        if (comboMatches(payoutNums, predNums, pred.bet_type)) {
          const key = `${payout.bet_type}:${payout.combination}`
          if (!seen.has(key)) { seen.add(key); result.push(payout) }
        }
      }
    }
  }
  return result
}

interface Props {
  race:        RaceEntry & {
    prerace?: {
      bias:         RaceBias
      ev_recommend: EvRecommend[]
      generated_at: string
    }
  }
  predictions: Prediction[]
}

export default function RaceDetail({ race, predictions }: Props) {
  const hasPrerace = !!race.prerace
  const [tab, setTab] = useState<Tab>('race_card')

  const payoutsByType = (race.payouts ?? [])
    .filter(isValidPayout)
    .reduce<Record<string, RacePayout[]>>(
      (acc, p) => { (acc[p.bet_type] ??= []).push(p); return acc },
      {},
    )
  const betTypes = Object.keys(payoutsByType).sort(
    (a, b) => (BET_ORDER[a] ?? 99) - (BET_ORDER[b] ?? 99),
  )

  const hasPayouts     = betTypes.length > 0
  const hasPredictions = predictions.length > 0

  const hitPayouts = getHitPayouts(predictions, race.payouts ?? [])
  const hitPayoutsByType = hitPayouts.reduce<Record<string, RacePayout[]>>(
    (acc, p) => { (acc[p.bet_type] ??= []).push(p); return acc },
    {},
  )
  const hitBetTypes = Object.keys(hitPayoutsByType).sort(
    (a, b) => (BET_ORDER[a] ?? 99) - (BET_ORDER[b] ?? 99),
  )

  const tabs: { key: Tab; label: string; count?: number }[] = [
    { key: 'race_card',   label: '出馬表' },
    { key: 'results',     label: 'レース結果' },
    { key: 'predictions', label: 'AI予想', count: hasPredictions ? predictions.length : undefined },
    { key: 'payouts',     label: '的中結果', count: hitPayouts.length > 0 ? hitPayouts.length : undefined },
    { key: 'today_buy',   label: '当日購入' },
  ]

  const winOddsMap = Object.fromEntries(
    (race.results ?? [])
      .filter(r => r.horse_number != null && r.win_odds != null)
      .map(r => [r.horse_number!, r.win_odds!])
  ) as Record<number, number>

  return (
    <div className="p-4 space-y-4 max-w-[1400px]">

      {/* ── レースヘッダー ─────────────────────────── */}
      <div className="neon-card-bright p-5">
        <div className="flex items-start justify-between gap-4 mb-4">
          <div>
            <div className="text-xs text-[var(--text-muted)] tracking-[0.2em] mb-1">
              {race.date}
              {race.venue && <>&nbsp;·&nbsp;{race.venue}</>}
              {race.race_number != null && <>&nbsp;·&nbsp;第 {race.race_number} R</>}
            </div>
            <h1 className="text-2xl font-bold neon-text tracking-wider leading-tight">
              {race.race_name || `第 ${race.race_number} レース`}
            </h1>
          </div>
          <div className="text-right shrink-0">
            <div className="text-[10px] text-[var(--text-muted)] mb-1 tracking-wider">RACE ID</div>
            <div className="font-mono text-xs opacity-40" style={{ color: 'var(--text-muted)' }}>{race.race_id}</div>
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          <AttrBadge label="馬場" value={`${race.surface}${race.track_direction || ''}`} />
          <AttrBadge label="距離" value={`${race.distance}m`} />
          {race.weather   && <AttrBadge label="天候" value={race.weather} />}
          {race.condition && (
            <AttrBadge label="馬場状態" value={race.condition} highlight={race.condition === '良'} />
          )}
        </div>

        {/* EV推奨馬サマリー（prerace がある場合） */}
        {hasPrerace && race.prerace!.ev_recommend.length > 0 && (
          <div className="mt-4 pt-3 border-t border-[var(--border)]">
            <div className="text-[10px] text-[var(--text-muted)] tracking-[0.2em] mb-2 uppercase">
              激アツ推奨馬 — EV &gt;= 1.0
            </div>
            <div className="flex flex-wrap gap-2">
              {race.prerace!.ev_recommend.map(h => (
                <div
                  key={h.horse_number}
                  className="badge-hot"
                >
                  🔥 {h.horse_number}番 {h.horse_name}
                  <span className="ml-1 opacity-70">EV {h.ev_score.toFixed(2)}</span>
                  {h.kelly_fraction > 0 && (
                    <span className="ml-1 opacity-60">
                      ¥{Math.round(h.kelly_fraction * 10000)}推奨
                    </span>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* ── タブ ───────────────────────────────────── */}
      <div className="flex gap-0 border-b border-[var(--border)]">
        {tabs.map(t => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={`relative px-5 md:px-6 text-sm font-semibold tracking-wider transition-colors min-h-[44px] flex items-center gap-1 ${
              tab === t.key
                ? 'text-[var(--neon-cyan)]'
                : 'text-[var(--text-muted)] hover:text-[var(--text-primary)]'
            }`}
          >
            {t.label}
            {t.count != null && (
              <span className="ml-2 text-xs font-normal opacity-60">{t.count}</span>
            )}
            {tab === t.key && (
              <span
                className="absolute bottom-0 inset-x-0 h-[2px] bg-[var(--neon-cyan)]"
              />
            )}
          </button>
        ))}
      </div>

      {/* ── 出馬表タブ ────────────────────────────── */}
      {tab === 'race_card' && (
        <RaceCardTable results={race.results ?? []} />
      )}

      {/* ── レース結果タブ ─────────────────────────── */}
      {tab === 'results' && (
        <ResultsTable results={race.results ?? []} payouts={race.payouts ?? []} />
      )}

      {/* ── AI予想タブ ────────────────────────────── */}
      {tab === 'predictions' && (
        <div className="space-y-4">
          <SnsButtons race={race} predictions={predictions} />
          {hasPrerace && (
            <>
              <BiasPanel
                bias={race.prerace!.bias}
                condition={race.condition || ''}
              />
              <PreraceTable results={race.results ?? []} />
            </>
          )}
          {hasPredictions
            ? <PredictionsPanel predictions={predictions} limit={200} payouts={race.payouts ?? []} />
            : (
              <div className="neon-card p-12 text-center">
                <div className="text-[var(--text-muted)] text-base tracking-widest">
                  このレースの予想データはありません
                </div>
              </div>
            )
          }
        </div>
      )}

      {/* ── 的中結果タブ ──────────────────────────── */}
      {tab === 'payouts' && (
        hitPayouts.length > 0
          ? (
            <div className="neon-card overflow-hidden">
              <div className="px-4 py-3 border-b border-[var(--border)]">
                <span className="text-sm neon-text tracking-[0.2em] font-semibold">
                  AI的中結果 — {hitPayouts.length}件
                </span>
              </div>
              <div className="p-4 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
                {hitBetTypes.map(betType => (
                  <PayoutCard
                    key={betType}
                    betType={betType}
                    payouts={hitPayoutsByType[betType]}
                  />
                ))}
              </div>
            </div>
          )
          : (
            <div className="neon-card p-12 text-center">
              <div className="text-[var(--text-muted)] text-base tracking-widest">
                {hasPredictions
                  ? 'このレースのAI買い目に的中はありませんでした'
                  : '予想データがないため的中結果を表示できません'}
              </div>
              {hasPayouts && !hasPredictions && (
                <div className="mt-3 text-xs text-[var(--text-muted)] opacity-60">
                  払戻情報は「レース結果」タブでご確認いただけます
                </div>
              )}
            </div>
          )
      )}

      {/* ── 当日購入タブ ──────────────────────────── */}
      {tab === 'today_buy' && (
        <TodayBuyPanel
          predictions={predictions}
          winOddsMap={winOddsMap}
          raceId={race.race_id}
        />
      )}
    </div>
  )
}

// ── 買い目フォーマット: マルチ・軸流し・ボックス自動判定 ──────────
function formatBetCompact(
  combinationJson: string | null,
  betType: string,
  nTickets: number,
  ev?: number | null,
): string {
  const evStr  = ev != null && ev > 0 ? `  EV=${ev.toFixed(2)}` : ''
  const costStr = nTickets > 0 ? `  ¥${(nTickets * 100).toLocaleString()}` : ''

  if (!combinationJson) return `${betType} ${nTickets}点${evStr}${costStr}`

  let combos: number[][]
  try {
    const parsed = JSON.parse(combinationJson)
    if (!Array.isArray(parsed) || parsed.length === 0) return `${betType} ${nTickets}点${evStr}${costStr}`
    combos = Array.isArray(parsed[0]) ? (parsed as number[][]) : [(parsed as number[])]
  } catch {
    return `${betType} ${nTickets}点${evStr}${costStr}`
  }

  const n         = combos.length
  const allNums   = new Set(combos.flatMap(c => c))
  const sorted    = [...allNums].sort((a, b) => a - b)

  // 単勝・複勝
  if (betType === '単勝' || betType === '複勝') {
    const nums = combos.map(c => c[0]).join(', ')
    return `${betType} ${nums}番  計${n}点${evStr}${costStr}`
  }

  // 三連単: マルチ（位置不問）または1着固定
  if (betType === '三連単') {
    const alwaysIn = sorted.filter(h => combos.every(c => c.includes(h)))
    if (alwaysIn.length >= 1) {
      const others = sorted.filter(n => !alwaysIn.includes(n)).join(', ')
      return `【三連単マルチ 軸${alwaysIn.join(', ')} - 相手${others}】 計${n}点${evStr}${costStr}`
    }
    const firsts = [...new Set(combos.map(c => c[0]))].sort((a, b) => a - b)
    if (firsts.length <= 2) {
      const others = sorted.filter(n => !firsts.includes(n)).join(', ')
      return `【三連単 軸${firsts.join(', ')} - 相手${others}】 計${n}点${evStr}${costStr}`
    }
    return `三連単フォーメーション 計${n}点${evStr}${costStr}`
  }

  // 三連複・馬連・ワイド・馬単: 軸流し or ボックス
  const axes = sorted.filter(h => combos.every(c => c.includes(h)))
  if (axes.length >= 1) {
    const others = sorted.filter(n => !axes.includes(n)).join(', ')
    return `【${betType}流し 軸${axes.join(', ')} - 相手${others}】 計${n}点${evStr}${costStr}`
  }
  return `${betType}ボックス ${sorted.join(', ')}番  計${n}点${evStr}${costStr}`
}

// ── クリップボード書き込み（HTTPS以外のフォールバック対応）───────
async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text)
    return true
  } catch {
    try {
      const el = document.createElement('textarea')
      el.value = text
      el.style.cssText = 'position:fixed;left:-9999px;top:-9999px;opacity:0'
      document.body.appendChild(el)
      el.select()
      const ok = document.execCommand('copy')
      document.body.removeChild(el)
      return ok
    } catch {
      return false
    }
  }
}

// ── X（旧Twitter）用テキスト生成（≤280文字・動的ハッシュタグ）───
function buildXText(
  race: RaceEntry & { prerace?: { ev_recommend: EvRecommend[]; bias: RaceBias; generated_at: string } },
  predictions: Prediction[],
): string {
  const venue   = race.venue ?? ''
  const raceNum = race.race_number ?? ''
  const name    = race.race_name || `${raceNum}R`

  // ハッシュタグ: 重賞名は数字・スペースを除いた短縮形のみ
  const isGrade    = name && !/^第\d+/.test(name) && !/^\d/.test(name)
  const gradeTag   = isGrade ? ` #${name.replace(/[\s　]/g, '')}` : ''
  const hashtags   = `#${venue}${raceNum}R #UMALOGI #競馬AI${gradeTag}`

  // 最高EV買い目を1行にまとめる
  const topBet = [...predictions]
    .filter(p => (p.expected_value ?? 0) >= 1.0)
    .sort((a, b) => (b.expected_value ?? 0) - (a.expected_value ?? 0))[0]

  let betLine = ''
  if (topBet) {
    const model = topBet.model_type.replace(/\(直前\)|\(暫定\)/g, '').trim()
    betLine = `\n${model}: ${formatBetCompact(topBet.combination_json, topBet.bet_type, topBet.n_tickets, topBet.expected_value)}`
  } else if ((race.prerace?.ev_recommend ?? []).length > 0) {
    const top = race.prerace!.ev_recommend[0]
    betLine = `\n本命◎${top.horse_number}番${top.horse_name}  EV${top.ev_score.toFixed(2)}`
  }

  const full = `【UMALOGI直前AI予想】${venue}${raceNum}R「${name}」${betLine}\n🤖フィルター済 🆓1レース目無料 📲Discord/NOTEで全買い目公開\n${hashtags}`
  if (full.length <= 280) return full

  // 280字を超えたら簡略版
  const brief = `【UMALOGI AI予想】${venue}${raceNum}R${betLine ? '\n' + betLine.trim() : ''}\n🤖フィルター済\n${hashtags}`
  return brief.slice(0, 280)
}

// ── NOTE用テキスト生成（Markdown・マルチ/軸流し表記）─────────────
function buildNoteText(
  race: RaceEntry & { prerace?: { ev_recommend: EvRecommend[]; bias: RaceBias; generated_at: string } },
  predictions: Prediction[],
): string {
  const venue   = race.venue ?? ''
  const raceNum = race.race_number ?? ''
  const name    = race.race_name || `${raceNum}R`
  const date    = race.date ?? ''
  const surf    = `${race.surface ?? ''}${race.track_direction ?? ''}${race.distance ?? 0}m`

  const lines: string[] = [
    `## 🏇 ${venue} ${raceNum}R「${name}」— AI厳選予想`,
    `> ${date}　${surf}　馬場: ${race.condition || '—'}`,
    `> 🤖 **AIウマスギROIフィルター適用済み**（EV≥1.5のみ発注 · 本命三連単は条件付き許可）`,
    '',
  ]

  const qualified = predictions.filter(p => (p.expected_value ?? 0) >= 1.0)
  if (qualified.length === 0) {
    lines.push('*このレースのAI予想データはありません。*')
    return lines.join('\n')
  }

  // モデルごとにグループ化（表示順: 本命 → 卍 → Alpha）
  const ORDER: Record<string, number> = { '本命': 1, '卍': 2, 'Alpha': 3 }
  const byModel = new Map<string, Prediction[]>()
  for (const p of qualified) {
    const key = p.model_type.replace(/\(直前\)|\(暫定\)/g, '').trim()
    if (!byModel.has(key)) byModel.set(key, [])
    byModel.get(key)!.push(p)
  }
  const modelKeys = [...byModel.keys()].sort((a, b) => {
    const oa = Object.entries(ORDER).find(([k]) => a.includes(k))?.[1] ?? 9
    const ob = Object.entries(ORDER).find(([k]) => b.includes(k))?.[1] ?? 9
    return oa - ob
  })

  let totalCost = 0
  for (const key of modelKeys) {
    const bets = byModel.get(key)!.sort((a, b) => (b.expected_value ?? 0) - (a.expected_value ?? 0))
    const icon = key.includes('卍') ? '🟩' : key.toLowerCase().includes('alpha') ? '🟦' : '🟥'
    lines.push(`### ${icon} ${key}`)
    for (const bet of bets) {
      lines.push(`- ${formatBetCompact(bet.combination_json, bet.bet_type, bet.n_tickets, bet.expected_value)}`)
      totalCost += bet.n_tickets * 100
    }
    lines.push('')
  }

  lines.push(`**合計投資額**: ¥${totalCost.toLocaleString()}`)
  lines.push('')
  lines.push('---')
  lines.push(`*UMALOGI AI予測システム | ${date}*`)

  return lines.join('\n')
}

// ── X用/NOTE用 2ボタンコピーコンポーネント ───────────────────────
function SnsButtons({
  race,
  predictions,
}: {
  race: RaceEntry & { prerace?: { ev_recommend: EvRecommend[]; bias: RaceBias; generated_at: string } }
  predictions: Prediction[]
}) {
  const [xCopied,    setXCopied]    = useState(false)
  const [noteCopied, setNoteCopied] = useState(false)
  const [failed,     setFailed]     = useState(false)

  const handleX = async () => {
    const text = buildXText(race, predictions)
    const ok = await copyToClipboard(text)
    if (ok) {
      setXCopied(true)
      setTimeout(() => setXCopied(false), 2000)
    } else {
      setFailed(true)
      setTimeout(() => setFailed(false), 2000)
    }
  }

  const handleNote = async () => {
    const text = buildNoteText(race, predictions)
    const ok = await copyToClipboard(text)
    if (ok) {
      setNoteCopied(true)
      setTimeout(() => setNoteCopied(false), 2000)
    } else {
      setFailed(true)
      setTimeout(() => setFailed(false), 2000)
    }
  }

  return (
    <div className="flex flex-wrap gap-2 justify-end items-center">
      {failed && (
        <span className="text-xs text-[var(--neon-red)] opacity-80">
          ⚠ クリップボードへのアクセスが拒否されました
        </span>
      )}
      {/* X（Twitter）用 */}
      <button
        onClick={handleX}
        className={`px-3 py-2 text-xs font-semibold rounded-md tracking-wider transition-all ${
          xCopied
            ? 'bg-[rgba(91,138,91,0.15)] text-[var(--neon-green)] border border-[rgba(91,138,91,0.35)]'
            : 'bg-[rgba(200,168,130,0.08)] text-[var(--neon-cyan)] border border-[var(--border)] hover:bg-[rgba(200,168,130,0.15)]'
        }`}
      >
        {xCopied ? '✅ コピー完了' : '𝕏 X用コピー（≤280字）'}
      </button>
      {/* NOTE用 */}
      <button
        onClick={handleNote}
        className={`px-3 py-2 text-xs font-semibold rounded-md tracking-wider transition-all ${
          noteCopied
            ? 'bg-[rgba(91,138,91,0.15)] text-[var(--neon-green)] border border-[rgba(91,138,91,0.35)]'
            : 'bg-[rgba(184,134,11,0.08)] text-[var(--neon-gold)] border border-[rgba(184,134,11,0.25)] hover:bg-[rgba(184,134,11,0.15)]'
        }`}
      >
        {noteCopied ? '✅ コピー完了' : '📝 NOTE用コピー（Markdown）'}
      </button>
    </div>
  )
}

// ── 出馬表テーブル ────────────────────────────────────────
function RaceCardTable({ results }: { results: RaceResult[] }) {
  const sorted = [...results].sort((a, b) => (a.horse_number ?? 99) - (b.horse_number ?? 99))

  return (
    <div className="neon-card overflow-hidden">
      <div className="px-4 py-3 border-b border-[var(--border)]">
        <span className="text-sm neon-text tracking-[0.2em] font-semibold">
          RACE CARD — 出馬表
        </span>
      </div>

      {/* デスクトップ */}
      <div className="hidden md:block table-scroll">
        <table className="w-full race-table">
          <thead>
            <tr>
              <th className="text-center">枠</th>
              <th className="text-center">馬番</th>
              <th className="text-left">馬名</th>
              <th>性齢</th>
              <th className="text-right">斤量</th>
              <th>騎手</th>
              <th>厩舎</th>
              <th className="text-right">単勝</th>
              <th className="text-center">人気</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r, i) => (
              <tr key={r.horse_name + i}>
                <td className="text-center">
                  {r.gate_number != null ? <GateBadge gate={r.gate_number} /> : <span className="text-[var(--text-muted)]">—</span>}
                </td>
                <td className="text-center font-mono text-[var(--text-muted)]">{r.horse_number ?? '—'}</td>
                <td>
                  <span className="font-semibold text-[var(--text-primary)]">{r.horse_name}</span>
                </td>
                <td className="text-[var(--text-muted)]">{r.sex_age}</td>
                <td className="text-right font-mono">{r.weight_carried}</td>
                <td>{r.jockey}</td>
                <td className="text-[var(--text-muted)]">{r.trainer || '—'}</td>
                <td className="text-right font-mono"><OddsCell odds={r.win_odds} /></td>
                <td className="text-center font-mono text-[var(--text-muted)]">
                  {r.popularity != null ? `${r.popularity}人気` : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* モバイル */}
      <div className="md:hidden">
        <div style={{ padding: '8px 10px', display: 'flex', flexDirection: 'column', gap: 6 }}>
          {sorted.map((r, i) => (
            <div key={r.horse_name + i} className="horse-row-card">
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 3, paddingTop: 2 }}>
                <span className="horse-num-lg">{r.horse_number}</span>
                {r.gate_number != null && <GateBadge gate={r.gate_number} />}
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                <span style={{ fontSize: '0.92rem', fontWeight: 700, color: 'var(--text-primary)' }}>
                  {r.horse_name}
                </span>
                <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>
                  {r.sex_age} · {r.weight_carried}kg · {r.jockey}
                  {r.trainer ? ` / ${r.trainer}` : ''}
                </div>
                <div style={{ display: 'flex', gap: 12 }}>
                  <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                    単勝 <OddsCell odds={r.win_odds} />
                  </span>
                  {r.popularity != null && (
                    <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                      {r.popularity}人気
                    </span>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// ── AI直前分析テーブル ────────────────────────────────────
function PreraceTable({ results }: { results: RaceResult[] }) {
  const sorted = [...results].sort((a, b) => (a.horse_number ?? 99) - (b.horse_number ?? 99))

  return (
    <div className="neon-card overflow-hidden">
      <div className="px-4 py-3 border-b border-[var(--border)]">
        <span className="text-sm neon-text tracking-[0.2em] font-semibold">
          PRE-RACE ANALYSIS — 直前情報
        </span>
      </div>

      {/* ── デスクトップ: テーブル ── */}
      <div className="hidden md:block table-scroll">
        <table className="w-full race-table">
          <thead>
            <tr>
              <th className="text-center">馬番</th>
              <th className="text-left">馬名</th>
              <th className="text-right">単勝</th>
              <th className="text-center">調教</th>
              <th className="text-right">本命スコア</th>
              <th className="text-right">EV</th>
              <th className="text-right">単勝Kelly推奨</th>
              <th className="text-right">オッズ朝比</th>
              <th className="text-center">大口</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r, i) => {
              const ev       = r.ev_score ?? 0
              const kelly    = r.kelly_fraction ?? 0
              const velocity = r.odds_velocity ?? 0
              const isHot    = ev >= 1.0
              const isFire   = velocity >= VELOCITY_THRESHOLD

              return (
                <tr
                  key={r.horse_name + i}
                  className={isHot ? 'row-hot' : ''}
                  style={isHot ? { borderLeft: '2px solid var(--neon-red)' } : {}}
                >
                  <td className="text-center">
                    {r.gate_number != null ? <GateBadge gate={r.gate_number} /> : null}
                    <span className="ml-1 font-mono text-[var(--text-muted)]">{r.horse_number}</span>
                  </td>
                  <td>
                    <div className="flex items-center gap-2">
                      <span className={`font-semibold ${isHot ? 'neon-text-red' : 'text-[var(--text-primary)]'}`}>
                        {r.horse_name}
                      </span>
                      {isHot && <span className="badge-hot" style={{ fontSize: '0.65rem', padding: '1px 5px' }}>激アツ</span>}
                    </div>
                    <div className="text-[10px] text-[var(--text-muted)] mt-0.5">
                      {r.sex_age} {r.weight_carried}kg
                    </div>
                  </td>
                  <td className="text-right font-mono"><OddsCell odds={r.win_odds} /></td>
                  <td className="text-center">
                    {r.training_eval ? <EvalBadge eval={r.training_eval} /> : <span className="text-[var(--text-muted)]">—</span>}
                  </td>
                  <td className="text-right font-mono text-xs">
                    {r.honmei_score != null
                      ? <span style={{ color: scoreColor(r.honmei_score ?? 0) }}>{((r.honmei_score ?? 0) * 100).toFixed(1)}%</span>
                      : <span className="text-[var(--text-muted)]">—</span>}
                  </td>
                  <td className="text-right font-mono">
                    {r.ev_score != null
                      ? <span className={ev >= 1.0 ? 'neon-text-red font-bold' : ev >= 0.8 ? 'neon-text-gold' : 'text-[var(--text-muted)]'}>
                          {ev.toFixed(2)}
                        </span>
                      : <span className="text-[var(--text-muted)]">—</span>}
                  </td>
                  <td className="text-right font-mono text-xs">
                    {kelly > 0
                      ? <span className="neon-text-green">¥{Math.round(kelly * 100000).toLocaleString()}</span>
                      : <span className="text-[var(--text-muted)]">—</span>}
                  </td>
                  <td className="text-right font-mono text-xs">
                    {r.odds_vs_morning != null
                      ? <span style={{ color: r.odds_vs_morning < 0.85 ? 'var(--neon-red)' : r.odds_vs_morning < 0.95 ? 'var(--neon-gold)' : 'var(--text-muted)' }}>
                          ×{r.odds_vs_morning.toFixed(2)}
                        </span>
                      : <span className="text-[var(--text-muted)]">—</span>}
                  </td>
                  <td className="text-center">
                    {isFire
                      ? <span className="signal-fire" title={`下落速度 ${velocity.toFixed(3)}/分`}>🔥</span>
                      : <span className="text-[var(--text-muted)]">·</span>}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {/* ── モバイル: EV重視カード ── */}
      <div className="md:hidden">
        <div style={{ padding: '8px 10px', display: 'flex', flexDirection: 'column', gap: 6 }}>
        {sorted.map((r, i) => {
          const ev       = r.ev_score ?? 0
          const kelly    = r.kelly_fraction ?? 0
          const velocity = r.odds_velocity ?? 0
          const isHot    = ev >= 1.0
          const isFire   = velocity >= VELOCITY_THRESHOLD

          return (
            <div
              key={r.horse_name + i}
              className={`horse-row-card ${isHot ? 'hot-card' : ''}`}
              style={{ gridTemplateColumns: '46px 1fr', alignItems: 'start' }}
            >
              {/* 左: 馬番サークル */}
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 3, paddingTop: 2 }}>
                <span className="horse-num-lg" style={{
                  background: isHot ? 'rgba(196,64,64,0.15)' : undefined,
                  color: isHot ? 'var(--neon-red)' : undefined,
                  borderColor: isHot ? 'rgba(196,64,64,0.45)' : undefined,
                }}>
                  {r.horse_number}
                </span>
                {r.gate_number != null && <GateBadge gate={r.gate_number} />}
              </div>

              {/* 右: 馬名 + 指標 */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                {/* 馬名行 */}
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 6 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 5, flexWrap: 'wrap' }}>
                    <span style={{
                      fontSize: '0.92rem', fontWeight: 700,
                      color: isHot ? 'var(--neon-red)' : 'var(--text-primary)',
                    }}>{r.horse_name}</span>
                    {isHot && (
                      <span className="badge-hot" style={{ fontSize: '0.6rem', padding: '1px 5px' }}>激アツ</span>
                    )}
                    {isFire && (
                      <span className="signal-fire" title={`下落速度 ${velocity.toFixed(3)}/分`}>🔥</span>
                    )}
                  </div>
                  {/* EV 大型 */}
                  {r.ev_score != null && (
                    <div style={{ textAlign: 'right', flexShrink: 0 }}>
                      <div style={{ fontSize: '0.5rem', color: 'var(--text-muted)', letterSpacing: '0.15em' }}>EV</div>
                      <div style={{
                        fontFamily: 'monospace', fontWeight: 900, fontSize: '1.5rem', lineHeight: 1,
                        color: ev >= 1.0 ? 'var(--neon-red)' : ev >= 0.8 ? 'var(--neon-gold)' : 'var(--text-muted)',
                        textShadow: 'none',
                      }}>{ev.toFixed(2)}</div>
                    </div>
                  )}
                </div>

                {/* 性齢 + 騎手 */}
                <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>
                  {r.sex_age} · {r.weight_carried}kg
                </div>

                {/* 指標行 */}
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px 12px' }}>
                  <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                    単勝 <OddsCell odds={r.win_odds} />
                  </span>
                  {r.honmei_score != null && (
                    <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                      本命 <span style={{ color: scoreColor(r.honmei_score ?? 0), fontWeight: 700 }}>
                        {((r.honmei_score ?? 0) * 100).toFixed(1)}%
                      </span>
                    </span>
                  )}
                  {r.training_eval && (
                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 3, fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                      調教 <EvalBadge eval={r.training_eval} />
                    </span>
                  )}
                  {kelly > 0 && (
                    <span style={{ fontSize: '0.72rem' }} className="neon-text-green">
                      Kelly ¥{Math.round(kelly * 100000).toLocaleString()}
                    </span>
                  )}
                  {r.odds_vs_morning != null && (
                    <span style={{
                      fontSize: '0.72rem',
                      color: r.odds_vs_morning < 0.85 ? 'var(--neon-red)' : r.odds_vs_morning < 0.95 ? 'var(--neon-gold)' : 'var(--text-muted)',
                    }}>
                      朝比 ×{r.odds_vs_morning.toFixed(2)}
                    </span>
                  )}
                </div>
              </div>
            </div>
          )
        })}
        </div>
      </div>

      <div className="px-4 py-2 text-[10px] text-[var(--text-muted)] border-t border-[var(--border)]">
        単勝Kelly推奨 = このバンクロールでの単勝買い参考額（1/4 Kelly）/ EV≥1.0 = 激アツ推奨馬 / 🔥 = 急激なオッズ下落（大口投票シグナル）
      </div>
    </div>
  )
}

// ── レース結果テーブル ─────────────────────────────────────
function ResultsTable({ results, payouts }: { results: RaceResult[]; payouts: RacePayout[] }) {
  const validPayouts = payouts.filter(isValidPayout)
  const payoutsByType = validPayouts.reduce<Record<string, RacePayout[]>>(
    (acc, p) => { (acc[p.bet_type] ??= []).push(p); return acc },
    {},
  )
  const payoutBetTypes = Object.keys(payoutsByType).sort(
    (a, b) => (BET_ORDER[a] ?? 99) - (BET_ORDER[b] ?? 99),
  )

  return (
    <div className="neon-card overflow-hidden">
      <div className="px-4 py-3 border-b border-[var(--border)]">
        <span className="text-sm neon-text tracking-[0.2em] font-semibold">
          RACE RESULTS — {results.length} runners
        </span>
      </div>

      {/* ── デスクトップ: テーブル ── */}
      <div className="hidden md:block table-scroll">
        <table className="w-full race-table">
          <thead>
            <tr>
              <th className="text-center">着順</th>
              <th className="text-center">枠</th>
              <th className="text-center">馬番</th>
              <th className="text-left">馬名</th>
              <th>性齢</th>
              <th className="text-right">斤量</th>
              <th>騎手</th>
              <th>厩舎</th>
              <th className="text-right">タイム</th>
              <th>着差</th>
              <th className="text-right">馬体重</th>
              <th className="text-right">単勝</th>
              <th className="text-center">人気</th>
              <th className="text-center">調教</th>
            </tr>
          </thead>
          <tbody>
            {results.map((r, i) => (
              <tr
                key={r.horse_name + i}
                className={`
                  ${r.rank === 1 ? 'row-rank-1' : ''}
                  ${r.rank === 2 ? 'row-rank-2' : ''}
                  ${r.rank === 3 ? 'row-rank-3' : ''}
                `}
              >
                <td className="text-center font-bold">
                  {r.rank != null
                    ? MEDAL[r.rank] ?? <span className="text-[var(--text-muted)]">{r.rank}</span>
                    : <span className="text-[var(--text-muted)]">—</span>}
                </td>
                <td className="text-center">
                  {r.gate_number != null ? <GateBadge gate={r.gate_number} /> : <span className="text-[var(--text-muted)]">—</span>}
                </td>
                <td className="text-center font-mono text-[var(--text-muted)]">{r.horse_number ?? '—'}</td>
                <td>
                  <span className={`font-semibold ${
                    r.rank === 1 ? 'neon-text-gold' :
                    r.rank != null && r.rank <= 3 ? 'text-[var(--text-primary)]' : 'text-[var(--text-muted)]'
                  }`}>{r.horse_name}</span>
                </td>
                <td className="text-[var(--text-muted)]">{r.sex_age}</td>
                <td className="text-right font-mono">{r.weight_carried}</td>
                <td>{r.jockey}</td>
                <td className="text-[var(--text-muted)]">{r.trainer || '—'}</td>
                <td className={`text-right font-mono ${r.rank === 1 ? 'neon-text' : 'text-[var(--text-primary)]'}`}>
                  {r.finish_time || '—'}
                </td>
                <td className="text-[var(--text-muted)]">
                  {r.margin || (r.rank === 1 ? <span className="neon-text">◎</span> : '—')}
                </td>
                <td className="text-right font-mono text-[var(--text-muted)]">
                  {r.horse_weight != null ? (
                    <>
                      {r.horse_weight}
                      {r.horse_weight_diff != null && (
                        <span className={`ml-1 text-xs ${
                          r.horse_weight_diff > 0 ? 'text-[var(--neon-red)]' :
                          r.horse_weight_diff < 0 ? 'text-[var(--neon-cyan)]' : 'text-[var(--text-muted)]'
                        }`}>
                          ({r.horse_weight_diff > 0 ? '+' : ''}{r.horse_weight_diff})
                        </span>
                      )}
                    </>
                  ) : '—'}
                </td>
                <td className="text-right font-mono"><OddsCell odds={r.win_odds} /></td>
                <td className="text-center"><PopBadge pop={r.popularity} /></td>
                <td className="text-center">
                  {r.training_eval ? <EvalBadge eval={r.training_eval} /> : <span className="text-[var(--text-muted)]">—</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* ── モバイル: 着順カード ── */}
      <div className="md:hidden">
        <div style={{ padding: '8px 10px', display: 'flex', flexDirection: 'column', gap: 6 }}>
        {results.map((r, i) => {
          const rankClass = r.rank === 1 ? 'rank-1' : r.rank === 2 ? 'rank-2' : r.rank === 3 ? 'rank-3' : ''
          const medalNum  = r.rank === 1 ? 'medal-1' : r.rank === 2 ? 'medal-2' : r.rank === 3 ? 'medal-3' : ''

          return (
            <div key={r.horse_name + i} className={`horse-row-card ${rankClass}`}>
              {/* 左: 着順メダル + 馬番 */}
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 4 }}>
                <span className={`horse-num-lg ${medalNum}`} style={{ fontSize: '1.1rem' }}>
                  {r.horse_number}
                </span>
                {r.rank != null && (
                  <span style={{ fontSize: '0.7rem', fontWeight: 700, color: 'var(--text-muted)' }}>
                    {MEDAL[r.rank] ?? `${r.rank}着`}
                  </span>
                )}
              </div>

              {/* 右: 馬名 + 騎手 + 指標 */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                {/* 馬名 + 枠番 */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 5, flexWrap: 'wrap' }}>
                  {r.gate_number != null && <GateBadge gate={r.gate_number} />}
                  <span style={{
                    fontSize: '0.95rem', fontWeight: 700,
                    color: r.rank === 1 ? 'var(--neon-gold)' : r.rank != null && r.rank <= 3 ? 'var(--text-primary)' : 'var(--text-muted)',
                  }}>{r.horse_name}</span>
                </div>

                {/* 騎手 + 性齢 */}
                <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                  {r.jockey && <span>{r.jockey}</span>}
                  {r.sex_age && <span style={{ marginLeft: 6 }}>{r.sex_age}</span>}
                  {r.weight_carried && <span style={{ marginLeft: 6 }}>{r.weight_carried}kg</span>}
                </div>

                {/* タイム + 着差 + オッズ + 人気 + 体重 */}
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '3px 12px', alignItems: 'center' }}>
                  {r.finish_time && (
                    <span style={{
                      fontFamily: 'monospace', fontWeight: 700, fontSize: '0.82rem',
                      color: r.rank === 1 ? 'var(--neon-cyan)' : 'var(--text-primary)',
                      textShadow: 'none',
                    }}>{r.finish_time}</span>
                  )}
                  {(r.margin || r.rank === 1) && (
                    <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>
                      {r.margin || (r.rank === 1 ? '◎' : '—')}
                    </span>
                  )}
                  {r.win_odds != null && (
                    <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                      単勝 <OddsCell odds={r.win_odds} />
                    </span>
                  )}
                  {r.popularity != null && (
                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 3, fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                      <PopBadge pop={r.popularity} />
                    </span>
                  )}
                  {r.horse_weight != null && (
                    <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)', fontFamily: 'monospace' }}>
                      {r.horse_weight}
                      {r.horse_weight_diff != null && (
                        <span style={{
                          marginLeft: 2, fontSize: '0.65rem',
                          color: r.horse_weight_diff > 0 ? 'var(--neon-red)' : r.horse_weight_diff < 0 ? 'var(--neon-cyan)' : 'var(--text-muted)',
                        }}>({r.horse_weight_diff > 0 ? '+' : ''}{r.horse_weight_diff})</span>
                      )}
                    </span>
                  )}
                  {r.training_eval && <EvalBadge eval={r.training_eval} />}
                </div>
              </div>
            </div>
          )
        })}
        </div>
      </div>

      {/* ── 払戻金セクション（結果と同じカード内に統合） ── */}
      {payoutBetTypes.length > 0 && (
        <div className="border-t border-[var(--border)]">
          <div className="px-4 py-3 border-b border-[var(--border)]">
            <span className="text-sm neon-text tracking-[0.2em] font-semibold">
              PAYOUTS — 払戻金
            </span>
          </div>
          <div className="p-4 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
            {payoutBetTypes.map(betType => (
              <PayoutCard key={betType} betType={betType} payouts={payoutsByType[betType]} />
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ── 払戻カード ─────────────────────────────────────────────
function PayoutCard({ betType, payouts }: { betType: string; payouts: RacePayout[] }) {
  const isBig = payouts.some(p => p.payout >= 10000)
  return (
    <div
      className="rounded-lg p-3 space-y-1.5"
      style={{
        background: isBig ? 'rgba(184,134,11,0.07)' : 'rgba(200,168,130,0.04)',
        border: `1px solid ${isBig ? 'rgba(184,134,11,0.25)' : 'rgba(200,168,130,0.15)'}`,
      }}
    >
      <div className="text-xs font-bold tracking-widest mb-2" style={{
        color: isBig ? 'var(--neon-gold)' : 'var(--neon-cyan)',
      }}>
        {betType}
      </div>
      {payouts.map((p, i) => (
        <div key={i} className="flex items-center justify-between gap-2">
          <span className="font-mono text-sm text-[var(--text-primary)]">{p.combination}</span>
          <span className={`font-bold font-mono text-sm ${
            p.payout >= 100000 ? 'neon-text-gold' :
            p.payout >= 10000  ? 'text-[var(--neon-gold)]' :
            p.payout >= 3000   ? 'neon-text-green' :
            'text-[var(--text-primary)]'
          }`}>
            ¥{p.payout.toLocaleString()}
          </span>
          {p.popularity != null && (
            <span className="text-[10px] text-[var(--text-muted)]">{p.popularity}番人気</span>
          )}
        </div>
      ))}
    </div>
  )
}

// ── 属性バッジ ─────────────────────────────────────────────
function AttrBadge({ label, value, highlight = false }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div
      className="flex items-center gap-1.5 px-2.5 py-1 rounded text-xs"
      style={{ background: 'rgba(200,168,130,0.05)', border: '1px solid rgba(200,168,130,0.18)' }}
    >
      <span className="text-[var(--text-muted)]">{label}</span>
      <span className={`font-semibold ${highlight ? 'neon-text-green' : 'text-[var(--text-primary)]'}`}>
        {value}
      </span>
    </div>
  )
}

// ── 枠番バッジ ─────────────────────────────────────────────
const GATE_BG: Record<number, string>   = { 1:'#fff', 2:'#000', 3:'#e00', 4:'#06c', 5:'#fa0', 6:'#080', 7:'#e88', 8:'#888' }
const GATE_TEXT: Record<number, string> = { 1:'#333', 2:'#fff', 3:'#fff', 4:'#fff', 5:'#333', 6:'#fff', 7:'#333', 8:'#fff' }

function GateBadge({ gate }: { gate: number }) {
  return (
    <span
      className="inline-flex items-center justify-center w-5 h-5 rounded text-xs font-bold"
      style={{ background: GATE_BG[gate] ?? '#555', color: GATE_TEXT[gate] ?? '#fff', border: '1px solid rgba(255,255,255,0.2)', fontSize: '0.7rem' }}
    >
      {gate}
    </span>
  )
}

// ── オッズ表示 ─────────────────────────────────────────────
function OddsCell({ odds }: { odds: number | null }) {
  if (odds == null) return <span className="text-[var(--text-muted)]">—</span>
  const color =
    odds <= 3  ? 'var(--neon-green)' :
    odds <= 10 ? 'var(--neon-cyan)'  :
    odds <= 30 ? 'var(--neon-gold)'  : 'var(--neon-red)'
  return <span style={{ color }}>{odds.toFixed(1)}</span>
}

// ── 人気バッジ ─────────────────────────────────────────────
function PopBadge({ pop }: { pop: number | null }) {
  if (pop == null) return <span className="text-[var(--text-muted)]">—</span>
  const bg  = pop === 1 ? 'rgba(91,138,91,0.2)' : pop <= 3 ? 'rgba(200,168,130,0.12)' : 'transparent'
  const col = pop === 1 ? 'var(--neon-green)' : pop <= 3 ? 'var(--neon-cyan)' : 'var(--text-muted)'
  return (
    <span className="inline-flex items-center justify-center w-6 h-6 rounded font-bold"
      style={{ background: bg, color: col, border: `1px solid ${col}44`, fontSize: '0.8rem' }}>
      {pop}
    </span>
  )
}

// ── 調教評価バッジ ─────────────────────────────────────────
function EvalBadge({ eval: e }: { eval: TrainingEval }) {
  const grade = e.eval_grade as 'A' | 'B' | 'C' | 'D'
  return (
    <span
      className={`eval-badge eval-badge-${grade}`}
      title={e.eval_text || ''}
    >
      {grade}
    </span>
  )
}

// ── 本命スコア色 ───────────────────────────────────────────
function scoreColor(score: number): string {
  if (score >= 0.30) return 'var(--neon-red)'
  if (score >= 0.15) return 'var(--neon-gold)'
  if (score >= 0.08) return 'var(--neon-cyan)'
  return 'var(--text-muted)'
}
