'use client'

import { useState } from 'react'
import type { Prediction, RacePayout } from '../types/race'

interface Props {
  predictions: Prediction[]
  raceId?:     string
  modelType?:  string
  limit?:      number
  payouts?:    RacePayout[]
}

// ── ヘルパー関数 ─────────────────────────────────────────────────────────

function hitClass(pred: Prediction): string {
  if (pred.is_hit === null) return ''
  if (!pred.is_hit)         return 'hit-miss'
  const roi = pred.roi ?? 0
  if (roi >= 500) return 'hit-explosion'
  if (roi >= 200) return 'hit-big'
  return 'hit-normal'
}

function payoutBadgeClass(pred: Prediction): string {
  const roi = pred.roi ?? 0
  if (roi >= 500) return 'payout-badge payout-badge-explosion'
  if (roi >= 200) return 'payout-badge payout-badge-big'
  return 'payout-badge payout-badge-normal'
}

function formatPayout(payout: number | null): string {
  if (payout == null || payout === 0) return '—'
  return `¥${Math.round(payout).toLocaleString()}`
}

function formatBet(nTickets: number | null | undefined, perTicket = 100): string {
  if (!nTickets || nTickets <= 0) return '—'
  return `¥${(nTickets * perTicket).toLocaleString()}（${perTicket}円×${nTickets}点）`
}

function formatRoi(roi: number | null): string {
  if (roi == null) return '—'
  return `${roi.toFixed(1)}%`
}

function hitLabel(pred: Prediction): string {
  if (pred.is_hit === null) return '—'
  return pred.is_hit ? '◎ 的中' : '✕ 外れ'
}

function hitLabelClass(pred: Prediction): string {
  if (pred.is_hit === null) return 'text-[var(--text-muted)]'
  if (!pred.is_hit)         return 'neon-text-red font-semibold'
  const roi = pred.roi ?? 0
  if (roi >= 200) return 'neon-text-gold font-bold'
  return 'neon-text-green font-bold'
}

function parseCombos(json: string | null | undefined): number[][] {
  if (!json) return []
  try {
    const raw = JSON.parse(json)
    if (!Array.isArray(raw) || raw.length === 0) return []
    return Array.isArray(raw[0]) ? (raw as number[][]) : [(raw as number[])]
  } catch { return [] }
}

function inferAxis(combos: number[][]): number[] {
  if (combos.length < 2) return []
  const all = new Set(combos.flatMap(c => c))
  return [...all].filter(h => combos.every(c => c.includes(h))).sort((a, b) => a - b)
}

function findHitCombo(
  combos:   number[][],
  betType:  string,
  payouts:  RacePayout[],
  raceId?:  string,
): number[] | null {
  const pool = raceId ? payouts.filter(p => p.race_id === raceId) : payouts
  const isOrdered = betType === '馬単' || betType === '三連単'
  for (const payout of pool) {
    if (payout.bet_type !== betType || payout.payout <= 0) continue
    const payNums = payout.combination.replace(/→/g, '-').split('-').map(Number)
    for (const combo of combos) {
      const hit = isOrdered
        ? combo.length === payNums.length && combo.every((n, i) => n === payNums[i])
        : (() => {
            const a = [...combo].sort((x, y) => x - y)
            const b = [...payNums].sort((x, y) => x - y)
            return a.length === b.length && a.every((n, i) => n === b[i])
          })()
      if (hit) return combo
    }
  }
  return null
}

/** モデルタイプから CSS クラスサフィックスを解決する */
function modelKey(modelType: string | null | undefined): 'alpha' | 'manzai' | 'honmei' | 'other' {
  if (!modelType) return 'other'
  const t = modelType.toLowerCase()
  if (t.includes('alpha')) return 'alpha'
  if (t.includes('卍') || t.includes('manzai')) return 'manzai'
  if (t.includes('本命') || t.includes('honmei')) return 'honmei'
  return 'other'
}

/** EV 値から表示クラスを返す */
function evDisplayClass(ev: number | null | undefined): string {
  const v = ev ?? 0
  if (v >= 3.0) return 'ev-display-large ev-display-red'
  if (v >= 1.5) return 'ev-display-large ev-display-gold'
  if (v >= 0.0) return 'ev-display-large ev-display-cyan'
  return 'ev-display-large ev-display-muted'
}

// ── サブコンポーネント（共通）───────────────────────────────────────────

function AxisBadge() {
  return (
    <span style={{
      display: 'inline-block',
      background: 'rgba(255,215,0,0.18)',
      color: '#FFD700',
      border: '1px solid rgba(255,215,0,0.55)',
      borderRadius: 3,
      padding: '0 4px',
      fontSize: '0.6rem',
      fontWeight: 800,
      letterSpacing: '0.08em',
      lineHeight: '1.7',
    }}>軸</span>
  )
}

const MEDAL_BG  = ['#FFD700', '#C0C0C0', '#CD7F32'] as const
const MEDAL_CLR = ['#000000', '#000000', '#ffffff'] as const

function HorseCircle({ num, pos, isOrdered }: { num: number; pos: number; isOrdered: boolean }) {
  const medal = isOrdered && pos >= 0 && pos < 3
  const bg    = medal ? MEDAL_BG[pos]  : 'rgba(255,215,0,0.12)'
  const clr   = medal ? MEDAL_CLR[pos] : '#FFD700'
  return (
    <span
      title={medal ? `${pos + 1}着` : undefined}
      style={{
        display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
        width: 24, height: 24, borderRadius: '50%',
        background: bg, color: clr,
        fontWeight: 'bold', fontSize: '0.8rem', fontFamily: 'monospace',
        border: medal ? 'none' : '1px solid rgba(255,215,0,0.4)',
        flexShrink: 0,
      }}
    >{num}</span>
  )
}

function ComboCell({
  pred, combos, axisList, hitCombo,
}: {
  pred: Prediction; combos: number[][]; axisList: number[]; hitCombo: number[] | null
}) {
  const isOrdered = pred.bet_type === '馬単' || pred.bet_type === '三連単'
  const sep       = isOrdered ? '→' : '-'
  const nameMap   = pred.horse_num_to_name ?? {}
  const name      = (n: number) => nameMap[String(n)] ?? null
  const axisSet   = new Set(axisList)
  const isHit     = pred.is_hit === 1
  const large     = combos.length > 8
  const allNums   = [...new Set(combos.flatMap(c => c))].sort((a, b) => a - b)
  const opponents = allNums.filter(h => !axisSet.has(h))

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
      {isHit && hitCombo && (
        <div style={{
          background: 'rgba(255,215,0,0.10)', border: '1px solid rgba(255,215,0,0.45)',
          borderRadius: 6, padding: '5px 8px',
        }}>
          <div style={{ marginBottom: 4 }}>
            <span style={{
              display: 'inline-block', background: '#FFD700', color: '#000',
              borderRadius: 3, padding: '0 5px', fontSize: '0.6rem', fontWeight: 900, letterSpacing: '0.05em',
            }}>🏆 的中</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 3, flexWrap: 'wrap' }}>
            {hitCombo.map((num, i) => (
              <span key={i} style={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                <HorseCircle num={num} pos={i} isOrdered={isOrdered} />
                {i < hitCombo.length - 1 && (
                  <span style={{ color: 'rgba(255,215,0,0.45)', fontSize: '0.65rem', fontFamily: 'monospace' }}>{sep}</span>
                )}
              </span>
            ))}
          </div>
          {hitCombo.some(n => name(n)) && (
            <div style={{ marginTop: 2, color: 'rgba(255,215,0,0.55)', fontSize: '0.6rem', fontFamily: 'monospace' }}>
              {hitCombo.map(n => name(n) ?? String(n)).join(sep)}
            </div>
          )}
        </div>
      )}

      {isHit && !hitCombo && (
        <span style={{
          display: 'inline-block', background: 'rgba(255,215,0,0.15)', color: '#FFD700',
          border: '1px solid rgba(255,215,0,0.4)', borderRadius: 4, padding: '1px 6px',
          fontSize: '0.65rem', fontWeight: 700,
        }}>🏆 的中</span>
      )}

      {axisList.length > 0 ? (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap' }}>
            {axisList.map(ax => (
              <span key={ax} style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
                <AxisBadge />
                <span title={name(ax) ?? undefined} style={{
                  fontFamily: 'monospace', fontWeight: 'bold', fontSize: '0.9rem',
                  color: '#FFD700', textShadow: '0 0 6px rgba(255,215,0,0.5)',
                }}>{ax}番</span>
                {name(ax) && <span style={{ color: 'rgba(255,215,0,0.55)', fontSize: '0.65rem' }}>{name(ax)}</span>}
              </span>
            ))}
          </div>
          {large ? (
            <span style={{ color: 'var(--text-muted)', fontSize: '0.65rem', fontFamily: 'monospace' }}>
              相手 {opponents.join('·')}
            </span>
          ) : (
            <div style={{ color: 'var(--text-primary)', fontSize: '0.7rem', fontFamily: 'monospace', overflowWrap: 'break-word', wordBreak: 'break-all' }}>
              {combos.map(c => c.join(sep)).join(' / ')}
            </div>
          )}
        </div>
      ) : (
        <div style={{ color: 'var(--text-primary)', fontSize: '0.7rem', fontFamily: 'monospace', lineHeight: 1.4, overflowWrap: 'break-word', wordBreak: 'break-all' }}>
          {large ? `${combos.length}点` : combos.map(c => c.join(sep)).join(' / ')}
        </div>
      )}
    </div>
  )
}

// ── モバイル専用ベットスリップカード ──────────────────────────────────────

function MobilePredCard({
  pred, payouts, raceId,
}: { pred: Prediction; payouts: RacePayout[]; raceId?: string }) {
  const combos   = parseCombos(pred.combination_json)
  const axisList = inferAxis(combos)
  const hitCombo = pred.is_hit === 1 && payouts.length > 0
    ? findHitCombo(combos, pred.bet_type, payouts, pred.race_id ?? raceId)
    : null

  const isOrdered = pred.bet_type === '馬単' || pred.bet_type === '三連単'
  const sep       = isOrdered ? '→' : '-'
  const nameMap   = pred.horse_num_to_name ?? {}
  const name      = (n: number) => nameMap[String(n)] ?? null
  const axisSet   = new Set(axisList)
  const allNums   = [...new Set(combos.flatMap(c => c))].sort((a, b) => a - b)
  const opponents = allNums.filter(h => !axisSet.has(h))
  const isHit     = pred.is_hit === 1
  const mk        = modelKey(pred.model_type)

  const cardClass = [
    'pred-card',
    `pred-card-${mk}`,
    isHit ? 'pred-card-hit' : '',
    !pred.is_hit && pred.is_hit !== null ? 'hit-miss' : '',
  ].filter(Boolean).join(' ')

  return (
    <div className={cardClass}>
      {/* ── ヘッダー行: 券種 + EV ── */}
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 8 }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          {/* 券種バッジ */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
            <span style={{
              background: 'rgba(255,255,255,0.06)',
              border: '1px solid rgba(255,255,255,0.12)',
              borderRadius: 4, padding: '2px 8px',
              fontSize: '0.78rem', fontWeight: 700,
              color: 'var(--text-primary)', letterSpacing: '0.04em',
            }}>
              {pred.bet_form ?? pred.bet_type}
            </span>
            {pred.n_tickets > 0 && (
              <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>
                {pred.n_tickets}点
              </span>
            )}
            {pred.is_provisional && (
              <span style={{
                display: 'inline-block', background: 'rgba(255,165,0,0.15)', color: '#FFA500',
                border: '1px solid rgba(255,165,0,0.45)', borderRadius: 3,
                padding: '0 4px', fontSize: '0.6rem', fontWeight: 700, letterSpacing: '0.05em',
              }}>⏳ 暫定</span>
            )}
            {/* 本命×三連単 かつ EV≥1.5 → 条件付き許可バッジ */}
            {pred.model_type?.includes('本命') && pred.bet_type === '三連単' && (pred.expected_value ?? 0) >= 1.5 && (
              <span title="ROIフィルター: EV≥1.5 条件を満たし許可" style={{
                display: 'inline-block', background: 'rgba(0,255,180,0.13)', color: '#00FFB4',
                border: '1px solid rgba(0,255,180,0.45)', borderRadius: 3,
                padding: '0 5px', fontSize: '0.6rem', fontWeight: 700, letterSpacing: '0.04em',
              }}>⚡条件付許可</span>
            )}
          </div>
          {/* レース名（複数レース表示の場合） */}
          {pred.race_name && (
            <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>
              {pred.venue} {pred.race_name}
            </span>
          )}
        </div>

        {/* EV 大型表示 */}
        {!pred.is_provisional && pred.expected_value != null && (
          <div style={{ textAlign: 'right', flexShrink: 0 }}>
            <div style={{ fontSize: '0.55rem', color: 'var(--text-muted)', letterSpacing: '0.15em', marginBottom: 1 }}>EV</div>
            <div className={evDisplayClass(pred.expected_value)}>
              {(pred.expected_value as number).toFixed(2)}
            </div>
          </div>
        )}
      </div>

      {/* ── コンボ表示 ── */}
      {pred.is_provisional ? (
        <div style={{ color: 'rgba(255,165,0,0.5)', fontSize: '0.72rem', fontStyle: 'italic' }}>
          暫定ランキング（EV未計算 · 直前予想をお待ちください）
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          {/* 的中コンボ（金銀銅サークル） */}
          {isHit && hitCombo && (
            <div style={{
              background: 'rgba(255,215,0,0.10)', border: '1px solid rgba(255,215,0,0.45)',
              borderRadius: 6, padding: '7px 10px',
            }}>
              <div style={{ marginBottom: 5 }}>
                <span style={{
                  display: 'inline-block', background: '#FFD700', color: '#000',
                  borderRadius: 3, padding: '1px 7px', fontSize: '0.65rem', fontWeight: 900,
                }}>🏆 的中</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap' }}>
                {hitCombo.map((num, i) => (
                  <span key={i} style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
                    <span className={`horse-num-lg ${i === 0 ? 'medal-1' : i === 1 ? 'medal-2' : i === 2 ? 'medal-3' : ''}`}>
                      {num}
                    </span>
                    {i < hitCombo.length - 1 && (
                      <span style={{ color: 'rgba(255,215,0,0.5)', fontSize: '0.8rem' }}>{sep}</span>
                    )}
                  </span>
                ))}
              </div>
              {hitCombo.some(n => name(n)) && (
                <div style={{ marginTop: 4, fontSize: '0.65rem', color: 'rgba(255,215,0,0.6)', fontFamily: 'monospace' }}>
                  {hitCombo.map(n => name(n) ?? String(n)).join(sep)}
                </div>
              )}
            </div>
          )}

          {/* 軸・相手表示 */}
          {axisList.length > 0 ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                <span style={{
                  display: 'inline-block', background: 'rgba(255,215,0,0.18)', color: '#FFD700',
                  border: '1px solid rgba(255,215,0,0.55)', borderRadius: 3,
                  padding: '1px 5px', fontSize: '0.62rem', fontWeight: 800,
                }}>軸</span>
                <div style={{ display: 'flex', gap: 5, flexWrap: 'wrap' }}>
                  {axisList.map(ax => (
                    <span key={ax} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                      <span className="horse-num-lg">{ax}</span>
                      {name(ax) && (
                        <span style={{ fontSize: '0.75rem', color: 'rgba(255,215,0,0.7)' }}>{name(ax)}</span>
                      )}
                    </span>
                  ))}
                </div>
              </div>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', fontFamily: 'monospace' }}>
                相手: {opponents.join(' · ')}
                {combos.length <= 8 && (
                  <span style={{ marginLeft: 6, opacity: 0.7 }}>
                    ({combos.map(c => c.join(sep)).join(' / ')})
                  </span>
                )}
              </div>
            </div>
          ) : combos.length > 0 ? (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
              {combos.length <= 6 ? (
                combos.map((c, i) => (
                  <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
                    {c.map((num, j) => (
                      <span key={j} style={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                        <span className="horse-num-lg">{num}</span>
                        {j < c.length - 1 && (
                          <span style={{ fontSize: '0.8rem', color: 'rgba(0,200,255,0.5)' }}>{sep}</span>
                        )}
                      </span>
                    ))}
                  </div>
                ))
              ) : (
                <span style={{ fontSize: '0.72rem', color: 'var(--text-primary)', fontFamily: 'monospace' }}>
                  {allNums.join(' · ')} ({combos.length}点)
                </span>
              )}
            </div>
          ) : null}
        </div>
      )}

      {/* ── フッター: 投資額・Kelly推奨・結果 ── */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        paddingTop: 8, borderTop: '1px solid rgba(255,255,255,0.06)', gap: 8,
        flexWrap: 'wrap',
      }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          {/* Kelly推奨額（recommended_bet が設定されている場合に優先表示） */}
          {!pred.is_provisional && pred.recommended_bet != null && pred.recommended_bet > 0 && (
            <span style={{
              fontSize: '0.78rem', fontFamily: 'monospace', fontWeight: 700,
              color: 'var(--neon-green)',
            }}>
              💰 Kelly推奨 ¥{Math.round(pred.recommended_bet).toLocaleString()}
            </span>
          )}
          <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)', fontFamily: 'monospace' }}>
            {pred.is_provisional ? '—' : formatBet(pred.n_tickets)}
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
          <span className={hitLabelClass(pred)} style={{ fontSize: '0.82rem' }}>
            {hitLabel(pred)}
          </span>
          {pred.is_hit === 1 && (
            <span className={payoutBadgeClass(pred)} style={{ fontSize: '0.8rem' }}>
              {formatPayout(pred.payout)}
            </span>
          )}
          {pred.is_hit === 1 && pred.roi != null && (
            <span className={`font-mono font-bold ${(pred.roi ?? 0) >= 200 ? 'neon-text-gold' : 'neon-text-green'}`}
              style={{ fontSize: '0.78rem' }}>
              ROI {formatRoi(pred.roi)}
            </span>
          )}
        </div>
      </div>
    </div>
  )
}

// ── モバイルモデルタブ ────────────────────────────────────────────────────

const MODEL_LABELS: Record<string, string> = {
  alpha: 'ALPHA', manzai: '卍', honmei: '本命', other: 'その他',
}

function MobilePredictionsView({
  items, payouts, raceId,
}: { items: Prediction[]; payouts: RacePayout[]; raceId?: string }) {
  // モデルタイプでグループ化
  const groups = items.reduce<Record<string, Prediction[]>>((acc, pred) => {
    const mk = modelKey(pred.model_type)
    ;(acc[mk] ??= []).push(pred)
    return acc
  }, {})

  const orderedKeys = (['alpha', 'manzai', 'honmei', 'other'] as const)
    .filter(k => groups[k]?.length)

  const [activeKey, setActiveKey] = useState<string>(orderedKeys[0] ?? 'alpha')
  const displayKey = orderedKeys.includes(activeKey as typeof orderedKeys[number]) ? activeKey : orderedKeys[0]
  const displayItems = groups[displayKey] ?? []

  if (orderedKeys.length === 0) {
    return (
      <div className="neon-card p-8 text-center text-[var(--text-muted)]">
        予想データなし
      </div>
    )
  }

  return (
    <div className="neon-card overflow-hidden">
      {/* モデルタブ */}
      <div className="model-tabs">
        {orderedKeys.map(k => (
          <button
            key={k}
            onClick={() => setActiveKey(k)}
            className={`model-tab-btn ${displayKey === k ? `tab-active-${k}` : ''}`}
          >
            <span>{MODEL_LABELS[k]}</span>
            <span className="model-tab-count">{groups[k]?.length}</span>
          </button>
        ))}
      </div>

      {/* カードリスト */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8, padding: '10px 10px' }}>
        {displayItems.map(pred => (
          <MobilePredCard
            key={pred.prediction_id}
            pred={pred}
            payouts={payouts}
            raceId={raceId}
          />
        ))}
      </div>

      <div style={{
        padding: '8px 12px', fontSize: '0.72rem', color: 'var(--text-muted)',
        borderTop: '1px solid var(--border)',
      }}>
        {MODEL_LABELS[displayKey]} — {displayItems.length} 件
        {payouts.length > 0 && <span style={{ marginLeft: 8, opacity: 0.5 }}>· 的中コンボ照合済</span>}
      </div>
    </div>
  )
}

// ── メインコンポーネント ──────────────────────────────────────────────────

export default function PredictionsPanel({
  predictions,
  raceId,
  modelType,
  limit = 50,
  payouts = [],
}: Props) {
  let filtered = predictions
  if (raceId)    filtered = filtered.filter(p => p.race_id === raceId)
  if (modelType) filtered = filtered.filter(p => p.model_type === modelType)

  const sorted = [...filtered].sort((a, b) => {
    const aH = a.is_hit === 1 ? 1 : 0
    const bH = b.is_hit === 1 ? 1 : 0
    if (aH !== bH) return bH - aH
    if (aH)        return (b.roi ?? 0) - (a.roi ?? 0)
    return 0
  })

  const items = sorted.slice(0, limit)

  if (items.length === 0) {
    return (
      <div className="neon-card p-6 text-center text-[var(--text-muted)] text-base">
        予想データなし
      </div>
    )
  }

  return (
    <>
      {/* ── AIウマスギフィルターバナー ────────────────────────────── */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8,
        padding: '6px 12px', marginBottom: 8, borderRadius: 6,
        background: 'rgba(0,255,180,0.07)', border: '1px solid rgba(0,255,180,0.25)',
        fontSize: '0.72rem', color: '#00FFB4',
      }}>
        <span style={{ fontWeight: 700, letterSpacing: '0.05em' }}>🤖 AIウマスギフィルター適用済み</span>
        <span style={{ opacity: 0.65 }}>
          — 本命: 単勝/複勝 + 三連単(EV≥1.5)  ·  卍: 三連単除外  ·  Alpha: 三連単除外
        </span>
      </div>

      {/* ── デスクトップ: テーブル表示 ─────────────────────────────── */}
      <div className="hidden md:block">
        <div className="neon-card overflow-hidden">
          <div className="table-scroll">
            <table className="race-table w-full">
              <thead>
                <tr>
                  <th className="text-left">日付</th>
                  <th className="text-left">レース</th>
                  <th className="text-left">モデル</th>
                  <th className="text-left">券種</th>
                  <th className="text-left">予想馬（軸・的中）</th>
                  <th className="text-right">投資金額</th>
                  <th className="text-center">結果</th>
                  <th className="text-right">払戻</th>
                  <th className="text-right">ROI</th>
                </tr>
              </thead>
              <tbody>
                {items.map(pred => {
                  const combos   = parseCombos(pred.combination_json)
                  const axisList = inferAxis(combos)
                  const hitCombo = pred.is_hit === 1 && payouts.length > 0
                    ? findHitCombo(combos, pred.bet_type, payouts, pred.race_id)
                    : null

                  return (
                    <tr key={pred.prediction_id} className={hitClass(pred)}>
                      <td className="text-[var(--text-muted)] font-mono">{pred.date}</td>
                      <td className="max-w-[160px] truncate" title={pred.race_name}>
                        <span className="text-[var(--text-muted)]">{pred.venue}</span>{' '}
                        <span className="font-semibold">{pred.race_name}</span>
                      </td>
                      <td>
                        <span className={`font-bold ${pred.model_type?.includes('卍') ? 'neon-text' : 'neon-text-gold'}`}>
                          {pred.model_type}
                        </span>
                        {pred.is_provisional && (
                          <div style={{
                            display: 'inline-block', marginLeft: 4,
                            background: 'rgba(255,165,0,0.15)', color: '#FFA500',
                            border: '1px solid rgba(255,165,0,0.45)', borderRadius: 3,
                            padding: '0 4px', fontSize: '0.55rem', fontWeight: 700,
                            letterSpacing: '0.05em', lineHeight: '1.8',
                          }}>⏳ 暫定</div>
                        )}
                      </td>
                      <td>
                        {pred.is_provisional ? (
                          <div style={{ color: 'rgba(255,165,0,0.7)', fontSize: '0.7rem', fontStyle: 'italic' }}>
                            ※オッズ未確定<br />
                            <span style={{ opacity: 0.6 }}>直前予想をお待ちください</span>
                          </div>
                        ) : (
                          <>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 5, flexWrap: 'wrap' }}>
                              <span>{pred.bet_form ?? pred.bet_type}</span>
                              {pred.model_type?.includes('本命') && pred.bet_type === '三連単' && (pred.expected_value ?? 0) >= 1.5 && (
                                <span title="ROIフィルター: EV≥1.5 条件付き許可" style={{
                                  background: 'rgba(0,255,180,0.13)', color: '#00FFB4',
                                  border: '1px solid rgba(0,255,180,0.45)', borderRadius: 3,
                                  padding: '0 4px', fontSize: '0.55rem', fontWeight: 700,
                                }}>⚡条件付</span>
                              )}
                            </div>
                            {pred.n_tickets > 0 && <div className="text-xs opacity-60">{pred.n_tickets}点</div>}
                          </>
                        )}
                      </td>
                      <td className="text-xs min-w-[120px] max-w-[260px] overflow-hidden">
                        {pred.is_provisional ? (
                          <span style={{ color: 'rgba(255,165,0,0.5)', fontSize: '0.7rem' }}>
                            暫定ランキング（EV未計算）
                          </span>
                        ) : (
                          <ComboCell pred={pred} combos={combos} axisList={axisList} hitCombo={hitCombo} />
                        )}
                      </td>
                      <td className="text-right text-[var(--text-muted)] font-mono text-xs">
                        {pred.is_provisional ? <span style={{ opacity: 0.4 }}>—</span> : formatBet(pred.n_tickets)}
                      </td>
                      <td className="text-center">
                        <span className={hitLabelClass(pred)}>{hitLabel(pred)}</span>
                      </td>
                      <td className="text-right">
                        {pred.is_hit === 1 ? (
                          <span className={payoutBadgeClass(pred)}>{formatPayout(pred.payout)}</span>
                        ) : (
                          <span className="text-[var(--text-muted)]">—</span>
                        )}
                      </td>
                      <td className="text-right font-mono">
                        {pred.is_hit === 1 ? (
                          <span className={`${(pred.roi ?? 0) >= 200 ? 'neon-text-gold font-bold' : 'neon-text-green'}`}>
                            {formatRoi(pred.roi)}
                          </span>
                        ) : (
                          <span className="text-[var(--text-muted)]">—</span>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
          <div className="px-4 py-2.5 text-sm text-[var(--text-muted)] border-t border-[var(--border)]">
            表示 {items.length} / {filtered.length} 件
            {payouts.length > 0 && <span className="ml-2 opacity-50">· 的中コンボ照合済</span>}
          </div>
        </div>
      </div>

      {/* ── モバイル: タブ＋カード表示 ────────────────────────────── */}
      <div className="block md:hidden">
        <MobilePredictionsView items={items} payouts={payouts} raceId={raceId} />
      </div>
    </>
  )
}
