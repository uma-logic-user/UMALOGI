'use client'

import type { Prediction, RacePayout } from '../types/race'

interface Props {
  predictions: Prediction[]
  raceId?:     string
  modelType?:  string
  limit?:      number
  /** レース払戻データ（的中コンボ特定に使用）*/
  payouts?:    RacePayout[]
}

// ── 既存ヘルパー ─────────────────────────────────────────────────────────

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

function formatBet(nTickets: number, perTicket = 100): string {
  if (nTickets <= 0) return '—'
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

// ── 新規ヘルパー ─────────────────────────────────────────────────────────

function parseCombos(json: string | null | undefined): number[][] {
  if (!json) return []
  try {
    const raw = JSON.parse(json)
    if (!Array.isArray(raw) || raw.length === 0) return []
    return Array.isArray(raw[0]) ? (raw as number[][]) : [(raw as number[])]
  } catch { return [] }
}

/** 全コンボに必ず含まれる馬（軸馬）を昇順で返す */
function inferAxis(combos: number[][]): number[] {
  if (combos.length < 2) return []
  const all = new Set(combos.flatMap(c => c))
  return [...all].filter(h => combos.every(c => c.includes(h))).sort((a, b) => a - b)
}

/** 払戻データと突合して的中した具体的コンボを返す */
function findHitCombo(
  combos:   number[][],
  betType:  string,
  payouts:  RacePayout[],
): number[] | null {
  const isOrdered = betType === '馬単' || betType === '三連単'
  for (const payout of payouts) {
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

// ── サブコンポーネント ────────────────────────────────────────────────────

function AxisBadge() {
  return (
    <span style={{
      display:       'inline-block',
      background:    'rgba(255,215,0,0.18)',
      color:         '#FFD700',
      border:        '1px solid rgba(255,215,0,0.55)',
      borderRadius:  3,
      padding:       '0 4px',
      fontSize:      '0.6rem',
      fontWeight:    800,
      letterSpacing: '0.08em',
      lineHeight:    '1.7',
    }}>軸</span>
  )
}

/** 着順別カラー: 1着=金・2着=銀・3着=銅 */
const MEDAL_BG  = ['#FFD700', '#C0C0C0', '#CD7F32'] as const
const MEDAL_CLR = ['#000000', '#000000', '#ffffff'] as const

function HorseCircle({
  num,
  pos,
  isOrdered,
}: {
  num:       number
  pos:       number
  isOrdered: boolean
}) {
  const medal = isOrdered && pos >= 0 && pos < 3
  const bg    = medal ? MEDAL_BG[pos]  : 'rgba(255,215,0,0.12)'
  const clr   = medal ? MEDAL_CLR[pos] : '#FFD700'
  return (
    <span
      title={medal ? `${pos + 1}着` : undefined}
      style={{
        display:        'inline-flex',
        alignItems:     'center',
        justifyContent: 'center',
        width:          24,
        height:         24,
        borderRadius:   '50%',
        background:     bg,
        color:          clr,
        fontWeight:     'bold',
        fontSize:       '0.8rem',
        fontFamily:     'monospace',
        border:         medal ? 'none' : '1px solid rgba(255,215,0,0.4)',
        flexShrink:     0,
      }}
    >{num}</span>
  )
}

/**
 * 的中コンボ + 軸馬 を表示するセルコンポーネント。
 *
 * - 的中時: 金銀銅サークル付きの的中コンボを最上部に表示
 * - 軸ありマルチ: 「軸」バッジ + 軸馬番（ゴールド） + 相手馬リスト
 * - コンボ数 > 12: 全コンボ列挙をやめ "相手 N頭" のサマリーに圧縮
 */
function ComboCell({
  pred,
  combos,
  axisList,
  hitCombo,
}: {
  pred:     Prediction
  combos:   number[][]
  axisList: number[]
  hitCombo: number[] | null
}) {
  const isOrdered = pred.bet_type === '馬単' || pred.bet_type === '三連単'
  const sep       = isOrdered ? '→' : '-'
  const nameMap   = pred.horse_num_to_name ?? {}
  const name      = (n: number) => nameMap[String(n)] ?? null
  const axisSet   = new Set(axisList)
  const isHit     = pred.is_hit === 1
  const large     = combos.length > 12
  const allNums   = [...new Set(combos.flatMap(c => c))].sort((a, b) => a - b)
  const opponents = allNums.filter(h => !axisSet.has(h))

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>

      {/* ══ 的中コンボ（金銀銅サークル）══════════════════ */}
      {isHit && hitCombo && (
        <div style={{
          background:   'rgba(255,215,0,0.10)',
          border:       '1px solid rgba(255,215,0,0.45)',
          borderRadius: 6,
          padding:      '5px 8px',
        }}>
          {/* 的中バッジ */}
          <div style={{ marginBottom: 4 }}>
            <span style={{
              display:       'inline-block',
              background:    '#FFD700',
              color:         '#000',
              borderRadius:  3,
              padding:       '0 5px',
              fontSize:      '0.6rem',
              fontWeight:    900,
              letterSpacing: '0.05em',
            }}>🏆 的中</span>
          </div>

          {/* 馬番サークル行 */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 3, flexWrap: 'wrap' }}>
            {hitCombo.map((num, i) => (
              <span key={i} style={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                <HorseCircle num={num} pos={i} isOrdered={isOrdered} />
                {i < hitCombo.length - 1 && (
                  <span style={{
                    color:      'rgba(255,215,0,0.45)',
                    fontSize:   '0.65rem',
                    fontFamily: 'monospace',
                  }}>{sep}</span>
                )}
              </span>
            ))}
          </div>

          {/* 馬名（存在する場合） */}
          {hitCombo.some(n => name(n)) && (
            <div style={{
              marginTop:  2,
              color:      'rgba(255,215,0,0.55)',
              fontSize:   '0.6rem',
              fontFamily: 'monospace',
            }}>
              {hitCombo.map(n => name(n) ?? String(n)).join(sep)}
            </div>
          )}
        </div>
      )}

      {/* 的中バッジのみ（payouts未渡しで的中フラグだけある場合） */}
      {isHit && !hitCombo && (
        <span style={{
          display:    'inline-block',
          background: 'rgba(255,215,0,0.15)',
          color:      '#FFD700',
          border:     '1px solid rgba(255,215,0,0.4)',
          borderRadius: 4,
          padding:    '1px 6px',
          fontSize:   '0.65rem',
          fontWeight: 700,
        }}>🏆 的中</span>
      )}

      {/* ══ 軸馬バッジ + コンボ情報 ═════════════════════ */}
      {axisList.length > 0 ? (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          {/* 軸馬 */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap' }}>
            {axisList.map(ax => (
              <span key={ax} style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
                <AxisBadge />
                <span
                  title={name(ax) ?? undefined}
                  style={{
                    fontFamily:  'monospace',
                    fontWeight:  'bold',
                    fontSize:    '0.9rem',
                    color:       '#FFD700',
                    textShadow:  '0 0 6px rgba(255,215,0,0.5)',
                  }}
                >{ax}番</span>
                {name(ax) && (
                  <span style={{ color: 'rgba(255,215,0,0.55)', fontSize: '0.65rem' }}>
                    {name(ax)}
                  </span>
                )}
              </span>
            ))}
          </div>
          {/* 相手馬（大量コンボはサマリー） */}
          {large ? (
            <span style={{
              color:      'var(--text-muted)',
              fontSize:   '0.65rem',
              fontFamily: 'monospace',
            }}>
              相手 {opponents.join('·')}
            </span>
          ) : (
            <div style={{
              color:      'var(--text-primary)',
              fontSize:   '0.7rem',
              fontFamily: 'monospace',
            }}>
              {combos.map(c => c.join(sep)).join(' / ')}
            </div>
          )}
        </div>
      ) : (
        /* 軸なし: 全コンボまたは点数のみ */
        <div style={{
          color:      'var(--text-primary)',
          fontSize:   '0.7rem',
          fontFamily: 'monospace',
          lineHeight: 1.4,
        }}>
          {large
            ? `${combos.length}点`
            : combos.map(c => c.join(sep)).join(' / ')}
        </div>
      )}
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

  // 的中を先頭へ（ROI降順）、以降は元の順序
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
                ? findHitCombo(combos, pred.bet_type, payouts)
                : null

              return (
                <tr key={pred.prediction_id} className={hitClass(pred)}>
                  <td className="text-[var(--text-muted)] font-mono">{pred.date}</td>
                  <td className="max-w-[160px] truncate" title={pred.race_name}>
                    <span className="text-[var(--text-muted)]">{pred.venue}</span>{' '}
                    <span className="font-semibold">{pred.race_name}</span>
                  </td>
                  <td>
                    <span className={`font-bold ${pred.model_type === '卍' ? 'neon-text' : 'neon-text-gold'}`}>
                      {pred.model_type}
                    </span>
                  </td>
                  <td>
                    <div>{pred.bet_form ?? pred.bet_type}</div>
                    {pred.n_tickets > 0 && (
                      <div className="text-xs opacity-60">{pred.n_tickets}点</div>
                    )}
                  </td>
                  <td className="text-xs min-w-[160px] max-w-[300px]">
                    <ComboCell
                      pred={pred}
                      combos={combos}
                      axisList={axisList}
                      hitCombo={hitCombo}
                    />
                  </td>
                  <td className="text-right text-[var(--text-muted)] font-mono text-xs">
                    {formatBet(pred.n_tickets)}
                  </td>
                  <td className="text-center">
                    <span className={hitLabelClass(pred)}>{hitLabel(pred)}</span>
                  </td>
                  <td className="text-right">
                    {pred.is_hit === 1 ? (
                      <span className={payoutBadgeClass(pred)}>
                        {formatPayout(pred.payout)}
                      </span>
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
        {payouts.length > 0 && (
          <span className="ml-2 opacity-50">· 的中コンボ照合済</span>
        )}
      </div>
    </div>
  )
}
