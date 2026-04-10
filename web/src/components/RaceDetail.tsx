'use client'

import { useState } from 'react'
import type { RaceEntry, Prediction, RacePayout, RaceResult, TrainingEval, RaceBias, EvRecommend } from '@/types/race'
import PredictionsPanel from './PredictionsPanel'
import BiasPanel from './BiasPanel'

type Tab = 'results' | 'prerace' | 'predictions'

const MEDAL: Record<number, string> = { 1: '🥇', 2: '🥈', 3: '🥉' }

const BET_ORDER: Record<string, number> = {
  '単勝': 1, '複勝': 2, '枠連': 3, '馬連': 4,
  'ワイド': 5, '馬単': 6, '三連複': 7, '三連単': 8,
}

// odds_velocity がこの値以上で🔥シグナル
const VELOCITY_THRESHOLD = 0.05

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
  const defaultTab: Tab = hasPrerace ? 'prerace' : 'results'
  const [tab, setTab] = useState<Tab>(defaultTab)

  const payoutsByType = (race.payouts ?? []).reduce<Record<string, RacePayout[]>>(
    (acc, p) => { (acc[p.bet_type] ??= []).push(p); return acc },
    {},
  )
  const betTypes = Object.keys(payoutsByType).sort(
    (a, b) => (BET_ORDER[a] ?? 99) - (BET_ORDER[b] ?? 99),
  )

  const hasPayouts     = betTypes.length > 0
  const hasPredictions = predictions.length > 0

  const tabs: { key: Tab; label: string; count?: number }[] = [
    ...(hasPrerace ? [{ key: 'prerace' as Tab, label: 'AI直前分析' }] : []),
    { key: 'results',     label: 'レース結果' },
    { key: 'predictions', label: 'AI予想', count: hasPredictions ? predictions.length : undefined },
  ]

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
              {race.race_name}
            </h1>
          </div>
          <div className="text-right shrink-0">
            <div className="text-[10px] text-[var(--text-muted)] mb-1 tracking-wider">RACE ID</div>
            <div className="font-mono text-xs text-[var(--neon-cyan)] opacity-60">{race.race_id}</div>
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
          <div className="mt-4 pt-3 border-t border-[rgba(0,200,255,0.12)]">
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
      <div className="flex gap-0 border-b border-[rgba(0,200,255,0.18)]">
        {tabs.map(t => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={`relative px-6 py-2.5 text-sm font-semibold tracking-wider transition-colors ${
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
                style={{ boxShadow: '0 0 8px rgba(0,200,255,0.9)' }}
              />
            )}
          </button>
        ))}
      </div>

      {/* ── AI直前分析タブ ─────────────────────────── */}
      {tab === 'prerace' && hasPrerace && (
        <div className="space-y-4">
          {/* バイアスパネル */}
          <BiasPanel
            bias={race.prerace!.bias}
            condition={race.condition || ''}
          />

          {/* AI直前予想テーブル */}
          <PreraceTable results={race.results ?? []} />
        </div>
      )}

      {/* ── レース結果タブ ─────────────────────────── */}
      {tab === 'results' && (
        <div className="space-y-4">
          <ResultsTable results={race.results ?? []} />
          {hasPayouts && (
            <div className="neon-card overflow-hidden">
              <div className="px-4 py-3 border-b border-[rgba(0,200,255,0.12)]">
                <span className="text-sm neon-text tracking-[0.2em] font-semibold">
                  PAYOUTS — 払戻金
                </span>
              </div>
              <div className="p-4 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
                {betTypes.map(betType => (
                  <PayoutCard
                    key={betType}
                    betType={betType}
                    payouts={payoutsByType[betType]}
                  />
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── AI予想タブ ────────────────────────────── */}
      {tab === 'predictions' && (
        hasPredictions
          ? <PredictionsPanel predictions={predictions} limit={200} />
          : (
            <div className="neon-card p-12 text-center">
              <div className="text-[var(--text-muted)] text-base tracking-widest">
                このレースの予想データはありません
              </div>
            </div>
          )
      )}
    </div>
  )
}

// ── AI直前分析テーブル ────────────────────────────────────
function PreraceTable({ results }: { results: RaceResult[] }) {
  const sorted = [...results].sort((a, b) => (a.horse_number ?? 99) - (b.horse_number ?? 99))

  return (
    <div className="neon-card overflow-hidden">
      <div className="px-4 py-3 border-b border-[rgba(0,200,255,0.12)]">
        <span className="text-sm neon-text tracking-[0.2em] font-semibold">
          PRE-RACE ANALYSIS — 直前情報
        </span>
      </div>
      <div className="table-scroll">
        <table className="w-full race-table">
          <thead>
            <tr>
              <th className="text-center">馬番</th>
              <th className="text-left">馬名</th>
              <th className="text-right">単勝</th>
              <th className="text-center">調教</th>
              <th className="text-right">本命スコア</th>
              <th className="text-right">EV</th>
              <th className="text-right">Kelly推奨</th>
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
                  {/* 馬番 */}
                  <td className="text-center">
                    {r.gate_number != null ? <GateBadge gate={r.gate_number} /> : null}
                    <span className="ml-1 font-mono text-[var(--text-muted)]">
                      {r.horse_number}
                    </span>
                  </td>

                  {/* 馬名 + 激アツバッジ */}
                  <td>
                    <div className="flex items-center gap-2">
                      <span className={`font-semibold ${isHot ? 'neon-text-red' : 'text-[var(--text-primary)]'}`}>
                        {r.horse_name}
                      </span>
                      {isHot && (
                        <span className="badge-hot" style={{ fontSize: '0.65rem', padding: '1px 5px' }}>
                          激アツ
                        </span>
                      )}
                    </div>
                    <div className="text-[10px] text-[var(--text-muted)] mt-0.5">
                      {r.sex_age} {r.weight_carried}kg
                    </div>
                  </td>

                  {/* 単勝オッズ */}
                  <td className="text-right font-mono">
                    <OddsCell odds={r.win_odds} />
                  </td>

                  {/* 調教評価バッジ */}
                  <td className="text-center">
                    {r.training_eval
                      ? <EvalBadge eval={r.training_eval} />
                      : <span className="text-[var(--text-muted)]">—</span>
                    }
                  </td>

                  {/* 本命スコア */}
                  <td className="text-right font-mono text-xs">
                    {r.honmei_score != null
                      ? <span style={{ color: scoreColor(r.honmei_score ?? 0) }}>
                          {((r.honmei_score ?? 0) * 100).toFixed(1)}%
                        </span>
                      : <span className="text-[var(--text-muted)]">—</span>
                    }
                  </td>

                  {/* EV */}
                  <td className="text-right font-mono">
                    {r.ev_score != null ? (
                      <span
                        className={ev >= 1.0 ? 'neon-text-red font-bold' : ev >= 0.8 ? 'neon-text-gold' : 'text-[var(--text-muted)]'}
                      >
                        {ev.toFixed(2)}
                      </span>
                    ) : <span className="text-[var(--text-muted)]">—</span>}
                  </td>

                  {/* Kelly推奨額（資金100万想定） */}
                  <td className="text-right font-mono text-xs">
                    {kelly > 0
                      ? <span className="neon-text-green">
                          ¥{Math.round(kelly * 100000).toLocaleString()}
                        </span>
                      : <span className="text-[var(--text-muted)]">—</span>
                    }
                  </td>

                  {/* オッズ朝一比 */}
                  <td className="text-right font-mono text-xs">
                    {r.odds_vs_morning != null ? (
                      <span style={{
                        color: r.odds_vs_morning < 0.85
                          ? 'var(--neon-red)'
                          : r.odds_vs_morning < 0.95
                          ? 'var(--neon-gold)'
                          : 'var(--text-muted)',
                      }}>
                        ×{r.odds_vs_morning.toFixed(2)}
                      </span>
                    ) : <span className="text-[var(--text-muted)]">—</span>}
                  </td>

                  {/* 大口シグナル */}
                  <td className="text-center">
                    {isFire ? (
                      <span className="signal-fire" title={`下落速度 ${velocity.toFixed(3)}/分`}>
                        🔥
                      </span>
                    ) : (
                      <span className="text-[var(--text-muted)]">·</span>
                    )}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      <div className="px-4 py-2 text-[10px] text-[var(--text-muted)] border-t border-[var(--border)]">
        Kelly推奨 = 100万円資金想定 / EV≥1.0 = 激アツ推奨馬 / 🔥 = 急激なオッズ下落（大口投票シグナル）
      </div>
    </div>
  )
}

// ── レース結果テーブル（変更なし）────────────────────────────
function ResultsTable({ results }: { results: RaceResult[] }) {
  return (
    <div className="neon-card overflow-hidden">
      <div className="px-4 py-3 border-b border-[rgba(0,200,255,0.12)]">
        <span className="text-sm neon-text tracking-[0.2em] font-semibold">
          RACE RESULTS — {results.length} runners
        </span>
      </div>
      <div className="table-scroll">
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
                <td className="text-center font-mono text-[var(--text-muted)]">
                  {r.horse_number ?? '—'}
                </td>
                <td>
                  <span className={`font-semibold ${
                    r.rank === 1 ? 'neon-text-gold' :
                    r.rank != null && r.rank <= 3 ? 'text-[var(--text-primary)]' :
                    'text-[var(--text-muted)]'
                  }`}>
                    {r.horse_name}
                  </span>
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
                          r.horse_weight_diff < 0 ? 'text-[var(--neon-cyan)]' :
                          'text-[var(--text-muted)]'
                        }`}>
                          ({r.horse_weight_diff > 0 ? '+' : ''}{r.horse_weight_diff})
                        </span>
                      )}
                    </>
                  ) : '—'}
                </td>
                <td className="text-right font-mono">
                  <OddsCell odds={r.win_odds} />
                </td>
                <td className="text-center">
                  <PopBadge pop={r.popularity} />
                </td>
                <td className="text-center">
                  {r.training_eval
                    ? <EvalBadge eval={r.training_eval} />
                    : <span className="text-[var(--text-muted)]">—</span>
                  }
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
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
        background: isBig ? 'rgba(255,215,0,0.07)' : 'rgba(0,200,255,0.04)',
        border: `1px solid ${isBig ? 'rgba(255,215,0,0.25)' : 'rgba(0,200,255,0.15)'}`,
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
      style={{ background: 'rgba(0,200,255,0.05)', border: '1px solid rgba(0,200,255,0.18)' }}
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
  return <span style={{ color, textShadow: `0 0 6px ${color}55` }}>{odds.toFixed(1)}</span>
}

// ── 人気バッジ ─────────────────────────────────────────────
function PopBadge({ pop }: { pop: number | null }) {
  if (pop == null) return <span className="text-[var(--text-muted)]">—</span>
  const bg  = pop === 1 ? 'rgba(0,255,136,0.2)' : pop <= 3 ? 'rgba(0,200,255,0.12)' : 'transparent'
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
