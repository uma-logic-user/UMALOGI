'use client'

import { useState, useMemo, Fragment } from 'react'

/* ── 型定義 ─────────────────────────────────────────────────────── */

interface RaceHit {
  race_id:     string
  race_name:   string
  venue:       string
  race_number: number
  invested:    number
  payout:      number
  is_hit:      number
}

interface BetTypeStats {
  bet_type:   string
  invested:   number
  payout:     number
  profit:     number
  roi:        number
  total_bets: number
  hits:       number
  races?:     RaceHit[]   // daily のみ存在
}

interface DailyStats {
  date:              string
  invested:          number
  payout:            number
  profit:            number
  roi:               number
  total_bets:        number
  hits:              number
  cumulative_profit: number
  by_bet_type:       BetTypeStats[]
}

interface PeriodStats {
  period:            string   // "2026-04" or "2026"
  label:             string   // "2026年4月" or "2026年"
  invested:          number
  payout:            number
  profit:            number
  roi:               number
  total_bets:        number
  hits:              number
  cumulative_profit: number
  by_bet_type:       BetTypeStats[]
}

interface FinancialModelData {
  daily:   DailyStats[]
  monthly: PeriodStats[]
  yearly:  PeriodStats[]
}

interface FinancialData {
  [key: string]: FinancialModelData
}

interface Props {
  data:           FinancialData
  onSelectRace?: (raceId: string) => void
}

type Granularity = 'daily' | 'monthly' | 'yearly'

/* ── ユーティリティ ─────────────────────────────────────────────── */

function modelColor(name: string): 'cyan' | 'gold' {
  return name.startsWith('卍') ? 'cyan' : 'gold'
}
function modelIcon(name: string): string {
  return name.startsWith('卍') ? '⚡' : '🎯'
}
function fmtNum(n: number): string { return Math.round(n).toLocaleString() }
function fmtProfit(n: number): string { return `${n >= 0 ? '+' : ''}${fmtNum(n)}` }

function periodKey(item: DailyStats | PeriodStats): string {
  return 'date' in item ? item.date : item.period
}
function periodLabel(item: DailyStats | PeriodStats): string {
  if ('label' in item) return item.label
  return item.date
}

/* ── メインコンポーネント ────────────────────────────────────────── */

export default function FinancialDashboard({ data, onSelectRace }: Props) {
  const availableModels = Object.keys(data)
  const [model,       setModel]       = useState<string>(availableModels[0] ?? '')
  const [granularity, setGranularity] = useState<Granularity>('daily')
  const [expandedKey, setExpKey]      = useState<string | null>(null)
  const [expandedBet, setExpBet]      = useState<string | null>(null)

  const modelData: FinancialModelData | undefined = data[model]
  const rows: (DailyStats | PeriodStats)[] = useMemo(() => {
    if (!modelData) return []
    const list = modelData[granularity] ?? []
    return [...list].reverse()          // 新しい日付を先頭に
  }, [modelData, granularity])

  // KPI は全期間合計
  const allDays = modelData?.daily ?? []
  const today     = new Date().toISOString().slice(0, 10)
  const todayData = allDays.find(d => d.date === today) ?? allDays[allDays.length - 1] ?? null
  const totalInvested = allDays.reduce((s, d) => s + d.invested, 0)
  const totalPayout   = allDays.reduce((s, d) => s + d.payout,   0)
  const totalProfit   = totalPayout - totalInvested
  const totalRoi      = totalInvested > 0 ? (totalPayout / totalInvested) * 100 : 0

  const isCyan = modelColor(model) === 'cyan'
  const accent = isCyan ? 'var(--neon-cyan)' : 'var(--neon-gold)'

  function switchModel(m: string) {
    setModel(m); setExpKey(null); setExpBet(null)
  }
  function toggleKey(key: string) {
    setExpKey(prev => prev === key ? null : key); setExpBet(null)
  }
  function toggleBet(bt: string) {
    setExpBet(prev => prev === bt ? null : bt)
  }

  const GRAN_LABELS: { id: Granularity; label: string }[] = [
    { id: 'daily',   label: '日次' },
    { id: 'monthly', label: '月別' },
    { id: 'yearly',  label: '年間' },
  ]

  // チャート用データは日次固定（時系列グラフは日次のみ意味がある）
  const chartDays = allDays

  return (
    <div className="space-y-5">

      {/* ── ヘッダー：モデルタブ ──────────────────────────────── */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-xl font-bold neon-text tracking-[0.2em]">収支管理</h1>
        <div className="flex gap-2 flex-wrap">
          {availableModels.map(m => {
            const active = model === m
            const cyan   = modelColor(m) === 'cyan'
            return (
              <button key={m} onClick={() => switchModel(m)}
                className="px-4 py-2 text-sm font-bold rounded tracking-wider transition-all"
                style={active ? {
                  background: cyan ? 'rgba(0,200,255,0.15)' : 'rgba(255,215,0,0.15)',
                  border: `1px solid ${cyan ? 'var(--neon-cyan)' : 'var(--neon-gold)'}`,
                  color:  cyan ? 'var(--neon-cyan)' : 'var(--neon-gold)',
                  boxShadow: `0 0 14px ${cyan ? 'rgba(0,200,255,0.35)' : 'rgba(255,215,0,0.35)'}`,
                } : {
                  background: 'rgba(255,255,255,0.03)',
                  border: '1px solid rgba(255,255,255,0.12)',
                  color: 'var(--text-muted)',
                }}>
                {modelIcon(m)} {m}
              </button>
            )
          })}
        </div>
      </div>

      {!modelData || allDays.length === 0 ? <EmptyState /> : (
        <>
          {/* ── KPI カード ────────────────────────────────────── */}
          <div className="grid grid-cols-2 xl:grid-cols-4 gap-4">
            <KpiCard
              label={todayData?.date === today ? '本日の損益' : `最終日の損益 (${todayData?.date ?? '—'})`}
              value={todayData ? `${fmtProfit(todayData.profit)}円` : '—'}
              positive={todayData ? todayData.profit >= 0 : null}
              color={isCyan ? 'cyan' : 'gold'}
            />
            <KpiCard
              label="累計回収率"
              value={`${totalRoi.toFixed(1)}%`}
              positive={totalRoi >= 100}
              color={isCyan ? 'cyan' : 'gold'}
            />
            <KpiCard
              label="累計投資額"
              value={`${fmtNum(totalInvested)}円`}
              positive={null}
              color="muted"
            />
            <KpiCard
              label="累計払戻金"
              value={`${fmtNum(totalPayout)}円`}
              positive={totalProfit >= 0}
              color={isCyan ? 'cyan' : 'gold'}
            />
          </div>

          {/* ── 累計収支グラフ（日次固定） ─────────────────────── */}
          <div className="neon-card p-5">
            <div className="text-xs font-semibold tracking-[0.2em] uppercase mb-4"
              style={{ color: accent }}>
              累計収支推移（日次）
            </div>
            <ProfitChart days={chartDays} isCyan={isCyan} />
          </div>

          {/* ── 日別収支ヒートマップ ─────────────────────────── */}
          <div className="neon-card p-5">
            <div className="text-xs font-semibold tracking-[0.2em] uppercase mb-4"
              style={{ color: accent }}>
              日別収支ヒートマップ
            </div>
            <ProfitHeatmap days={chartDays} isCyan={isCyan} />
          </div>

          {/* ── 粒度トグル＋収支テーブル ──────────────────────── */}
          <div className="neon-card overflow-hidden">
            {/* ヘッダー行：トグル＋累計損益 */}
            <div className="px-5 py-3 border-b border-[rgba(0,200,255,0.12)] flex items-center justify-between gap-4 flex-wrap">
              {/* 粒度トグル */}
              <div className="flex gap-1 p-1 rounded"
                style={{ background: 'rgba(0,0,0,0.3)', border: '1px solid rgba(0,200,255,0.12)' }}>
                {GRAN_LABELS.map(g => (
                  <button key={g.id}
                    onClick={() => { setGranularity(g.id); setExpKey(null); setExpBet(null) }}
                    className="px-4 py-1.5 text-sm font-bold rounded transition-all"
                    style={granularity === g.id ? {
                      background: isCyan ? 'rgba(0,200,255,0.2)' : 'rgba(255,215,0,0.2)',
                      color: accent,
                      boxShadow: `0 0 8px ${isCyan ? 'rgba(0,200,255,0.3)' : 'rgba(255,215,0,0.3)'}`,
                    } : {
                      color: 'var(--text-muted)',
                    }}>
                    {g.label}
                  </button>
                ))}
              </div>
              <span className={`text-base font-black ${totalProfit >= 0 ? 'neon-text-green' : 'neon-text-red'}`}>
                累計 {fmtProfit(totalProfit)}円
              </span>
            </div>

            {/* テーブル本体 — overflow-x-auto でスマホ横スクロール対応 */}
            <div className="table-scroll overflow-x-auto">
              <table className="w-full race-table" style={{ minWidth: '560px' }}>
                <thead>
                  <tr>
                    <th className="text-left w-8"></th>
                    <th>{granularity === 'daily' ? '日付' : granularity === 'monthly' ? '月' : '年'}</th>
                    <th className="text-right">投資額</th>
                    <th className="text-right">払戻額</th>
                    <th className="text-right">損益</th>
                    <th className="text-right">回収率</th>
                    <th className="text-right hidden sm:table-cell">的中/買い目</th>
                    <th className="text-right">累計損益</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map(item => {
                    const key = periodKey(item)
                    return (
                      <Fragment key={key}>
                        {/* 集計行 */}
                        <tr
                          className="cursor-pointer"
                          style={{ background: expandedKey === key ? 'rgba(0,200,255,0.06)' : undefined }}
                          onClick={() => toggleKey(key)}
                        >
                          <td className="text-center text-[var(--text-muted)]">
                            {expandedKey === key ? '▼' : '▶'}
                          </td>
                          <td className="font-mono font-bold" style={{ color: accent }}>
                            {periodLabel(item)}
                          </td>
                          <td className="text-right font-mono">{fmtNum(item.invested)}</td>
                          <td className="text-right font-mono">{fmtNum(item.payout)}</td>
                          <td className={`text-right font-mono font-bold ${
                            item.profit > 0 ? 'neon-text-green' : item.profit < 0 ? 'neon-text-red' : ''
                          }`}>
                            {fmtProfit(item.profit)}
                          </td>
                          <td className={`text-right font-mono ${item.roi >= 100 ? 'neon-text-green' : 'neon-text-red'}`}>
                            {item.roi.toFixed(1)}%
                          </td>
                          <td className="text-right text-[var(--text-muted)] hidden sm:table-cell">
                            {item.hits} / {item.total_bets}
                          </td>
                          <td className={`text-right font-mono font-bold ${
                            item.cumulative_profit > 0 ? 'neon-text-green' : item.cumulative_profit < 0 ? 'neon-text-red' : ''
                          }`}>
                            {fmtProfit(item.cumulative_profit)}
                          </td>
                        </tr>

                        {/* 券種ドリルダウン */}
                        {expandedKey === key && (
                          <tr key={`${key}-detail`}>
                            <td colSpan={8} style={{ padding: 0, background: 'rgba(0,200,255,0.02)' }}>
                              <BetTypeBreakdown
                                stats={item.by_bet_type}
                                expandedBet={expandedBet}
                                onToggleBet={toggleBet}
                                onSelectRace={granularity === 'daily' ? onSelectRace : undefined}
                                isCyan={isCyan}
                              />
                            </td>
                          </tr>
                        )}
                      </Fragment>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  )
}


/* ── 券種別ブレークダウン ───────────────────────────────────────── */

function BetTypeBreakdown({
  stats, expandedBet, onToggleBet, onSelectRace, isCyan,
}: {
  stats:         BetTypeStats[]
  expandedBet:   string | null
  onToggleBet:   (bt: string) => void
  onSelectRace?: (raceId: string) => void
  isCyan:        boolean
}) {
  const accent = isCyan ? 'var(--neon-cyan)' : 'var(--neon-gold)'
  return (
    <div className="px-2 sm:px-4 py-3 space-y-1.5">
      <div className="text-xs text-[var(--text-muted)] tracking-[0.15em] pb-1.5 border-b border-[rgba(0,200,255,0.08)]">
        券種別内訳{onSelectRace ? ' — レース行をクリックで詳細表示' : ''}
      </div>
      {stats.map(bt => (
        <div key={bt.bet_type}>
          {/* overflow-x-auto でスマホ横スクロール対応 */}
          <div className="overflow-x-auto">
          <div
            className="flex items-center gap-2 px-2 py-2 rounded cursor-pointer transition-all"
            style={{
              minWidth: '440px',
              background: expandedBet === bt.bet_type ? 'rgba(0,200,255,0.08)' : 'rgba(0,200,255,0.02)',
              border: `1px solid ${expandedBet === bt.bet_type ? 'rgba(0,200,255,0.2)' : 'transparent'}`,
            }}
            onClick={() => onToggleBet(bt.bet_type)}
          >
            <span className="text-xs text-[var(--text-muted)] w-3 shrink-0">
              {expandedBet === bt.bet_type ? '▼' : '▶'}
            </span>
            <span className="text-sm font-bold w-14 shrink-0" style={{ color: accent }}>{bt.bet_type}</span>
            <span className="text-xs text-[var(--text-muted)] w-24 text-right shrink-0">投 {fmtNum(bt.invested)}</span>
            <span className="text-xs text-[var(--text-muted)] w-24 text-right shrink-0">払 {fmtNum(bt.payout)}</span>
            <span className={`text-xs font-bold w-20 text-right shrink-0 ${bt.profit > 0 ? 'neon-text-green' : 'neon-text-red'}`}>
              {fmtProfit(bt.profit)}
            </span>
            <span className={`text-xs font-mono w-16 text-right shrink-0 ${bt.roi >= 100 ? 'neon-text-green' : 'neon-text-red'}`}>
              {bt.roi.toFixed(1)}%
            </span>
            <span className="text-xs text-[var(--text-muted)] ml-auto shrink-0">
              {bt.hits}的中 / {bt.total_bets}件
            </span>
            {bt.hits > 0 && (
              <span className="text-xs px-1.5 py-0.5 rounded font-bold shrink-0"
                style={{ background: 'rgba(0,255,136,0.15)', color: 'var(--neon-green)', border: '1px solid rgba(0,255,136,0.3)' }}>
                HIT
              </span>
            )}
          </div>
          </div>

          {expandedBet === bt.bet_type && bt.races && bt.races.length > 0 && (
            <RaceList races={bt.races} onSelectRace={onSelectRace} />
          )}
          {expandedBet === bt.bet_type && (!bt.races || bt.races.length === 0) && (
            <div className="ml-6 mt-1 text-xs text-[var(--text-muted)] py-2 px-3">
              レース詳細は日次ビューで確認できます
            </div>
          )}
        </div>
      ))}
    </div>
  )
}


/* ── レース一覧 ─────────────────────────────────────────────────── */

function RaceList({
  races, onSelectRace,
}: {
  races:          RaceHit[]
  onSelectRace?:  (raceId: string) => void
}) {
  return (
    <div className="ml-6 mt-1 mb-2 rounded overflow-hidden border border-[rgba(0,200,255,0.08)]">
      <table className="w-full" style={{ fontSize: '0.8rem' }}>
        <thead>
          <tr style={{ background: 'rgba(0,0,0,0.3)' }}>
            <th className="px-3 py-1.5 text-left text-[var(--text-muted)] font-normal tracking-wider">会場</th>
            <th className="px-2 py-1.5 text-center text-[var(--text-muted)] font-normal">R</th>
            <th className="px-3 py-1.5 text-left text-[var(--text-muted)] font-normal">レース名</th>
            <th className="px-3 py-1.5 text-right text-[var(--text-muted)] font-normal">投資</th>
            <th className="px-3 py-1.5 text-right text-[var(--text-muted)] font-normal">払戻</th>
            <th className="px-3 py-1.5 text-center text-[var(--text-muted)] font-normal">結果</th>
            {onSelectRace && <th className="px-2 py-1.5"></th>}
          </tr>
        </thead>
        <tbody>
          {races.map((r, i) => (
            <tr key={r.race_id + i}
              style={{ borderTop: '1px solid rgba(0,200,255,0.05)' }}
              className={r.is_hit ? 'hit-normal' : 'hit-miss'}
            >
              <td className="px-3 py-1.5 text-[var(--text-muted)]">{r.venue}</td>
              <td className="px-2 py-1.5 text-center text-[var(--text-muted)]">{r.race_number}R</td>
              <td className="px-3 py-1.5 text-[var(--text-primary)] font-medium"
                style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {r.race_name || `${r.venue} ${r.race_number}R`}
              </td>
              <td className="px-3 py-1.5 text-right font-mono text-[var(--text-muted)]">
                {fmtNum(r.invested)}
              </td>
              <td className="px-3 py-1.5 text-right font-mono">
                {r.is_hit
                  ? <span className="neon-text-green font-bold">{fmtNum(r.payout)}</span>
                  : <span className="text-[var(--text-muted)]">—</span>}
              </td>
              <td className="px-3 py-1.5 text-center">
                {r.is_hit ? (
                  <span className="text-xs font-bold px-1.5 py-0.5 rounded"
                    style={{ background: 'rgba(0,255,136,0.2)', color: 'var(--neon-green)', border: '1px solid rgba(0,255,136,0.4)' }}>
                    ◎的中
                  </span>
                ) : (
                  <span className="text-xs text-[rgba(255,51,102,0.6)]">✕</span>
                )}
              </td>
              {onSelectRace && (
                <td className="px-2 py-1.5 text-center">
                  <button onClick={() => onSelectRace(r.race_id)}
                    className="text-xs px-2 py-0.5 rounded transition-all"
                    style={{
                      background: 'rgba(0,200,255,0.08)',
                      border: '1px solid rgba(0,200,255,0.2)',
                      color: 'var(--neon-cyan)',
                    }}>
                    詳細→
                  </button>
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}


/* ── KPI カード ─────────────────────────────────────────────────── */

function KpiCard({
  label, value, positive, color,
}: {
  label:    string
  value:    string
  positive: boolean | null
  color:    'cyan' | 'gold' | 'muted'
}) {
  const accent = { cyan: '#00c8ff', gold: '#ffd700', muted: '#4a7a96' }[color]
  const valueColor = positive === true  ? 'var(--neon-green)'
    : positive === false ? 'var(--neon-red)'
    : accent
  return (
    <div className="neon-card p-5 flex flex-col gap-2" style={{ borderColor: `${accent}44` }}>
      <div className="text-sm text-[var(--text-muted)] tracking-wider">{label}</div>
      <div className="text-3xl font-black leading-none"
        style={{ color: valueColor, textShadow: `0 0 20px ${valueColor}80, 0 0 40px ${valueColor}30` }}>
        {value}
      </div>
    </div>
  )
}


/* ── SVG ラインチャート ─────────────────────────────────────────── */

function ProfitChart({ days, isCyan }: { days: DailyStats[]; isCyan: boolean }) {
  if (days.length < 2) {
    return (
      <div className="text-center text-sm text-[var(--text-muted)] py-8">
        データが2日以上になるとグラフが表示されます
      </div>
    )
  }
  const W = 800, H = 200
  const PAD = { top: 20, right: 20, bottom: 30, left: 65 }
  const cW = W - PAD.left - PAD.right
  const cH = H - PAD.top  - PAD.bottom

  const values = days.map(d => d.cumulative_profit)
  const rawMin = Math.min(0, ...values)
  const rawMax = Math.max(0, ...values)
  const margin = (rawMax - rawMin) * 0.05 || 100
  const minV = rawMin - margin, maxV = rawMax + margin
  const range = maxV - minV

  const toX = (i: number) => PAD.left + (i / (days.length - 1)) * cW
  const toY = (v: number) => PAD.top  + (1 - (v - minV) / range) * cH
  const zeroY = toY(0)

  const pts     = days.map((d, i) => `${toX(i)},${toY(d.cumulative_profit)}`).join(' ')
  const fillPts = `${toX(0)},${zeroY} ${pts} ${toX(days.length - 1)},${zeroY}`

  const color    = isCyan ? '#00c8ff' : '#ffd700'
  const colorRgb = isCyan ? '0,200,255' : '255,215,0'

  const step = Math.max(1, Math.floor((days.length - 1) / 4))
  const labelIdxs: number[] = []
  for (let i = 0; i < days.length; i += step) labelIdxs.push(i)
  if (labelIdxs[labelIdxs.length - 1] !== days.length - 1) labelIdxs.push(days.length - 1)

  const yLabels = [
    { v: rawMin, show: rawMin !== 0 },
    { v: 0,      show: true },
    { v: rawMax, show: rawMax !== 0 },
  ]

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', height: 'auto' }}>
      <line x1={PAD.left} y1={zeroY} x2={W - PAD.right} y2={zeroY}
        stroke="rgba(255,255,255,0.2)" strokeWidth="1" strokeDasharray="5 4" />
      <polygon points={fillPts} fill={`rgba(${colorRgb},0.07)`} />
      <polyline points={pts} fill="none" stroke={color} strokeWidth="2.5"
        style={{ filter: `drop-shadow(0 0 6px ${color})` }} />
      {days.length <= 60 && days.map((d, i) => (
        <circle key={i} cx={toX(i)} cy={toY(d.cumulative_profit)} r="3" fill={color}
          style={{ filter: `drop-shadow(0 0 4px ${color})` }} />
      ))}
      {labelIdxs.map(i => (
        <text key={i} x={toX(i)} y={H - 6} textAnchor="middle"
          fill="rgba(255,255,255,0.4)" fontSize="10">
          {days[i].date.slice(5)}
        </text>
      ))}
      {yLabels.filter(l => l.show).map(({ v }) => (
        <text key={v} x={PAD.left - 6} y={toY(v) + 4} textAnchor="end"
          fill="rgba(255,255,255,0.4)" fontSize="10">
          {v >= 0 ? '+' : ''}{Math.round(v / 1000)}k
        </text>
      ))}
    </svg>
  )
}


/* ── 日別収支ヒートマップ（GitHub草スタイル） ─────────────────── */

function ProfitHeatmap({ days, isCyan }: { days: DailyStats[]; isCyan: boolean }) {
  if (days.length === 0) return null

  // 全日程を Date → profit のマップに変換
  const profitMap = new Map<string, number>()
  for (const d of days) profitMap.set(d.date, d.profit)

  // 表示範囲: 最古〜最新を含む 52 週 × 7 日グリッド
  const allDates = days.map(d => new Date(d.date)).sort((a, b) => +a - +b)
  const firstDate = allDates[0]
  const lastDate  = allDates[allDates.length - 1]

  // 最初の日曜まで巻き戻し（週の先頭を日曜にする）
  const gridStart = new Date(firstDate)
  gridStart.setDate(gridStart.getDate() - gridStart.getDay())

  // 週数を計算（最大 53 週）
  const msPerDay  = 86400000
  const totalDays = Math.ceil((+lastDate - +gridStart) / msPerDay) + 1
  const numWeeks  = Math.ceil(totalDays / 7)

  // profit の最大絶対値でスケーリング
  const maxAbs = Math.max(1, ...days.map(d => Math.abs(d.profit)))

  function getColor(date: Date): string {
    const key = date.toISOString().slice(0, 10)
    if (!profitMap.has(key)) return 'rgba(255,255,255,0.04)'   // データなし
    const p   = profitMap.get(key)!
    const t   = Math.min(1, Math.abs(p) / maxAbs)              // 0〜1 強度
    const lvl = Math.ceil(t * 4)                               // 1〜4 段階
    if (p > 0) {
      // 利益: 緑グラデーション
      const alphas = [0.25, 0.45, 0.70, 0.95]
      return `rgba(0,255,136,${alphas[lvl - 1]})`
    } else {
      // 損失: 赤グラデーション
      const alphas = [0.2, 0.4, 0.65, 0.9]
      return `rgba(255,51,102,${alphas[lvl - 1]})`
    }
  }

  const CELL    = 13   // セルサイズ (px)
  const GAP     = 2    // セル間隔 (px)
  const STEP    = CELL + GAP
  const PAD_TOP = 22   // 月ラベル分の余白
  const PAD_LEFT = 26  // 曜日ラベル分の余白
  const W = PAD_LEFT + numWeeks * STEP
  const H = PAD_TOP  + 7 * STEP

  // 月ラベルの位置（週の先頭が月初ならラベル描画）
  const monthLabels: { x: number; label: string }[] = []
  for (let w = 0; w < numWeeks; w++) {
    const d = new Date(+gridStart + w * 7 * msPerDay)
    if (d.getDate() <= 7) {
      monthLabels.push({
        x: PAD_LEFT + w * STEP,
        label: `${d.getMonth() + 1}月`,
      })
    }
  }

  const DAY_LABELS = ['日', '月', '火', '水', '木', '金', '土']

  // セルを生成
  const cells: { x: number; y: number; color: string; title: string }[] = []
  for (let w = 0; w < numWeeks; w++) {
    for (let d = 0; d < 7; d++) {
      const date = new Date(+gridStart + (w * 7 + d) * msPerDay)
      const key  = date.toISOString().slice(0, 10)
      const profit = profitMap.get(key)
      const title  = profit !== undefined
        ? `${key}: ${profit >= 0 ? '+' : ''}${Math.round(profit).toLocaleString()}円`
        : key
      cells.push({
        x: PAD_LEFT + w * STEP,
        y: PAD_TOP  + d * STEP,
        color: getColor(date),
        title,
      })
    }
  }

  return (
    <div className="overflow-x-auto">
      <svg
        viewBox={`0 0 ${W} ${H}`}
        style={{ width: Math.min(W, 900), height: 'auto', display: 'block' }}
      >
        {/* 曜日ラベル */}
        {[1, 3, 5].map(d => (
          <text key={d}
            x={PAD_LEFT - 4}
            y={PAD_TOP + d * STEP + CELL - 1}
            fontSize="9"
            textAnchor="end"
            fill="rgba(255,255,255,0.3)"
          >
            {DAY_LABELS[d]}
          </text>
        ))}

        {/* 月ラベル */}
        {monthLabels.map(({ x, label }) => (
          <text key={`m-${x}`}
            x={x} y={PAD_TOP - 6}
            fontSize="9"
            fill="rgba(255,255,255,0.35)"
          >
            {label}
          </text>
        ))}

        {/* ヒートマップセル */}
        {cells.map(({ x, y, color, title }) => (
          <rect key={`${x}-${y}`}
            x={x} y={y}
            width={CELL} height={CELL}
            rx="2" ry="2"
            fill={color}
          >
            <title>{title}</title>
          </rect>
        ))}
      </svg>

      {/* 凡例 */}
      <div className="flex items-center gap-3 mt-3 text-xs text-[var(--text-muted)]">
        <span>損失</span>
        {[0.2, 0.4, 0.65, 0.9].map(a => (
          <svg key={a} width="13" height="13" style={{ display: 'inline' }}>
            <rect width="13" height="13" rx="2" fill={`rgba(255,51,102,${a})`} />
          </svg>
        ))}
        <span className="mx-1">|</span>
        {[0.25, 0.45, 0.7, 0.95].map(a => (
          <svg key={a} width="13" height="13" style={{ display: 'inline' }}>
            <rect width="13" height="13" rx="2" fill={`rgba(0,255,136,${a})`} />
          </svg>
        ))}
        <span>利益</span>
        <svg width="13" height="13" style={{ display: 'inline', marginLeft: 8 }}>
          <rect width="13" height="13" rx="2" fill="rgba(255,255,255,0.04)" />
        </svg>
        <span>データなし</span>
      </div>
    </div>
  )
}


/* ── データなし ─────────────────────────────────────────────────── */

function EmptyState() {
  return (
    <div className="neon-card p-12 text-center">
      <div className="text-4xl mb-4 opacity-30">📊</div>
      <div className="neon-text text-lg tracking-[0.3em] mb-2">NO FINANCIAL DATA</div>
      <div className="text-sm text-[var(--text-muted)]">
        python web/generate_data.py を実行してデータを生成してください
      </div>
    </div>
  )
}
