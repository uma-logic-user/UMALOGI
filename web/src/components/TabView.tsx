'use client'

import { useState } from 'react'
import type { RaceEntry, Prediction } from '@/types/race'
import RaceHeader    from './RaceHeader'
import StatCards     from './StatCards'
import RaceTable     from './RaceTable'
import PedigreeChart from './PedigreeChart'
import PredictionsPanel from './PredictionsPanel'

type Tab = 'dashboard' | 'results' | 'predictions'

interface Props {
  races:       RaceEntry[]
  predictions: Prediction[]
  summary:     { total_races_in_db: number; overall: Record<string, unknown> }
}

export default function TabView({ races, predictions, summary }: Props) {
  const [activeTab, setActiveTab] = useState<Tab>('dashboard')

  const hasRaces       = Array.isArray(races)       && races.length > 0
  const hasPredictions = Array.isArray(predictions) && predictions.length > 0
  const featured       = hasRaces ? races[0] : null

  const reconciled = predictions.filter(p => p.is_hit !== null)
  const hits       = reconciled.filter(p => p.is_hit === 1)
  const bigHits    = hits.filter(p => (p.roi ?? 0) >= 200)
  const hitRate    = reconciled.length > 0 ? hits.length / reconciled.length * 100 : null
  const maxRoi     = hits.length > 0
    ? Math.max(...hits.map(p => p.roi ?? 0))
    : null

  const TABS: { id: Tab; label: string; badge?: number }[] = [
    { id: 'dashboard',   label: 'ダッシュボード' },
    { id: 'results',     label: 'レース結果',    badge: races.length },
    { id: 'predictions', label: 'AI予想',        badge: predictions.length },
  ]

  return (
    <div className="space-y-5">

      {/* ── サマリーバッジ ──────────────────────────────────── */}
      <div className="flex gap-3 flex-wrap">
        <SummaryBadge label="DB総レース数" value={`${summary?.total_races_in_db ?? 0} races`} />
        <SummaryBadge label="取得済み"     value={`${races.length} races`} />
        <SummaryBadge
          label="予想データ"
          value={hasPredictions ? `${predictions.length} predictions` : 'NO DATA'}
          dim={!hasPredictions}
        />
        {hitRate !== null && (
          <SummaryBadge
            label="的中率"
            value={`${hitRate.toFixed(1)}%  (${hits.length} / ${reconciled.length})`}
          />
        )}
      </div>

      {/* ── タブバー ────────────────────────────────────────── */}
      <div className="flex gap-0 border-b border-[rgba(0,200,255,0.18)]">
        {TABS.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`relative px-6 py-3 text-sm font-semibold tracking-wider transition-colors ${
              activeTab === tab.id
                ? 'text-[var(--neon-cyan)]'
                : 'text-[var(--text-muted)] hover:text-[var(--text-primary)]'
            }`}
          >
            {tab.label}
            {tab.badge !== undefined && (
              <span className="ml-2 text-xs font-normal opacity-50">{tab.badge}</span>
            )}
            {activeTab === tab.id && (
              <span
                className="absolute bottom-0 inset-x-0 h-[2px] bg-[var(--neon-cyan)]"
                style={{ boxShadow: '0 0 8px rgba(0,200,255,0.9)' }}
              />
            )}
          </button>
        ))}
      </div>

      {/* ════════════════════════════════════════════════════════
          TAB: ダッシュボード
          ════════════════════════════════════════════════════════ */}
      {activeTab === 'dashboard' && (
        <div className="space-y-5 slide-in">
          {featured ? (
            <>
              <RaceHeader race={featured} />
              <StatCards  results={featured.results} />
              <div className="grid grid-cols-1 xl:grid-cols-[1fr_300px] gap-5">
                <PredictionSummaryCard
                  predictions={predictions}
                  hits={hits.length}
                  reconciled={reconciled.length}
                  hitRate={hitRate}
                  bigHits={bigHits.length}
                  maxRoi={maxRoi}
                />
                <PedigreeChart results={featured.results} />
              </div>
            </>
          ) : (
            <NoData />
          )}
        </div>
      )}

      {/* ════════════════════════════════════════════════════════
          TAB: レース結果
          ════════════════════════════════════════════════════════ */}
      {activeTab === 'results' && (
        <div className="space-y-5 slide-in">
          {featured ? (
            <>
              <RaceHeader race={featured} />
              <RaceTable  results={featured.results} />
            </>
          ) : (
            <NoData />
          )}
          {hasRaces && races.length > 1 && <AllRacesTable races={races} />}
        </div>
      )}

      {/* ════════════════════════════════════════════════════════
          TAB: AI予想
          ════════════════════════════════════════════════════════ */}
      {activeTab === 'predictions' && (
        <div className="space-y-4 slide-in">
          <div className="flex items-center justify-between">
            <span className="text-sm font-semibold neon-text tracking-[0.2em]">
              AI PREDICTIONS — {predictions.length} RECORDS
            </span>
            {reconciled.length > 0 && (
              <div className="flex gap-4 text-sm text-[var(--text-muted)]">
                <span>照合済 <span className="neon-text font-bold">{reconciled.length}</span></span>
                <span>的中 <span className="neon-text-green font-bold">{hits.length}</span></span>
                <span>的中率 <span className="neon-text font-bold">{hitRate?.toFixed(1)}%</span></span>
                {bigHits.length > 0 && (
                  <span>高配当 <span className="neon-text-gold font-bold">{bigHits.length}</span></span>
                )}
              </div>
            )}
          </div>
          {hasPredictions ? (
            <PredictionsPanel predictions={predictions} limit={500} />
          ) : (
            <div className="neon-card p-12 text-center">
              <div className="text-[var(--text-muted)] text-base tracking-widest">NO PREDICTIONS AVAILABLE</div>
              <div className="text-sm text-[var(--text-muted)] mt-2 opacity-50">
                予想モデルの実行後にここに表示されます
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── フッター ────────────────────────────────────────── */}
      <footer className="text-center text-sm text-[var(--text-muted)] tracking-widest pt-4 pb-2 border-t border-[rgba(0,200,255,0.08)]">
        UMALOGI &nbsp;·&nbsp; データソース: netkeiba.com
        {featured && <>&nbsp;·&nbsp; 最終更新: {featured.date}</>}
      </footer>
    </div>
  )
}


/* ── 共通サブコンポーネント ──────────────────────────────────── */

function SummaryBadge({
  label, value, dim = false,
}: { label: string; value: string; dim?: boolean }) {
  return (
    <div
      className="flex items-center gap-2 px-3 py-1.5 rounded text-sm"
      style={{
        background: 'rgba(0,200,255,0.04)',
        border: `1px solid rgba(0,200,255,${dim ? '0.08' : '0.2'})`,
        opacity: dim ? 0.5 : 1,
      }}
    >
      <span className="text-[var(--text-muted)] tracking-wider">{label}</span>
      <span className={dim ? 'text-[var(--text-muted)]' : 'neon-text font-semibold'}>{value}</span>
    </div>
  )
}

function NoData() {
  return (
    <div className="neon-card p-12 text-center">
      <div className="text-4xl mb-4 opacity-30">🏇</div>
      <div className="neon-text text-lg tracking-[0.3em] mb-2">NO RACE DATA</div>
      <div className="text-sm text-[var(--text-muted)] tracking-widest">
        python web/generate_data.py を実行してデータを生成してください
      </div>
    </div>
  )
}

/* ── AI 予想パフォーマンスサマリーカード ──────────────────────── */

function PredictionSummaryCard({
  predictions, hits, reconciled, hitRate, bigHits, maxRoi,
}: {
  predictions: Prediction[]
  hits:        number
  reconciled:  number
  hitRate:     number | null
  bigHits:     number
  maxRoi:      number | null
}) {
  const models = ['本命', '卍'] as const
  const modelStats = models.map(model => {
    const preds      = predictions.filter(p => p.model_type === model && p.is_hit !== null)
    const modelHits  = preds.filter(p => p.is_hit === 1)
    return {
      model,
      total: preds.length,
      hits:  modelHits.length,
      rate:  preds.length > 0 ? modelHits.length / preds.length * 100 : null,
    }
  })

  const recent = predictions.filter(p => p.is_hit !== null).slice(0, 12)

  return (
    <div className="neon-card p-5 space-y-5">
      <div className="text-xs neon-text tracking-[0.2em] font-semibold uppercase">
        AI 予想パフォーマンス
      </div>

      {reconciled === 0 ? (
        <div className="text-sm text-[var(--text-muted)] text-center py-8">
          照合済みデータがありません
        </div>
      ) : (
        <>
          {/* 総合的中率バー */}
          <div>
            <div className="flex justify-between text-sm mb-2">
              <span className="text-[var(--text-muted)]">総合的中率</span>
              <span className="neon-text font-bold text-base">{hitRate?.toFixed(1)}%</span>
            </div>
            <div className="h-2.5 rounded-full bg-[rgba(0,200,255,0.1)] overflow-hidden">
              <div
                className="h-full rounded-full bg-[var(--neon-cyan)]"
                style={{
                  width: `${Math.min(hitRate ?? 0, 100)}%`,
                  boxShadow: '0 0 10px rgba(0,200,255,0.6)',
                  transition: 'width 0.6s ease',
                }}
              />
            </div>
            <div className="flex justify-between text-xs text-[var(--text-muted)] mt-1.5">
              <span>照合済 {reconciled} 件</span>
              <span>的中 {hits} 件</span>
            </div>
          </div>

          {/* モデル別内訳 */}
          <div className="space-y-3">
            {modelStats.map(s => (
              <div key={s.model}>
                <div className="flex justify-between text-sm mb-1.5">
                  <span className={s.model === '卍' ? 'neon-text font-semibold' : 'neon-text-gold font-semibold'}>
                    {s.model === '卍' ? '卍モデル' : '本命モデル'}
                  </span>
                  <span className="text-[var(--text-muted)]">
                    {s.rate !== null ? `${s.rate.toFixed(1)}%` : '—'}
                    <span className="text-xs ml-2 opacity-60">({s.hits} / {s.total})</span>
                  </span>
                </div>
                <div className="h-1.5 rounded-full bg-[rgba(0,200,255,0.08)] overflow-hidden">
                  <div
                    className="h-full rounded-full"
                    style={{
                      width: `${Math.min(s.rate ?? 0, 100)}%`,
                      background:  s.model === '卍' ? 'var(--neon-cyan)' : 'var(--neon-gold)',
                      boxShadow:   `0 0 6px ${s.model === '卍' ? 'rgba(0,200,255,0.5)' : 'rgba(255,215,0,0.5)'}`,
                      transition: 'width 0.6s ease',
                    }}
                  />
                </div>
              </div>
            ))}
          </div>

          {/* キースタッツ */}
          <div className="grid grid-cols-2 gap-3">
            <div className="bg-[rgba(255,215,0,0.05)] border border-[rgba(255,215,0,0.15)] rounded-lg p-3 text-center">
              <div className="text-xs text-[var(--text-muted)] mb-1 tracking-wider">高配当的中</div>
              <div className="text-3xl font-bold neon-text-gold">{bigHits}</div>
              <div className="text-xs text-[var(--text-muted)] mt-0.5">ROI 200%超</div>
            </div>
            <div className="bg-[rgba(0,255,136,0.05)] border border-[rgba(0,255,136,0.15)] rounded-lg p-3 text-center">
              <div className="text-xs text-[var(--text-muted)] mb-1 tracking-wider">最高ROI</div>
              <div className="text-3xl font-bold neon-text-green">
                {maxRoi !== null ? `${maxRoi.toFixed(0)}%` : '—'}
              </div>
              <div className="text-xs text-[var(--text-muted)] mt-0.5">払戻率</div>
            </div>
          </div>

          {/* 直近の結果ストリーク */}
          {recent.length > 0 && (
            <div>
              <div className="text-xs text-[var(--text-muted)] mb-2 tracking-wider">直近の結果</div>
              <div className="flex gap-1.5 flex-wrap">
                {recent.map((p, i) => (
                  <span
                    key={i}
                    className="inline-flex items-center justify-center w-8 h-8 rounded text-sm font-bold"
                    style={{
                      background: p.is_hit
                        ? (p.roi ?? 0) >= 200
                          ? 'rgba(255,215,0,0.15)'
                          : 'rgba(0,255,136,0.15)'
                        : 'rgba(255,51,102,0.10)',
                      color: p.is_hit
                        ? (p.roi ?? 0) >= 200 ? 'var(--neon-gold)' : 'var(--neon-green)'
                        : 'var(--neon-red)',
                      border: `1px solid ${p.is_hit
                        ? (p.roi ?? 0) >= 200 ? 'rgba(255,215,0,0.3)' : 'rgba(0,255,136,0.3)'
                        : 'rgba(255,51,102,0.2)'}`,
                    }}
                    title={`${p.race_name} ${p.bet_type}`}
                  >
                    {p.is_hit ? '◎' : '✕'}
                  </span>
                ))}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}

/* ── 全レース一覧テーブル ──────────────────────────────────────── */

function AllRacesTable({ races }: { races: RaceEntry[] }) {
  return (
    <div className="neon-card overflow-hidden">
      <div className="px-4 py-3 border-b border-[rgba(0,200,255,0.12)]">
        <span className="text-sm neon-text tracking-[0.2em] font-semibold">
          ALL RACES — {races.length} RECORDS
        </span>
      </div>
      <div className="table-scroll">
        <table className="w-full race-table">
          <thead>
            <tr>
              <th>日付</th>
              <th>会場</th>
              <th>レース名</th>
              <th className="text-center">R</th>
              <th>距離</th>
              <th className="text-center">頭数</th>
              <th>優勝馬</th>
              <th className="text-right">単勝</th>
            </tr>
          </thead>
          <tbody>
            {races.map(r => {
              const winner = r.results?.find(h => h.rank === 1)
              return (
                <tr key={r.race_id}>
                  <td className="font-mono text-[var(--text-muted)]">{r.date}</td>
                  <td>{r.venue}</td>
                  <td className="font-semibold text-[var(--text-primary)]">{r.race_name}</td>
                  <td className="text-center text-[var(--text-muted)]">{r.race_number}</td>
                  <td className="text-[var(--text-muted)]">{r.surface}{r.distance}m</td>
                  <td className="text-center">{r.results?.length ?? 0}頭</td>
                  <td>
                    {winner
                      ? <span className="neon-text-gold font-semibold">{winner.horse_name}</span>
                      : <span className="text-[var(--text-muted)]">—</span>}
                  </td>
                  <td className="text-right font-mono">
                    {winner?.win_odds != null
                      ? <span className="text-[var(--neon-cyan)]">{winner.win_odds.toFixed(1)}</span>
                      : <span className="text-[var(--text-muted)]">—</span>}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
