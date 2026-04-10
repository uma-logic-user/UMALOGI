'use client'

import type { RaceBias } from '@/types/race'

interface Props {
  bias:      RaceBias
  condition: string
}

/**
 * 当日バイアスパネル
 *
 * today_inner_bias : 内枠率 - 外枠率
 *   > +0.05 → 内枠有利
 *   < -0.05 → 外枠有利
 * today_front_bias : 1番人気的中率（当日先行済みレース）
 *   > 0.6  → 前残り傾向
 *   < 0.3  → 差し・追い込み傾向
 */
export default function BiasPanel({ bias, condition }: Props) {
  const inner = bias.today_inner_bias
  const front = bias.today_front_bias
  const count = bias.today_race_count ?? 0

  // 内枠バイアス: -1 〜 +1 を 0〜100% に正規化
  const innerPct = inner != null
    ? Math.round(Math.min(Math.max((inner + 0.15) / 0.30, 0), 1) * 100)
    : 50

  // 前残り率: 0 〜 1 を 0〜100%
  const frontPct = front != null ? Math.round(front * 100) : 50

  const innerLabel =
    inner == null         ? 'データ待ち' :
    inner > 0.10          ? '内枠大有利' :
    inner > 0.05          ? '内枠有利'   :
    inner < -0.10         ? '外枠大有利' :
    inner < -0.05         ? '外枠有利'   :
                            'フラット'

  const innerColor =
    inner == null         ? 'var(--text-muted)' :
    inner > 0.05          ? 'var(--neon-green)' :
    inner < -0.05         ? 'var(--neon-cyan)'  :
                            'var(--text-primary)'

  const frontLabel =
    front == null  ? 'データ待ち' :
    front > 0.60   ? '前残り有力' :
    front > 0.45   ? '標準'       :
    front > 0.30   ? '差し有利'   :
                     '追い込み場'

  const frontColor =
    front == null  ? 'var(--text-muted)'  :
    front > 0.60   ? 'var(--neon-gold)'   :
    front > 0.45   ? 'var(--text-primary)':
                     'var(--neon-cyan)'

  return (
    <div
      className="rounded-lg px-5 py-4 mb-4"
      style={{
        background: 'rgba(0,200,255,0.03)',
        border: '1px solid rgba(0,200,255,0.15)',
      }}
    >
      {/* ヘッダー */}
      <div className="flex items-center justify-between mb-3">
        <span className="text-xs neon-text tracking-[0.2em] font-semibold uppercase">
          Track Bias — 当日馬場傾向
        </span>
        <span className="text-[10px] text-[var(--text-muted)]">
          {count > 0 ? `先行 ${count} R のデータ` : '当日データなし（推定値）'}
        </span>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">

        {/* ① 内外バイアス */}
        <div className="space-y-1.5">
          <div className="flex items-center justify-between text-xs">
            <span className="text-[var(--text-muted)]">外枠有利</span>
            <span className="font-bold" style={{ color: innerColor }}>{innerLabel}</span>
            <span className="text-[var(--text-muted)]">内枠有利</span>
          </div>
          <div
            className="relative h-2 rounded-full overflow-hidden"
            style={{ background: 'rgba(255,255,255,0.07)' }}
          >
            {/* 左（外枠）= 青系 / 右（内枠）= 緑系 */}
            <div
              className="absolute top-0 left-0 h-full rounded-full transition-all duration-700"
              style={{
                width: `${innerPct}%`,
                background: innerPct > 55
                  ? 'linear-gradient(90deg, rgba(0,255,136,0.3), rgba(0,255,136,0.8))'
                  : innerPct < 45
                  ? 'linear-gradient(90deg, rgba(0,200,255,0.8), rgba(0,200,255,0.3))'
                  : 'linear-gradient(90deg, rgba(0,200,255,0.5), rgba(0,255,136,0.5))',
              }}
            />
            {/* センターライン */}
            <div
              className="absolute top-0 bottom-0 w-px bg-white opacity-30"
              style={{ left: '50%' }}
            />
          </div>
          {inner != null && (
            <div className="text-[10px] text-[var(--text-muted)] text-center">
              {inner > 0 ? `+${(inner * 100).toFixed(1)}` : (inner * 100).toFixed(1)} pt
            </div>
          )}
        </div>

        {/* ② 前残り率 */}
        <div className="space-y-1.5">
          <div className="flex items-center justify-between text-xs">
            <span className="text-[var(--text-muted)]">差し</span>
            <span className="font-bold" style={{ color: frontColor }}>{frontLabel}</span>
            <span className="text-[var(--text-muted)]">前残り</span>
          </div>
          <div
            className="relative h-2 rounded-full overflow-hidden"
            style={{ background: 'rgba(255,255,255,0.07)' }}
          >
            <div
              className="absolute top-0 left-0 h-full rounded-full transition-all duration-700"
              style={{
                width: `${frontPct}%`,
                background: frontPct > 60
                  ? 'linear-gradient(90deg, rgba(255,215,0,0.3), rgba(255,215,0,0.9))'
                  : frontPct < 35
                  ? 'linear-gradient(90deg, rgba(0,200,255,0.8), rgba(0,200,255,0.3))'
                  : 'linear-gradient(90deg, rgba(0,200,255,0.4), rgba(255,215,0,0.4))',
              }}
            />
          </div>
          {front != null && (
            <div className="text-[10px] text-[var(--text-muted)] text-center">
              前残り率 {(front * 100).toFixed(0)}%
            </div>
          )}
        </div>

        {/* ③ 馬場状態 + ステータス */}
        <div className="flex flex-col justify-between">
          <div className="flex gap-2 flex-wrap">
            {condition && (
              <ConditionBadge condition={condition} />
            )}
            {inner != null && Math.abs(inner) > 0.10 && (
              <div
                className="px-2 py-1 rounded text-[10px] font-bold"
                style={{
                  background: 'rgba(255,51,102,0.15)',
                  color: 'var(--neon-red)',
                  border: '1px solid rgba(255,51,102,0.35)',
                }}
              >
                ⚠ バイアス強
              </div>
            )}
          </div>
          {count === 0 && (
            <div className="text-[10px] text-[var(--text-muted)] mt-2">
              ※ R1以前は過去データ参考値
            </div>
          )}
        </div>

      </div>
    </div>
  )
}

function ConditionBadge({ condition }: { condition: string }) {
  const [bg, color, border] =
    condition === '良'   ? ['rgba(0,255,136,0.12)', 'var(--neon-green)', 'rgba(0,255,136,0.35)'] :
    condition === '稍重' ? ['rgba(255,215,0,0.12)',  'var(--neon-gold)',  'rgba(255,215,0,0.35)']  :
    condition === '重'   ? ['rgba(0,200,255,0.12)',  'var(--neon-cyan)',  'rgba(0,200,255,0.35)']  :
                           ['rgba(255,51,102,0.12)', 'var(--neon-red)',   'rgba(255,51,102,0.35)']

  return (
    <div
      className="px-2.5 py-1 rounded text-xs font-bold"
      style={{ background: bg, color, border: `1px solid ${border}` }}
    >
      馬場 {condition}
    </div>
  )
}
