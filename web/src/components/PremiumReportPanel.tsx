'use client'

import { useState, useEffect } from 'react'

interface PremiumCandidate {
  bet_type: string
  combo: number[]
  label: string
  prob: number
  odds: number
  ev: number
  stake: number
}

interface PremiumRace {
  race_id: string
  venue: string
  race_number: number
  race_name: string
  max_ev: number
  candidates: PremiumCandidate[]
}

interface PremiumSignals {
  date: string | null
  generated_at?: string
  ev_min?: number
  n_races: number
  n_signals: number
  max_ev: number
  races: PremiumRace[]
}

const GOLD = '#C9A227'
const GOLD_BRIGHT = '#E8C766'

export default function PremiumReportPanel() {
  const [signals, setSignals] = useState<PremiumSignals | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [reportAvailable, setReportAvailable] = useState(false)

  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const [sigRes, repRes] = await Promise.all([
          fetch('/api/premium-signals'),
          fetch('/api/premium-report', { method: 'GET' }),
        ])
        if (cancelled) return
        if (sigRes.ok) setSignals(await sigRes.json())
        setReportAvailable(repRes.ok)
      } catch (e) {
        if (!cancelled) setError(String(e))
      }
    }
    load()
    // 週末の自動生成に追従するため 60 秒ごとに再取得
    const timer = setInterval(load, 60_000)
    return () => { cancelled = true; clearInterval(timer) }
  }, [])

  if (error) {
    return <div className="p-6 text-red-400 text-sm">プレミアムデータ取得エラー: {error}</div>
  }

  const hasSignals = !!signals?.date && signals.n_signals > 0

  return (
    <div className="min-w-0 w-full space-y-4">
      {/* ── ヘッダー ─────────────────────────────── */}
      <div
        className="rounded-xl px-6 py-5"
        style={{
          background: `linear-gradient(135deg, rgba(201,162,39,0.10), rgba(11,10,8,0.6))`,
          border: `1px solid rgba(201,162,39,0.35)`,
        }}
      >
        <div className="flex flex-wrap items-baseline justify-between gap-3">
          <div>
            <div className="text-[10px] tracking-[0.4em]" style={{ color: GOLD_BRIGHT }}>
              UMA-LOGIC PRIVATE
            </div>
            <h2 className="text-xl font-bold mt-1" style={{ color: GOLD_BRIGHT, fontFamily: 'Georgia, serif' }}>
              💎 三連系プレミアムレポート
            </h2>
          </div>
          {signals?.date && (
            <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
              対象日: {signals.date}
              {signals.generated_at && ` ／ 生成: ${signals.generated_at.replace('T', ' ')}`}
            </div>
          )}
        </div>

        {/* サマリ統計 */}
        <div className="grid grid-cols-3 gap-3 mt-4">
          {[
            { label: 'RACES', value: signals?.n_races ?? 0 },
            { label: 'SIGNALS', value: signals?.n_signals ?? 0 },
            { label: 'MAX EV', value: (signals?.max_ev ?? 0).toFixed(2), highlight: (signals?.max_ev ?? 0) >= 1.42 },
          ].map(s => (
            <div
              key={s.label}
              className="rounded-lg px-4 py-3 text-center"
              style={{ background: 'rgba(0,0,0,0.45)', border: '1px solid rgba(201,162,39,0.2)' }}
            >
              <div className="text-[9px] tracking-[0.3em]" style={{ color: 'var(--text-muted)' }}>{s.label}</div>
              <div
                className="text-2xl font-bold mt-1"
                style={{ color: s.highlight ? '#FFD75E' : GOLD, fontFamily: 'Georgia, serif' }}
              >
                {s.value}{s.highlight ? ' 🔥' : ''}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ── 高EVシグナル一覧 ──────────────────────── */}
      {hasSignals ? (
        <div className="space-y-3">
          {signals!.races.map(r => (
            <div
              key={r.race_id}
              className="rounded-xl px-5 py-4"
              style={{ background: 'rgba(20,18,14,0.7)', border: '1px solid rgba(201,162,39,0.25)' }}
            >
              <div className="flex items-baseline justify-between gap-3 flex-wrap">
                <div className="font-bold" style={{ color: GOLD_BRIGHT }}>
                  {r.venue}{r.race_number}R{r.race_name ? `（${r.race_name}）` : ''}
                </div>
                <div className="text-sm" style={{ color: r.max_ev >= 1.42 ? '#FFD75E' : GOLD }}>
                  MAX EV {r.max_ev.toFixed(2)}{r.max_ev >= 1.42 ? ' 🔥' : ''}
                </div>
              </div>
              <div className="overflow-x-auto mt-2">
                <table className="w-full text-xs" style={{ color: 'var(--text-primary)' }}>
                  <thead>
                    <tr className="text-[10px] tracking-widest uppercase" style={{ color: 'var(--text-muted)' }}>
                      <th className="text-left py-1 pr-3">券種</th>
                      <th className="text-left py-1 pr-3">買い目</th>
                      <th className="text-right py-1 pr-3">確率</th>
                      <th className="text-right py-1 pr-3">推定オッズ</th>
                      <th className="text-right py-1 pr-3">EV</th>
                      <th className="text-right py-1">参考ベット</th>
                    </tr>
                  </thead>
                  <tbody>
                    {r.candidates.map((c, i) => (
                      <tr key={i} style={{ borderTop: '1px solid rgba(201,162,39,0.1)' }}>
                        <td className="py-1.5 pr-3">{c.bet_type}</td>
                        <td className="py-1.5 pr-3 font-mono">{c.label || c.combo.join('-')}</td>
                        <td className="py-1.5 pr-3 text-right">{(c.prob * 100).toFixed(2)}%</td>
                        <td className="py-1.5 pr-3 text-right">{c.odds.toFixed(1)}</td>
                        <td
                          className="py-1.5 pr-3 text-right font-bold"
                          style={{ color: c.ev >= 1.42 ? '#FFD75E' : GOLD }}
                        >
                          {c.ev.toFixed(2)}{c.ev >= 1.42 ? ' 🔥' : ''}
                        </td>
                        <td className="py-1.5 text-right">
                          {c.stake > 0 ? `¥${c.stake.toLocaleString()}` : '見送り可'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div
          className="rounded-xl px-6 py-8 text-center"
          style={{ background: 'rgba(20,18,14,0.7)', border: '1px solid rgba(201,162,39,0.2)' }}
        >
          <div className="text-sm" style={{ color: 'var(--text-primary)' }}>
            {signals?.date
              ? `本日は EV ${(signals.ev_min ?? 1.30).toFixed(2)} 超の三連系候補がありません。基準を満たさない日は買い目を作らないのが規律です。`
              : 'プレミアムシグナルは週末バッチ（開催日朝）に自動生成されます。生成後ここに自動反映されます。'}
          </div>
        </div>
      )}

      {/* ── 漆黒×ゴールド HTML レポート（自己完結文書を iframe 表示）── */}
      {reportAvailable && (
        <div
          className="rounded-xl overflow-hidden"
          style={{ border: '1px solid rgba(201,162,39,0.35)' }}
        >
          <div
            className="px-4 py-2 text-[10px] tracking-[0.3em] flex items-center justify-between"
            style={{ background: 'rgba(201,162,39,0.08)', color: GOLD_BRIGHT }}
          >
            <span>PREMIUM REPORT — FULL DOCUMENT</span>
            <a
              href="/api/premium-report"
              target="_blank"
              rel="noopener noreferrer"
              className="underline hover:opacity-80"
              style={{ color: GOLD }}
            >
              全画面で開く ↗
            </a>
          </div>
          <iframe
            src="/api/premium-report"
            title="プレミアムレポート"
            className="w-full border-0"
            style={{ height: '70vh', background: '#0B0A08' }}
          />
        </div>
      )}
    </div>
  )
}
