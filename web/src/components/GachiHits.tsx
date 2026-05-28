'use client'

interface GachiHit {
  race_id:           string
  race_name:         string
  date:              string
  venue:             string
  surface:           string
  distance:          number
  model_type:        string
  bet_type:          string
  bet_form:          string
  n_tickets:         number
  combination_json:  string
  payout:            number
  is_hit:            number
  rank:              string | null
  notes:             string
  recommended_bet?:  number
  horse_num_to_name?: Record<string, string>
}

interface Props {
  data: GachiHit[]
}

const RANK_STYLE: Record<string, { bg: string; border: string; text: string; label: string }> = {
  S: { bg: 'rgba(255,71,87,0.15)',   border: 'rgba(255,71,87,0.5)',   text: '#ff4757', label: '💎 SUPER HIT' },
  A: { bg: 'rgba(255,165,2,0.15)',   border: 'rgba(255,165,2,0.5)',   text: '#ffa502', label: '🥇 BIG HIT'   },
  B: { bg: 'rgba(46,213,115,0.12)',  border: 'rgba(46,213,115,0.4)',  text: '#2ed573', label: '🎯 HIT'       },
  C: { bg: 'rgba(200,168,130,0.08)', border: 'rgba(200,168,130,0.3)', text: '#C8A882', label: '✓ 的中'       },
}

const ORDERED_TYPES = new Set(['馬単', '三連単'])
const MEDAL_BG  = ['#FFD700', '#C0C0C0', '#CD7F32'] as const
const MEDAL_CLR = ['#000000', '#000000', '#ffffff'] as const

// ── ヘルパー ──────────────────────────────────────────────────────────────

function parseCombos(json: string): number[][] {
  try {
    const parsed = JSON.parse(json)
    if (!Array.isArray(parsed) || parsed.length === 0) return []
    return Array.isArray(parsed[0]) ? (parsed as number[][]) : [(parsed as number[])]
  } catch { return [] }
}

/** 全コンボに必ず含まれる馬（軸馬）を昇順で返す */
function inferAxis(combos: number[][]): number[] {
  if (combos.length < 2) return []
  const all = new Set(combos.flatMap(c => c))
  return [...all].filter(h => combos.every(c => c.includes(h))).sort((a, b) => a - b)
}

/** 全コンボの全馬番を昇順で返す（相手馬リスト用） */
function allUniqueNums(combos: number[][]): number[] {
  return [...new Set(combos.flatMap(c => c))].sort((a, b) => a - b)
}

// ── サブコンポーネント ────────────────────────────────────────────────────

function AxisBadge() {
  return (
    <span style={{
      display:       'inline-block',
      background:    'rgba(184,134,11,0.2)',
      color:         '#FFD700',
      border:        '1px solid rgba(184,134,11,0.6)',
      borderRadius:  3,
      padding:       '0 4px',
      fontSize:      '0.6rem',
      fontWeight:    800,
      letterSpacing: '0.08em',
      lineHeight:    '1.7',
    }}>軸</span>
  )
}

function MedalCircle({ num, pos, isOrdered }: { num: number; pos: number; isOrdered: boolean }) {
  const medal = isOrdered && pos >= 0 && pos < 3
  return (
    <span
      title={medal ? `${pos + 1}着` : undefined}
      style={{
        display:        'inline-flex',
        alignItems:     'center',
        justifyContent: 'center',
        width:          26,
        height:         26,
        borderRadius:   '50%',
        background:     medal ? MEDAL_BG[pos]  : 'rgba(184,134,11,0.15)',
        color:          medal ? MEDAL_CLR[pos] : '#FFD700',
        fontWeight:     'bold',
        fontSize:       '0.85rem',
        fontFamily:     'monospace',
        border:         medal ? 'none' : '1px solid rgba(184,134,11,0.4)',
        flexShrink:     0,
      }}
    >{num}</span>
  )
}

/**
 * 買い目の視覚表示コンポーネント。
 * - 軸ありマルチ: 「軸」バッジ + 軸番号 + 相手馬リスト
 * - コンボ数 > 10: 全列挙をやめ "相手 N頭 / M点" サマリーに圧縮
 * - 小数コンボ: 馬番を並べて表示
 */
function ComboBadge({
  combos,
  betType,
  nameMap,
  accentColor,
}: {
  combos:      number[][]
  betType:     string
  nameMap:     Record<string, string>
  accentColor: string
}) {
  const isOrdered = ORDERED_TYPES.has(betType)
  const sep        = isOrdered ? '→' : '-'
  const axisList   = inferAxis(combos)
  const axisSet    = new Set(axisList)
  const allNums    = allUniqueNums(combos)
  const opponents  = allNums.filter(n => !axisSet.has(n))
  const large      = combos.length > 10
  const name       = (n: number) => nameMap[String(n)] ?? null

  if (axisList.length > 0) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
        {/* 軸馬 */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap' }}>
          {axisList.map(ax => (
            <span key={ax} style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
              <AxisBadge />
              <span style={{
                fontFamily: 'monospace',
                fontWeight: 'bold',
                fontSize:   '1rem',
                color:      '#FFD700',
              }} title={name(ax) ?? undefined}>{ax}番</span>
              {name(ax) && (
                <span style={{ color: 'rgba(184,134,11,0.6)', fontSize: '0.7rem' }}>
                  {name(ax)}
                </span>
              )}
            </span>
          ))}
        </div>
        {/* 相手馬 */}
        {large ? (
          <span style={{ color: 'rgba(255,255,255,0.45)', fontSize: '0.7rem', fontFamily: 'monospace' }}>
            相手 {opponents.join('·')}（{combos.length}点）
          </span>
        ) : (
          <span style={{ color: accentColor, fontSize: '0.75rem', fontFamily: 'monospace', opacity: 0.8 }}>
            {combos.map(c => c.join(sep)).join(' / ')}
          </span>
        )}
      </div>
    )
  }

  // 軸なし
  return (
    <span style={{ color: accentColor, fontSize: '0.82rem', fontFamily: 'monospace' }}>
      {large
        ? `${combos.length}点`
        : combos.map(c => c.join(sep)).join(' / ')}
    </span>
  )
}

// ── メインコンポーネント ──────────────────────────────────────────────────

export default function GachiHits({ data }: Props) {
  const hits    = data.filter(d => d.is_hit === 1)
  const nonHits = data.filter(d => d.is_hit !== 1).slice(0, 20)

  return (
    <div className="space-y-4 p-4 w-full min-w-0">
      {/* ヒットセクションヘッダー */}
      <div className="flex items-center gap-2 mb-2">
        <span className="text-xl">🎯</span>
        <h2 className="text-lg font-bold"
            style={{ color: 'var(--neon-gold)' }}>
          AIガチ予想・的中実績
        </h2>
        {hits.length > 0 && (
          <span className="ml-auto text-sm font-bold px-2 py-1 rounded"
                style={{ background: 'rgba(184,134,11,0.15)', color: 'var(--neon-gold)', border: '1px solid rgba(184,134,11,0.3)' }}>
            {hits.length}件的中
          </span>
        )}
      </div>

      {hits.length === 0 && (
        <div className="text-center py-6 text-[var(--text-secondary)]">
          まだ的中実績がありません
        </div>
      )}

      {hits.map((h, idx) => {
        const style    = RANK_STYLE[h.rank ?? 'C'] ?? RANK_STYLE.C
        const combos   = parseCombos(h.combination_json)
        const nameMap  = h.horse_num_to_name ?? {}
        const isOrdered = ORDERED_TYPES.has(h.bet_type)
        const sep       = isOrdered ? '→' : '-'

        // 軸馬（マルチ表示用）
        const axisList = inferAxis(combos)
        const axisSet  = new Set(axisList)
        const allNums  = allUniqueNums(combos)
        const opponents = allNums.filter(n => !axisSet.has(n))
        const large    = combos.length > 10

        // notes から的中コンボを推定（"14→3→11" 形式のみ対応）
        const notesComboMatch = h.notes
          ? h.notes.match(/(\d+[→-]\d+[→-]\d+|\d+[→-]\d+)/)
          : null
        const notesCombo = notesComboMatch ? notesComboMatch[0] : null

        return (
          <div key={idx} className="rounded-lg p-4 relative overflow-hidden"
               style={{ background: style.bg, border: `1px solid ${style.border}` }}>
            {/* キラキラエフェクト */}
            <div className="absolute inset-0 pointer-events-none"
                 style={{
                   background: `radial-gradient(ellipse at top left, ${style.border} 0%, transparent 60%)`,
                   opacity: 0.3,
                 }} />

            <div className="relative flex flex-col sm:flex-row items-start justify-between gap-3 sm:gap-4">
              {/* 左: レース情報 + 買い目 */}
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1.5">
                  <span className="text-sm font-bold px-2 py-0.5 rounded"
                        style={{ background: style.bg, color: style.text, border: `1px solid ${style.border}` }}>
                    {style.label}
                  </span>
                  <span className="text-xs text-[var(--text-secondary)]">{h.date}</span>
                </div>

                <div className="font-bold text-base mb-0.5" style={{ color: 'var(--text-primary)' }}>
                  {h.race_name}
                </div>
                <div className="text-xs text-[var(--text-secondary)] mb-2">
                  {h.venue}
                  {h.surface ? `　${h.surface}` : ''}
                  {h.distance ? `${h.distance}m` : ''}
                  {' ／ '}
                  {h.model_type}{'　'}
                  <span className="font-semibold" style={{ color: style.text }}>
                    {h.bet_form ?? h.bet_type}
                    {h.n_tickets > 0 ? `（${h.n_tickets}点）` : ''}
                  </span>
                </div>

                {/* 買い目表示（軸バッジ付き） */}
                <ComboBadge
                  combos={combos}
                  betType={h.bet_type}
                  nameMap={nameMap}
                  accentColor={style.text}
                />

                {/* 的中コンボ（notes から推定 or コンボが1点の場合） */}
                {combos.length === 1 && (
                  <div style={{ marginTop: 8 }}>
                    <div style={{ marginBottom: 4 }}>
                      <span style={{
                        display:   'inline-block',
                        background: '#FFD700',
                        color:      '#000',
                        borderRadius: 3,
                        padding:    '0 5px',
                        fontSize:   '0.6rem',
                        fontWeight: 900,
                      }}>🏆 的中コンボ</span>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
                      {combos[0].map((num, i) => (
                        <span key={i} style={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                          <MedalCircle num={num} pos={i} isOrdered={isOrdered} />
                          {i < combos[0].length - 1 && (
                            <span style={{ color: 'rgba(184,134,11,0.45)', fontSize: '0.7rem' }}>{sep}</span>
                          )}
                        </span>
                      ))}
                      {combos[0].some(n => nameMap[String(n)]) && (
                        <span style={{ color: 'rgba(184,134,11,0.55)', fontSize: '0.65rem', marginLeft: 4 }}>
                          {combos[0].map(n => nameMap[String(n)] ?? String(n)).join(sep)}
                        </span>
                      )}
                    </div>
                  </div>
                )}
              </div>

              {/* 右: 払戻・ROI */}
              <div className="flex-shrink-0 sm:text-right">
                <div className="text-xl sm:text-2xl font-black font-mono"
                     style={{ color: style.text }}>
                  ¥{h.payout.toLocaleString()}
                </div>
                <div className="text-xs text-[var(--text-secondary)] mt-1">払戻</div>
                {h.n_tickets > 0 && (
                  <div className="text-xs mt-0.5" style={{ color: 'var(--text-secondary)' }}>
                    {(() => {
                      const invested = h.n_tickets * 100
                      const roi      = h.payout > 0 ? Math.round(h.payout / invested * 100) : 0
                      return (
                        <>
                          投資 ¥{invested.toLocaleString()}（100円×{h.n_tickets}点）
                          {' / ROI '}
                          <span style={{ color: roi >= 200 ? 'var(--neon-gold)' : 'var(--neon-green)' }}>
                            {roi.toLocaleString()}%
                          </span>
                        </>
                      )
                    })()}
                  </div>
                )}
              </div>
            </div>
          </div>
        )
      })}

      {/* 未的中分（最近20件） */}
      {nonHits.length > 0 && (
        <>
          <div className="text-xs text-[var(--text-secondary)] mt-4 mb-1">最近の予想（未的中）</div>
          <div className="space-y-1">
            {nonHits.map((h, idx) => {
              const combos  = parseCombos(h.combination_json)
              const nameMap = h.horse_num_to_name ?? {}
              const isOrdered = ORDERED_TYPES.has(h.bet_type)
              const sep = isOrdered ? '→' : '-'
              const axisList = inferAxis(combos)
              const allNums  = allUniqueNums(combos)
              const opponents = allNums.filter(n => !new Set(axisList).has(n))
              const large    = combos.length > 10
              const axisStr  = axisList.length > 0
                ? `軸${axisList.join('·')}→相手${opponents.join('·')}`
                : (large ? `${combos.length}点` : combos.map(c => c.join(sep)).join(' / '))

              return (
                <div key={idx} className="flex items-center gap-3 px-3 py-2 rounded text-xs"
                     style={{ background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.06)' }}>
                  <span className="text-[var(--text-secondary)] font-mono w-24 flex-shrink-0">{h.date}</span>
                  <span className="font-semibold flex-1 truncate">{h.race_name}</span>
                  <span className="text-[var(--text-secondary)] flex-shrink-0">{h.bet_type}</span>
                  <span className="font-mono text-[var(--text-secondary)] flex-shrink-0">
                    {axisStr.slice(0, 28)}
                  </span>
                </div>
              )
            })}
          </div>
        </>
      )}
    </div>
  )
}
