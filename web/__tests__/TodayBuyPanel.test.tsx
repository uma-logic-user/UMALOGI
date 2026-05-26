/**
 * TodayBuyPanel ユニットテスト
 *
 * 検証対象:
 *  1. is_hit=null のみ → サマリーカード非表示
 *  2. 確定済み行混在 → 投資額・払い戻し・回収率の正確な計算
 *  3. 的中行のCSSハイライト / 外れ行のグレーダウン
 */
import React from 'react'
import { render, screen, within } from '@testing-library/react'
import { TodayBuyPanel } from '@/components/TodayBuyPanel'
import type { RacePrediction } from '@/types/race'

// ── ヘルパー ───────────────────────────────────────────────────────────

/** 最小限のRacePrediction モックファクトリ */
function makePred(overrides: Partial<RacePrediction> & { prediction_id: string }): RacePrediction {
  return {
    model_type:        '本命',
    bet_type:          '単勝',
    bet_form:          'single',
    n_tickets:         1,
    confidence:        0.6,
    expected_value:    1.5,
    recommended_bet:   1000,
    combination_json:  '[5]',
    notes:             null,
    created_at:        '2026-05-26T09:00:00',
    is_hit:            null,
    payout:            null,
    profit:            null,
    roi:               null,
    horses:            [],
    horse_num_to_name: {},
    ...overrides,
  }
}

/**
 * winOddsMap: 馬番5 → 3.5倍
 *
 * Kelly stake 計算（デフォルト bankroll=100,000, kellyFrac=0.25, ev=1.5, odds=3.5）:
 *   f* = (1.5-1)/(3.5-1) = 0.2
 *   stake = floor(100_000 * 0.25 * 0.2 / 100) * 100 = 5,000
 */
const WIN_ODDS_MAP: Record<number, number> = { 5: 3.5, 7: 2.0 }
const STAKE_PER_ROW = 5_000  // ev=1.5, odds=3.5, bankroll=100,000, 1/4 Kelly

/** サマリーカード要素を取得するヘルパー */
function getSummaryCard() {
  const label = screen.getByText('合計投資額')
  // "合計投資額" p → 親 div → 親コンテナ div（grid）
  return label.closest('div')!.parentElement! as HTMLElement
}

/** サマリーカード内の値テキスト（ラベルの直後 p）を返す */
function getSummaryValue(labelText: string): string | null {
  const label = screen.getByText(labelText)
  const container = label.closest('div')!
  const valuePara = container.querySelector('p:last-child')
  return valuePara?.textContent ?? null
}

// ── テスト 1: サマリーカード表示制御 ─────────────────────────────────

describe('サマリーカード表示制御', () => {
  test('全予想が is_hit=null の場合、サマリーカードは表示されない', () => {
    const preds = [
      makePred({ prediction_id: 'p1', is_hit: null }),
      makePred({ prediction_id: 'p2', is_hit: null, bet_type: '複勝', combination_json: '[7]' }),
    ]

    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)

    expect(screen.queryByText('合計投資額')).toBeNull()
    expect(screen.queryByText('合計払い戻し額')).toBeNull()
    expect(screen.queryByText('回収率')).toBeNull()
  })

  test('予想データが0件の場合、「予想データがありません」メッセージを表示する', () => {
    render(<TodayBuyPanel predictions={[]} winOddsMap={{}} />)
    expect(screen.getByText('このレースの予想データがありません')).toBeInTheDocument()
  })

  test('is_hit が1件でも確定していればサマリーカードが表示される', () => {
    const preds = [
      makePred({ prediction_id: 'hit', is_hit: 1, payout: 2000 }),
      makePred({ prediction_id: 'pend', is_hit: null }),
    ]

    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)

    expect(screen.getByText('合計投資額')).toBeInTheDocument()
    expect(screen.getByText('合計払い戻し額')).toBeInTheDocument()
    expect(screen.getByText('回収率')).toBeInTheDocument()
  })
})

// ── テスト 2: サマリー KPI 計算 ──────────────────────────────────────

describe('サマリーKPI計算（確定済み行あり）', () => {
  test('的中1件・外れ1件の場合、投資額・払い戻し・回収率が正しく表示される', () => {
    const preds = [
      makePred({ prediction_id: 'hit1', expected_value: 1.5, is_hit: 1, payout: 3_000 }),
      makePred({ prediction_id: 'miss1', expected_value: 1.5, is_hit: 0, payout: null }),
    ]

    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)

    // サマリーカード表示確認
    expect(screen.getByText('合計投資額')).toBeInTheDocument()

    // 合計投資額 = 確定2行 × 5,000 = 10,000
    expect(getSummaryValue('合計投資額')).toBe(`¥${(STAKE_PER_ROW * 2).toLocaleString()}`)

    // 合計払い戻し = 的中1件のpayout
    expect(getSummaryValue('合計払い戻し額')).toBe('¥3,000')

    // 回収率 = 3000 / 10000 * 100 = 30.0%
    const roi = (3_000 / (STAKE_PER_ROW * 2)) * 100
    expect(getSummaryValue('回収率')).toBe(`${roi.toFixed(1)}%`)
  })

  test('全件外れの場合、払い戻しは¥0・回収率は0.0%を表示する', () => {
    const preds = [
      makePred({ prediction_id: 'miss1', expected_value: 1.5, is_hit: 0, payout: null }),
      makePred({ prediction_id: 'miss2', expected_value: 1.5, is_hit: 0, payout: null }),
    ]

    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)

    expect(getSummaryValue('合計払い戻し額')).toBe('¥0')
    expect(getSummaryValue('回収率')).toBe('0.0%')
  })

  test('回収率 ≥ 100% の場合は緑色テキストが適用される', () => {
    // payout 20,000 / invest 5,000 → ROI=400% > 100
    const preds = [
      makePred({ prediction_id: 'bigwin', expected_value: 1.5, is_hit: 1, payout: 20_000 }),
    ]
    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)

    const roiLabel = screen.getByText('回収率').closest('div')!
    const roiValue = roiLabel.querySelector('p:last-child')!
    expect(roiValue.className).toContain('text-green-400')
  })

  test('回収率 < 100% の場合は赤色テキストが適用される', () => {
    const preds = [
      makePred({ prediction_id: 'miss', expected_value: 1.5, is_hit: 0, payout: null }),
    ]
    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)

    const roiLabel = screen.getByText('回収率').closest('div')!
    const roiValue = roiLabel.querySelector('p:last-child')!
    expect(roiValue.className).toContain('text-red-400')
  })

  test('is_hit=null の行は投資額の計算に含まれない', () => {
    const preds = [
      makePred({ prediction_id: 'hit1', expected_value: 1.5, is_hit: 1, payout: 2_000 }),
      makePred({ prediction_id: 'pend', is_hit: null }),  // 未確定 → 除外
    ]

    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)

    // 投資額 = 確定済み1行分のみ = 5,000
    expect(getSummaryValue('合計投資額')).toBe(`¥${STAKE_PER_ROW.toLocaleString()}`)
  })

  test('単勝以外(noOdds=true)の確定行は投資額が0なので「—」を表示する', () => {
    const preds = [
      makePred({
        prediction_id:   'hit-fukusho',
        bet_type:         '複勝',    // noOdds=true → stake=0
        combination_json: '[7]',
        expected_value:   1.5,
        is_hit:           1,
        payout:           500,
      }),
    ]

    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)

    // confirmedInvest=0 → "—"
    expect(getSummaryValue('合計投資額')).toBe('—')
    // payout=500 → ¥500
    expect(getSummaryValue('合計払い戻し額')).toBe('¥500')
    // roi=null (invest=0) → "—"
    expect(getSummaryValue('回収率')).toBe('—')
  })
})

// ── テスト 3: 条件付き行スタイリング ─────────────────────────────────

describe('行スタイリング（的中 / 外れ / 未確定）', () => {
  function renderAndGetRow(overrides: Partial<RacePrediction>): Element {
    const preds = [makePred({ prediction_id: 'r1', ...overrides })]
    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)
    return document.querySelector('tbody tr')!
  }

  test('is_hit=1 の行には的中ハイライトクラス（薄い赤）が付く', () => {
    const row = renderAndGetRow({ is_hit: 1, payout: 2000, expected_value: 1.5 })
    expect(row.className).toContain('bg-[rgba(220,80,80,0.15)]')
    expect(row.className).not.toContain('opacity-40')
  })

  test('is_hit=0 の行には opacity-40 クラスが付く（グレーダウン）', () => {
    const row = renderAndGetRow({ is_hit: 0, payout: null, expected_value: 1.5 })
    expect(row.className).toContain('opacity-40')
    expect(row.className).not.toContain('bg-[rgba(220,80,80,0.15)]')
  })

  test('is_hit=null (未確定) の行にはハイライトもグレーダウンも付かない', () => {
    const row = renderAndGetRow({ is_hit: null, expected_value: 1.5 })
    expect(row.className).not.toContain('bg-[rgba(220,80,80,0.15)]')
    expect(row.className).not.toContain('opacity-40')
    // EV≥1.0 かつ stake>0 → hover クラスが付く
    expect(row.className).toContain('hover:bg-')
  })

  test('的中行の「結果」列に「✓ 的中」と払い戻し額が表示される', () => {
    const preds = [
      makePred({ prediction_id: 'hit', is_hit: 1, payout: 7_800, expected_value: 1.5 }),
    ]
    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)
    expect(screen.getByText(/✓ 的中/)).toBeInTheDocument()
    // 払い戻し額はサマリーカードと結果列の両方に出るため tbody 内に絞る
    const tbody = document.querySelector('tbody')!
    expect(within(tbody as HTMLElement).getByText(/¥7,800/)).toBeInTheDocument()
  })

  test('外れ行の「結果」列に「✗ 外れ」が表示される', () => {
    const preds = [
      makePred({ prediction_id: 'miss', is_hit: 0, payout: null, expected_value: 1.5 }),
    ]
    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)
    expect(screen.getByText('✗ 外れ')).toBeInTheDocument()
  })

  test('is_hit=null のみの場合、thead に「結果」列ヘッダーが表示されない', () => {
    const preds = [makePred({ prediction_id: 'pend', is_hit: null })]
    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)
    const thead = document.querySelector('thead')!
    expect(within(thead).queryByText('結果')).toBeNull()
  })

  test('確定済み行がある場合、thead に「結果」列ヘッダーが表示される', () => {
    const preds = [
      makePred({ prediction_id: 'hit', is_hit: 1, payout: 1_000, expected_value: 1.5 }),
    ]
    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)
    const thead = document.querySelector('thead')!
    expect(within(thead).getByText('結果')).toBeInTheDocument()
  })
})

// ── テスト 4: オッズ確認要・購入見送りの表示 ─────────────────────────

describe('単勝以外のオッズ確認要表示', () => {
  test('bet_type=複勝 の行は「オッズ確認要」と表示する', () => {
    const preds = [makePred({ prediction_id: 'f1', bet_type: '複勝', is_hit: null })]
    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)
    expect(screen.getByText('オッズ確認要')).toBeInTheDocument()
  })

  test('bet_type=馬連 の行も「オッズ確認要」と表示する', () => {
    const preds = [
      makePred({ prediction_id: 'ur', bet_type: '馬連', combination_json: '[[5,7]]', is_hit: null }),
    ]
    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)
    expect(screen.getByText('オッズ確認要')).toBeInTheDocument()
  })

  test('単勝で EV<1.0 の行は「購入見送り」と表示する', () => {
    const preds = [
      makePred({ prediction_id: 'low', bet_type: '単勝', expected_value: 0.8, is_hit: null }),
    ]
    render(<TodayBuyPanel predictions={preds} winOddsMap={WIN_ODDS_MAP} />)
    expect(screen.getByText('購入見送り')).toBeInTheDocument()
  })
})
