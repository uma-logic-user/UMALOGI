/**
 * TodayBuyPanel リアルタイムオッズポーリング テスト
 *
 * 検証対象:
 *  1. raceId が渡された場合、マウント時に /api/realtime-odds/{raceId} をフェッチする
 *  2. ライブオッズが静的 winOddsMap を上書きして EV/推奨額が再計算される
 *  3. raceId が未指定の場合はフェッチしない
 *  4. API が空の odds を返しても静的 winOddsMap を壊さない
 *  5. pollingIntervalMs 経過後に再フェッチが走る
 *  6. アンマウント時にポーリングが停止する（clearInterval が呼ばれる）
 */
import React from 'react'
import { render, screen, act, waitFor } from '@testing-library/react'
import { TodayBuyPanel } from '@/components/TodayBuyPanel'
import type { RacePrediction } from '@/types/race'

// ── ヘルパー ───────────────────────────────────────────────────────────

function makePred(overrides: Partial<RacePrediction> & { prediction_id: number }): RacePrediction {
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
    created_at:        '2026-05-31T09:00:00',
    is_hit:            null,
    payout:            null,
    profit:            null,
    roi:               null,
    horses:            [],
    horse_num_to_name: {},
    ...overrides,
  }
}

const BASE_WIN_ODDS_MAP = { 5: 3.5 }

// レスポンスビルダー
function makeOddsResponse(winOdds: number, updatedAt: string = '2026-05-31 14:46:00') {
  return {
    race_id: '202605310511',
    updated_at: updatedAt,
    odds: {
      '5': { horse_name: 'テスト馬A', win_odds: winOdds, place_odds_min: 1.2, place_odds_max: 1.6, popularity: 1 },
    },
  }
}

// ── テスト 1: マウント時にフェッチする ───────────────────────────────

describe('リアルタイムオッズポーリング', () => {
  let fetchMock: jest.Mock

  beforeEach(() => {
    fetchMock = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => makeOddsResponse(3.5),
    })
    global.fetch = fetchMock
    jest.useFakeTimers()
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  test('raceId が渡された場合、マウント時に API をフェッチする', async () => {
    const preds = [makePred({ prediction_id: 1 })]
    render(
      <TodayBuyPanel
        predictions={preds}
        winOddsMap={BASE_WIN_ODDS_MAP}
        raceId="202605310511"
      />
    )

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith('/api/realtime-odds/202605310511')
    })
  })

  test('raceId が未指定の場合、フェッチしない', async () => {
    const preds = [makePred({ prediction_id: 2 })]
    render(
      <TodayBuyPanel
        predictions={preds}
        winOddsMap={BASE_WIN_ODDS_MAP}
      />
    )

    // 少し待ってもフェッチされないことを確認
    await act(async () => { jest.advanceTimersByTime(200) })
    expect(fetchMock).not.toHaveBeenCalled()
  })

  test('ライブオッズ取得後にオッズ更新タイムスタンプが表示される', async () => {
    const preds = [makePred({ prediction_id: 3 })]
    render(
      <TodayBuyPanel
        predictions={preds}
        winOddsMap={BASE_WIN_ODDS_MAP}
        raceId="202605310511"
      />
    )

    await waitFor(() => {
      expect(screen.getByText(/オッズ更新/)).toBeInTheDocument()
    })
  })

  test('raceId を渡すと「オッズ取得中」状態が初期表示される', () => {
    const preds = [makePred({ prediction_id: 4 })]

    // fetch をまだ解決させない
    fetchMock.mockReturnValue(new Promise(() => {}))

    render(
      <TodayBuyPanel
        predictions={preds}
        winOddsMap={BASE_WIN_ODDS_MAP}
        raceId="202605310511"
      />
    )

    expect(screen.getByText(/オッズ取得中/)).toBeInTheDocument()
  })

  test('pollingIntervalMs 経過後に再フェッチが走る', async () => {
    const preds = [makePred({ prediction_id: 5 })]
    render(
      <TodayBuyPanel
        predictions={preds}
        winOddsMap={BASE_WIN_ODDS_MAP}
        raceId="202605310511"
        pollingIntervalMs={5_000}
      />
    )

    // 初回フェッチ
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))

    // 5秒後に2回目
    await act(async () => { jest.advanceTimersByTime(5_000) })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
  })

  test('アンマウント時にポーリングが停止する', async () => {
    const preds = [makePred({ prediction_id: 6 })]
    const { unmount } = render(
      <TodayBuyPanel
        predictions={preds}
        winOddsMap={BASE_WIN_ODDS_MAP}
        raceId="202605310511"
        pollingIntervalMs={5_000}
      />
    )

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))

    unmount()

    // アンマウント後に時間を進めてもフェッチが増えない
    await act(async () => { jest.advanceTimersByTime(10_000) })
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  test('API が空の odds を返しても静的 winOddsMap を壊さない', async () => {
    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({ race_id: '202605310511', updated_at: null, odds: {} }),
    } as Response)

    const preds = [makePred({ prediction_id: 7, expected_value: 1.5 })]
    render(
      <TodayBuyPanel
        predictions={preds}
        winOddsMap={{ 5: 3.5 }}
        raceId="202605310511"
      />
    )

    await waitFor(() => expect(fetchMock).toHaveBeenCalled())

    // 静的オッズ 3.5倍 → stake が表示されているはず（EV=1.5 で計算可能）
    expect(screen.getAllByText(/¥/).length).toBeGreaterThan(0)
  })

  test('ライブオッズが静的 winOddsMap より優先される', async () => {
    // ライブオッズ 5.0倍（静的は 3.5倍）→ Kelly stake が変わる
    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => makeOddsResponse(5.0),
    } as Response)

    const preds = [makePred({ prediction_id: 8, expected_value: 1.5 })]
    render(
      <TodayBuyPanel
        predictions={preds}
        winOddsMap={{ 5: 3.5 }}   // 静的: 3.5倍
        raceId="202605310511"
      />
    )

    await waitFor(() => {
      // odds 5.0 → f* = (1.5-1)/(5.0-1) = 0.125
      // stake = floor(100000 × 0.25 × 0.125 / 100) × 100 = 3,100
      const oddsCell = screen.getByText('5.0倍')
      expect(oddsCell).toBeInTheDocument()
    })
  })

  test('API が 200 以外を返した場合は静的オッズを維持する', async () => {
    fetchMock.mockResolvedValue({
      ok: false,
      status: 500,
    } as Response)

    const preds = [makePred({ prediction_id: 9, expected_value: 1.5 })]
    render(
      <TodayBuyPanel
        predictions={preds}
        winOddsMap={{ 5: 3.5 }}
        raceId="202605310511"
      />
    )

    await waitFor(() => expect(fetchMock).toHaveBeenCalled())

    // 静的オッズが生きている（¥5,000 が複数箇所に表示される）
    expect(screen.getAllByText('¥5,000').length).toBeGreaterThan(0)
  })
})
