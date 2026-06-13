import { classifyTrack } from '@/lib/dbHelpers'

describe('classifyTrack — SNS / ガチ ハイブリッド振り分け', () => {
  test('_v0525 シャドーは券種によらず常に SNS', () => {
    expect(classifyTrack('本命_v0525(再計算)', '単勝')).toBe('sns')
    expect(classifyTrack('卍_v0525(再計算)', '複勝')).toBe('sns')
    expect(classifyTrack('HitFocus_v0525(再計算)', '三連単')).toBe('sns')
  })

  test('ライブモデルの単勝・複勝は実弾ガチ', () => {
    expect(classifyTrack('本命(直前)', '単勝')).toBe('gachi')
    expect(classifyTrack('卍(直前)', '複勝')).toBe('gachi')
    expect(classifyTrack('Pure_EV_Edge(直前)', '単勝')).toBe('gachi')
    expect(classifyTrack('FukushoElite(直前)', '複勝')).toBe('gachi')
  })

  test('ライブモデルの多券種（馬連/馬単/ワイド/三連複/三連単）は SNS', () => {
    expect(classifyTrack('本命(直前)', '馬連')).toBe('sns')
    expect(classifyTrack('卍(直前)', '三連複')).toBe('sns')
    expect(classifyTrack('HitFocus', '三連単')).toBe('sns')
    expect(classifyTrack('Oracle', 'ワイド')).toBe('sns')
    expect(classifyTrack('本命(直前)', '馬単')).toBe('sns')
  })

  test('null / undefined / 空文字でも例外なく gachi 判定に倒れない（多券種は sns）', () => {
    expect(classifyTrack('', '単勝')).toBe('gachi')
    expect(classifyTrack(undefined as unknown as string, '複勝')).toBe('gachi')
    expect(classifyTrack(null as unknown as string, '三連複')).toBe('sns')
  })
})
