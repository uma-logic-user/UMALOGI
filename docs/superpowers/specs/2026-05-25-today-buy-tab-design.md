# 当日購入タブ 実装設計書

## Goal

各レース詳細画面に「当日購入」タブを追加し、AI のケリー基準計算に基づく「具体的な購入金額（円単位）」を一覧表示する。ユーザーが当日に何をいくらで買えばよいかを即座に把握できるようにする。

## Architecture

バックエンド変更なし。既存の `predictions` データ（`expected_value`, `win_odds`, `recommended_bet`）を使い、フロントエンドのみでケリー計算を完結させる。

```
RaceDetail.tsx
 ├─ tab: 'today_buy'  (新規追加)
 └─ <TodayBuyPanel predictions={predictions} />
          ↓
    web/src/lib/kelly.ts  (calcKellyStake / calcKellyFraction)
```

## Tech Stack

- Next.js 14 App Router
- TypeScript
- Tailwind CSS 4.1（既存に合わせる）
- shadcn/ui なし

---

## Design Details

### 1. ケリー計算ユーティリティ `web/src/lib/kelly.ts`

```typescript
/** ケリー最適比率 f* = (EV - 1) / (odds - 1) */
export function calcKellyFraction(ev: number, odds: number): number {
  if (odds <= 1 || ev <= 0) return 0;
  return (ev - 1) / (odds - 1);
}

/**
 * 推奨購入金額を返す（100円単位切り捨て）
 * @param bankroll  総資金（円）
 * @param kellyFrac ケリー安全係数（デフォルト 0.25 = 1/4 Kelly）
 */
export function calcKellyStake(
  ev: number,
  odds: number,
  bankroll: number,
  kellyFrac: number = 0.25
): number {
  const f = calcKellyFraction(ev, odds);
  if (f <= 0) return 0;
  return Math.floor((bankroll * kellyFrac * f) / 100) * 100;
}
```

### 2. 新規コンポーネント `web/src/components/TodayBuyPanel.tsx`

**Props:**
```typescript
interface TodayBuyPanelProps {
  predictions: Prediction[];  // 既存型を流用
}
```

**UI 構成:**

```
┌─────────────────────────────────────────┐
│ 総資金: [¥100,000 ▼]  ケリー係数: [0.25]  │  ← useState でリアルタイム再計算
├────────────────────────────────────────-┤
│ 買い目         オッズ   EV    推奨購入額  │
│ 単勝 3番        5.2倍  1.35  ¥2,500     │  ← EV>=1.0 & stake>0
│ 複勝 7番        2.1倍  1.12  ¥1,100     │
│ 三連複 3-7-1   45.0倍  0.87  購入見送り  │  ← EV<1.0 → グレー表示
├─────────────────────────────────────────┤
│ 合計推奨購入額                   ¥3,600  │
└─────────────────────────────────────────┘
```

**表示ルール:**
- `ev >= 1.0` かつ `stake > 0` → 通常行（白背景）
- `ev < 1.0` または `stake === 0` → グレー行、「購入見送り」表示
- 合計は stake > 0 の行のみを合算
- バンクロール・係数変更時はリアルタイムで全行を再計算（useEffect 不要、useMemo で対応）

### 3. `web/src/components/RaceDetail.tsx` への変更

**変更箇所のみ（最小変更）:**

```typescript
// 1. Tab 型に追加
type Tab = 'race_card' | 'results' | 'predictions' | 'payouts' | 'today_buy';

// 2. タブボタン追加（既存パターンに従う）
<button onClick={() => setTab('today_buy')} ...>当日購入</button>

// 3. パネル表示
{tab === 'today_buy' && <TodayBuyPanel predictions={predictions} />}
```

### 4. バックアップ

作業前に以下を実行:
```bash
mkdir -p .backup
cp web/src/components/RaceDetail.tsx .backup/RaceDetail.tsx.bak
```

---

## Data Flow

```
/api/races/[race_id]
  └─ predictions: Prediction[]
       ├─ bet_type        → 「買い目」列
       ├─ combination_json → 馬番テキスト生成
       ├─ win_odds         → 「オッズ」列
       ├─ expected_value   → 「EV」列 + ケリー計算入力
       └─ (stake)         → calcKellyStake() で算出
```

## Error Handling

- `win_odds` が null / 0 → ケリー計算スキップ、「データなし」表示
- `expected_value` が null → 0 扱い（購入見送り）
- 全件購入見送りの場合 → 「本日の推奨購入なし」メッセージを表示

## Testing

1. `kelly.ts` ユニットテスト（`web/src/__tests__/kelly.test.ts`）:
   - EV=1.35, odds=5.2 → f*≈0.0875, stake(bankroll=100000, frac=0.25)=2100
   - EV=0.87 → f*<0 → stake=0
   - odds=1.0 → stake=0（ゼロ除算防止）

2. コンポーネント動作確認:
   - バンクロール変更時にテーブル金額が即座に更新されること
   - 全件見送り時にメッセージが表示されること
