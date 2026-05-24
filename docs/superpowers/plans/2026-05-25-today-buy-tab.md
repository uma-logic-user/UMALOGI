# 当日購入タブ実装プラン

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 各レース詳細画面に「当日購入」タブを追加し、ケリー基準で算出した推奨購入金額（円単位）を一覧表示する。

**Architecture:** フロントエンド完結。既存の `predictions[].expected_value` と `win_odds` から `f* = (EV-1)/(odds-1)` を計算し、バンクロール×ケリー係数×f* を100円単位で切り捨てた金額を表示する。バックエンド変更なし。

**Tech Stack:** Next.js 14 App Router, TypeScript, Tailwind CSS 4.1, Jest（既存テスト環境）

---

## ファイル構成

| ファイル | 種別 | 役割 |
|---------|------|------|
| `.backup/RaceDetail.tsx.bak` | バックアップ | 作業前退避 |
| `web/src/lib/kelly.ts` | 新規 | calcKellyFraction / calcKellyStake |
| `web/src/__tests__/kelly.test.ts` | 新規 | kelly.ts のユニットテスト |
| `web/src/components/TodayBuyPanel.tsx` | 新規 | 当日購入タブのUIコンポーネント |
| `web/src/components/RaceDetail.tsx` | 変更 | Tab型追加・タブボタン・パネル呼び出し |
| `docs/4_ui_design.md` | 変更 | Changelog追記 |

---

### Task 1: バックアップと環境確認

**Files:**
- Read: `web/src/components/RaceDetail.tsx`（現在の Tab 型・タブボタン行を確認）
- Create: `.backup/RaceDetail.tsx.bak`

- [ ] **Step 1: RaceDetail.tsx の Tab 型とタブボタン実装行を確認する**

```bash
grep -n "type Tab\|today_buy\|tab ==\|setTab\|TabButton\|当日" web/src/components/RaceDetail.tsx | head -30
```

Expected: `type Tab = 'race_card' | 'results' | 'predictions' | 'payouts'` が見つかる。

- [ ] **Step 2: バックアップを作成する**

```bash
mkdir -p .backup
cp web/src/components/RaceDetail.tsx .backup/RaceDetail.tsx.bak
echo "バックアップ完了: .backup/RaceDetail.tsx.bak"
```

- [ ] **Step 3: Commit**

```bash
git add .backup/RaceDetail.tsx.bak
git commit -m "chore: RaceDetail.tsx バックアップ作成（当日購入タブ実装前）"
```

---

### Task 2: kelly.ts ユーティリティ（TDD）

**Files:**
- Create: `web/src/lib/kelly.ts`
- Create: `web/src/__tests__/kelly.test.ts`

- [ ] **Step 1: テストファイルを書く（先にテスト）**

`web/src/__tests__/kelly.test.ts` を作成:

```typescript
import { calcKellyFraction, calcKellyStake } from '../lib/kelly';

describe('calcKellyFraction', () => {
  it('EV=1.35, odds=5.2 → f*≈0.0875', () => {
    const f = calcKellyFraction(1.35, 5.2);
    expect(f).toBeCloseTo(0.0875, 3);
  });

  it('EV<1.0 → 負の値（0以下）', () => {
    const f = calcKellyFraction(0.87, 5.2);
    expect(f).toBeLessThanOrEqual(0);
  });

  it('EV=1.0 → 0', () => {
    expect(calcKellyFraction(1.0, 5.0)).toBeCloseTo(0, 6);
  });

  it('odds=1.0 → ゼロ除算防止で0', () => {
    expect(calcKellyFraction(1.5, 1.0)).toBe(0);
  });

  it('odds<=0 → 0', () => {
    expect(calcKellyFraction(1.5, 0)).toBe(0);
  });

  it('ev<=0 → 0', () => {
    expect(calcKellyFraction(0, 5.0)).toBe(0);
  });
});

describe('calcKellyStake', () => {
  it('EV=1.35, odds=5.2, bankroll=100000, frac=0.25 → 100円単位', () => {
    // f*=0.0875, stake=100000×0.25×0.0875=2187.5 → floor/100*100=2100
    const stake = calcKellyStake(1.35, 5.2, 100000, 0.25);
    expect(stake).toBe(2100);
    expect(stake % 100).toBe(0);
  });

  it('EV<1.0 → 0円（購入見送り）', () => {
    expect(calcKellyStake(0.87, 5.2, 100000)).toBe(0);
  });

  it('EV=1.0 → 0円', () => {
    expect(calcKellyStake(1.0, 5.0, 100000)).toBe(0);
  });

  it('bankroll変更で金額がスケールする', () => {
    const s1 = calcKellyStake(1.35, 5.2, 100000);
    const s2 = calcKellyStake(1.35, 5.2, 200000);
    expect(s2).toBeGreaterThan(s1);
  });

  it('kellyFrac=0 → 0円', () => {
    expect(calcKellyStake(1.35, 5.2, 100000, 0)).toBe(0);
  });
});
```

- [ ] **Step 2: テストが失敗することを確認する**

```bash
cd web && npx jest --testPathPattern="kelly.test" --no-coverage 2>&1 | tail -5
```

Expected: `Cannot find module '../lib/kelly'` というエラー。

- [ ] **Step 3: kelly.ts を実装する**

`web/src/lib/kelly.ts` を作成:

```typescript
/**
 * ケリー基準計算ユーティリティ
 *
 * EV = P(win) × odds なので P(win) = EV / odds
 * ケリー公式: f* = (b×p - q) / b = (EV - 1) / (odds - 1)
 */

/** ケリー最適比率 f* を返す。EV≤1.0 または odds≤1.0 の場合は 0 を返す。 */
export function calcKellyFraction(ev: number, odds: number): number {
  if (odds <= 1 || ev <= 0) return 0;
  return (ev - 1) / (odds - 1);
}

/**
 * 推奨購入金額を返す（100円単位切り捨て）。
 * f* ≤ 0 の場合（EV≤1.0）は 0 を返す（購入見送り）。
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

- [ ] **Step 4: テストが通ることを確認する**

```bash
cd web && npx jest --testPathPattern="kelly.test" --no-coverage 2>&1 | tail -10
```

Expected: `Tests: 11 passed, 11 total`

- [ ] **Step 5: Commit**

```bash
git add web/src/lib/kelly.ts web/src/__tests__/kelly.test.ts
git commit -m "feat: kelly.ts ケリー基準計算ユーティリティ + テスト追加"
```

---

### Task 3: TodayBuyPanel.tsx コンポーネント

**Files:**
- Read: `web/src/types/race.ts`（Prediction 型の全フィールドを確認）
- Create: `web/src/components/TodayBuyPanel.tsx`

- [ ] **Step 1: Prediction 型のフィールドを確認する**

```bash
grep -A 30 "interface Prediction\|type Prediction" web/src/types/race.ts | head -40
```

Expected: `expected_value`, `win_odds`, `bet_type`, `combination_json`, `recommended_bet` が含まれる。

- [ ] **Step 2: TodayBuyPanel.tsx を実装する**

`web/src/components/TodayBuyPanel.tsx` を作成。

- [ ] **Step 3: TypeScript コンパイルエラーがないことを確認する**

```bash
cd web && npx tsc --noEmit 2>&1 | grep "TodayBuyPanel\|kelly" | head -10
```

Expected: 出力なし（エラーなし）

- [ ] **Step 4: Commit**

```bash
git add web/src/components/TodayBuyPanel.tsx
git commit -m "feat: TodayBuyPanel コンポーネント追加（当日購入タブUI）"
```

---

### Task 4: RaceDetail.tsx にタブを追加

**Files:**
- Modify: `web/src/components/RaceDetail.tsx`

- [ ] **Step 1: Tab 型に 'today_buy' を追加する**

- [ ] **Step 2: TodayBuyPanel の import を追加する**

- [ ] **Step 3: タブボタンを追加する**

- [ ] **Step 4: パネルの表示条件を追加する**

- [ ] **Step 5: TypeScript コンパイルエラーがないことを確認する**

```bash
cd web && npx tsc --noEmit 2>&1 | head -20
```

- [ ] **Step 6: Jest テストが全件パスすることを確認する**

```bash
cd web && npx jest --no-coverage 2>&1 | tail -10
```

- [ ] **Step 7: Commit**

```bash
git add web/src/components/RaceDetail.tsx
git commit -m "feat: レース詳細画面に当日購入タブを追加（ケリー基準推奨購入額）"
```

---

### Task 5: ドキュメント更新

**Files:**
- Modify: `docs/4_ui_design.md`

- [ ] **Step 1: Changelog に追記する**

- [ ] **Step 2: Commit**

```bash
git add docs/4_ui_design.md
git commit -m "docs: 当日購入タブ実装のChangelog追記"
```
