# 設計ドキュメント: Claude風デザインシステムへの移植

**作成日**: 2026-05-27  
**ステータス**: 承認済み  
**対象**: `web/` ディレクトリ（Next.js 15 App Router + Tailwind v4）

---

## 概要

現在の UMALOGI Web UI（ダーク・ネオン系サイバーパンクデザイン）を、  
Claude の UI を参考にした **ウォームベージュ×セリフ体×ミニマル** なデザインシステムへ刷新する。

バックエンド（SQLite・API Routes・lib/ 層）は**完全に据え置き**、  
フロントエンドの見た目だけを入れ替える「デザイン移植」として実施する。

---

## デザイン仕様（承認済み）

### カラーパレット（Tailwind v4 `@theme` ブロック）

| トークン | 値 | 用途 |
|---|---|---|
| `--color-bg-base` | `#F9F8F6` | ページ背景 |
| `--color-bg-surface` | `#FDFCFB` | カード・パネル背景 |
| `--color-bg-muted` | `#F0EDE6` | サイドバー・ヘッダー背景 |
| `--color-text-primary` | `#2D2C28` | 見出し・メインテキスト |
| `--color-text-muted` | `#6B6960` | ラベル・補足テキスト |
| `--color-text-subtle` | `#9E9B93` | プレースホルダー |
| `--color-accent` | `#C8A882` | テラコッタアクセント（ブランドカラー） |
| `--color-accent-light` | `#FEF3E2` | アクセント薄（バッジ背景） |
| `--color-border` | `#E5E3DB` | ボーダー全般 |
| `--color-border-strong` | `#D4D0C8` | フォーカス・強調ボーダー |

### タイポグラフィ

| 用途 | フォント |
|---|---|
| AI見解・レース名・見出し | `Georgia, 'Times New Roman', serif`（セリフ体） |
| データ（オッズ・馬名・騎手） | `system-ui, -apple-system, sans-serif` |
| コード・数値 | `'SF Mono', 'Fira Code', monospace` |

### スペーシング・シェイプ

- カード角丸: `rounded-xl`（14px）
- ボーダー: `1px solid var(--color-border)`
- シャドウ: `shadow-sm`（`0 1px 4px rgba(45,44,40,0.06)`）
- パネル内パディング: `p-5`（20px）

---

## 対象コンポーネントと変更方針

### 変更対象（フロントエンドのみ）

| コンポーネント | 変更方針 |
|---|---|
| `globals.css` | `@theme` ブロックにカラー変数定義。ネオン系変数・グリッド背景を削除 |
| `NavBar.tsx` | 背景をウォームベージュに変更。ロゴにセリフ体適用 |
| `AppShell.tsx` | 全体レイアウト背景色・サイドバー色を更新 |
| `RaceDetail.tsx` | レースバナー（濃色）・タブUI・AI分析パネルを新デザインに |
| `PredictionsPanel.tsx` | AI予想カードを「バブル風」カードUIに変更。EV表示を強調 |
| `RaceTable.tsx` | テーブルのボーダー・ホバー色を新カラーに |
| `StatCards.tsx` | カード背景・アクセントカラーを更新 |
| `TodayBuyPanel.tsx` | 購入パネルの配色を更新 |
| `FinancialDashboard.tsx` | チャート色・カード配色を更新 |
| `RaceTree.tsx` | サイドバー配色（`--color-bg-muted`）に更新 |
| その他コンポーネント | ネオン系クラス（`neon-text` 等）を新カラートークンに置換 |

### 変更しないもの（完全据え置き）

| レイヤー | 理由 |
|---|---|
| `app/api/**` | API Routes はデータ層。UI変更と無関係 |
| `lib/db.ts`, `lib/dbHelpers.ts` | SQLite接続・クエリヘルパー。変更不要 |
| `types/race.ts` | 型定義。変更不要 |
| `data/*.json` | 静的データキャッシュ。変更不要 |
| `public/sw.js` | PWA Service Worker。変更不要 |

---

## アーキテクチャ：データフロー（変更なし）

```
SQLite (umalogi.db)
    ↓
lib/db.ts + lib/dbHelpers.ts
    ↓
app/api/** (Route Handlers)
    ↓ fetch()
AppShell.tsx (クライアントコンポーネント)
    ↓ props
各コンポーネント ← ★ ここだけデザイン変更
```

---

## 実装フェーズ計画

### Phase 1: デザイン基盤（globals.css + 依存追加）
- `npm install lucide-react` で `web/` にアイコンライブラリを追加
- `@theme` ブロックにカラートークン・フォント変数を定義
- ネオン系 CSS ユーティリティクラスを廃止
- ベーススタイル（body, html）を新デザインに

### Phase 2: シェルとナビ（AppShell, NavBar）
- ページ全体の背景・サイドバー・ヘッダー配色を適用

### Phase 3: コアコンポーネント（RaceDetail, RaceTable, PredictionsPanel）
- 最も視認頻度の高いコンポーネントを優先実装
- AI分析パネル（Claude chat 風バブルカード）

### Phase 4: サブコンポーネント群
- StatCards, TodayBuyPanel, FinancialDashboard, RaceTree 等

### Phase 5: ビルド検証
- `npm run build` で型エラー・コンパイルエラーゼロを確認

---

## 成功基準

1. `npm run build` がエラーなく通る
2. 既存の API エンドポイントが全て正常に動作する（データ層に変更なし）
3. カラーパレット・フォントが設計仕様通りに適用されている
4. モバイル表示（スマートフォン幅）でもレイアウト崩れがない
