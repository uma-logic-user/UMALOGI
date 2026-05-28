# Claude風デザイン移植 実装プラン

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 既存 UMALOGI Web UI のダーク・ネオン配色を、ウォームベージュ × セリフ体 × ミニマルの Claude 風デザインに刷新する

**Architecture:** バックエンド・API・型定義は完全据え置き。`globals.css` でデザイン変数を一括再定義し（後方互換変数名を維持）、各コンポーネントのハードコードされた neon rgba 値のみをピンポイント修正する。

**Tech Stack:** Next.js 15, Tailwind CSS v4, TypeScript, lucide-react（新規追加）

---

## ファイルマップ

| ファイル | 操作 |
|---|---|
| `web/package.json` | Modify — lucide-react 追加 |
| `web/src/app/globals.css` | Complete rewrite — デザイン変数・ユーティリティクラス全置換 |
| `web/src/components/NavBar.tsx` | Modify — ハードコード neon 値を削除 |
| `web/src/components/AppShell.tsx` | Modify — inline textShadow/boxShadow を削除 |
| `web/src/components/RaceDetail.tsx` | Modify — neon border を CSS 変数に変更 |
| `web/src/components/RaceTable.tsx` | Modify — neon border と sort 色を変更 |
| `web/src/components/PredictionsPanel.tsx` | Modify — neon border と inline rgba を変更 |

---

## Task 1: lucide-react のインストール

**Files:**
- Modify: `web/package.json`（npm install で更新）

- [ ] **Step 1: lucide-react をインストール**

```bash
cd web && npm install lucide-react
```

Expected output: `added 1 package` もしくは既存バージョンの確認メッセージ

- [ ] **Step 2: インストール確認**

```bash
node -e "require('lucide-react'); console.log('OK')"
```

Expected: `OK`

- [ ] **Step 3: コミット**

```bash
cd .. && git add web/package.json web/package-lock.json
git commit -m "feat: lucide-reactをwebに追加"
```

---

## Task 2: globals.css — 完全刷新

**Files:**
- Modify: `web/src/app/globals.css`

これが最大のインパクトを持つタスク。CSS 変数名は**後方互換を保ちながら**値を warm カラーへ置換する。

- [ ] **Step 1: globals.css を以下の内容に完全置換**

`web/src/app/globals.css` を以下で上書きする:

```css
@import "tailwindcss";

/* ─── Tailwind v4 テーマトークン ──────────────────────── */
@theme {
  --color-bg-base:         #F9F8F6;
  --color-bg-surface:      #FDFCFB;
  --color-bg-muted:        #F0EDE6;
  --color-text-primary:    #2D2C28;
  --color-text-muted:      #6B6960;
  --color-text-subtle:     #9E9B93;
  --color-accent:          #C8A882;
  --color-accent-light:    #FEF3E2;
  --color-border:          #E5E3DB;
  --color-border-strong:   #D4D0C8;
  --color-hit-green:       #5B8A5B;
  --color-hit-amber:       #B8860B;
  --color-hit-red:         #C44040;
}

/* ─── カラー変数（後方互換マッピング）──────────────────── */
:root {
  --bg-base:      #F9F8F6;
  --bg-surface:   #FDFCFB;
  --bg-card:      #FDFCFB;
  --neon-cyan:    #C8A882;   /* テラコッタアクセント */
  --neon-blue:    #8B7355;
  --neon-green:   #5B8A5B;   /* ミュートグリーン */
  --neon-red:     #C44040;   /* ミュートレッド */
  --neon-gold:    #B8860B;   /* ダークゴールデンロッド */
  --text-primary: #2D2C28;
  --text-muted:   #6B6960;
  --border:       #E5E3DB;
  --bg-muted:     #F0EDE6;   /* サイドバー・ヘッダー背景 */
}

/* ─── ベース ─────────────────────────────────────────── */
html {
  font-size: 110%;
  overflow-x: hidden;
}

body {
  background-color: var(--bg-base);
  color: var(--text-primary);
  font-family: system-ui, -apple-system, 'Helvetica Neue', sans-serif;
}

/* ─── ウォーム系ユーティリティ（neon- クラス名を維持）── */
.neon-text       { color: var(--neon-cyan); }
.neon-text-gold  { color: var(--neon-gold); }
.neon-text-green { color: var(--neon-green); }
.neon-text-red   { color: var(--neon-red); }

.neon-border {
  border: 1px solid var(--border);
}

.neon-card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  box-shadow: 0 1px 4px rgba(45,44,40,0.06);
  border-radius: 12px;
  min-width: 0;
  overflow: hidden;
}

.neon-card-bright {
  background: var(--bg-surface);
  border: 1px solid var(--border);
  box-shadow: 0 2px 8px rgba(45,44,40,0.08);
  border-radius: 12px;
}

/* scanlines → 無効化（後方互換のみ） */
.scanlines { position: relative; }

/* ─── テーブルスクロールコンテナ ──────────────────────── */
.table-scroll {
  overflow-x: auto;
  overflow-y: auto;
  max-height: 72vh;
  min-width: 0;
  width: 100%;
  -webkit-overflow-scrolling: touch;
}

/* ─── テーブル ───────────────────────────────────────── */
.race-table th {
  position: sticky;
  top: 0;
  z-index: 10;
  background: var(--bg-surface);
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 0.75rem;
  font-weight: 600;
  padding: 10px 14px;
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
}

.race-table td {
  padding: 11px 14px;
  border-bottom: 1px solid rgba(229,227,219,0.7);
  font-size: 0.9rem;
  white-space: nowrap;
}

.race-table tr:hover td { background: rgba(240,237,230,0.5); }

.row-rank-1 td { background: rgba(184,134,11,0.04); }
.row-rank-2 td { background: rgba(160,160,160,0.03); }
.row-rank-3 td { background: rgba(139,94,60,0.03); }

/* ─── アニメーション ─────────────────────────────────── */
@keyframes pulse-warm {
  0%, 100% { opacity: 1; }
  50%       { opacity: 0.65; }
}
.pulse-neon { animation: pulse-warm 3s ease-in-out infinite; }

@keyframes odds-flash {
  0%   { background-color: rgba(200,168,130,0.18); }
  100% { background-color: transparent; }
}
.odds-flash { animation: odds-flash 2s ease-out forwards; }

@keyframes slide-in {
  from { opacity: 0; transform: translateY(10px); }
  to   { opacity: 1; transform: translateY(0); }
}
.slide-in { animation: slide-in 0.35s ease-out both; }

/* ─── 的中ハイライト ─────────────────────────────────── */
.hit-normal {
  background: rgba(91,138,91,0.05) !important;
  border-left: 2px solid var(--neon-green);
}
.hit-big {
  background: rgba(184,134,11,0.05) !important;
  border-left: 2px solid var(--neon-gold);
}
.hit-explosion {
  background: rgba(184,134,11,0.09) !important;
  border-left: 3px solid var(--neon-gold);
}
.hit-miss { opacity: 0.5; }

/* 払戻額バッジ */
.payout-badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.82rem;
  font-weight: 700;
  letter-spacing: 0.03em;
}
.payout-badge-normal {
  background: rgba(91,138,91,0.12);
  color: var(--neon-green);
  border: 1px solid rgba(91,138,91,0.3);
}
.payout-badge-big {
  background: rgba(184,134,11,0.12);
  color: var(--neon-gold);
  border: 1px solid rgba(184,134,11,0.35);
}
.payout-badge-explosion {
  background: rgba(184,134,11,0.18);
  color: var(--neon-gold);
  border: 1px solid rgba(184,134,11,0.5);
}

/* ─── アプリシェル ──────────────────────────────────── */
.app-shell {
  display: grid;
  grid-template-rows: auto 1fr;
  grid-template-columns: 260px minmax(0, 1fr);
  height: 100vh;
  max-height: 100vh;
  overflow: hidden;
}
.app-navbar {
  grid-column: 1 / -1;
  grid-row: 1;
  z-index: 50;
}
.app-sidebar {
  grid-column: 1;
  grid-row: 2;
  overflow-y: auto;
  overflow-x: hidden;
  border-right: 1px solid var(--border);
  background: #F0EDE6;
}
.app-main {
  grid-column: 2;
  grid-row: 2;
  overflow-y: auto;
  overflow-x: hidden;
  min-width: 0;
  width: 100%;
  max-width: 100%;
  background: var(--bg-base);
}

/* ─── サイドバーツリー ─────────────────────────────── */
.tree-section-btn {
  width: 100%;
  text-align: left;
  padding: 8px 12px;
  font-size: 0.75rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--text-muted);
  background: transparent;
  border: none;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 6px;
  transition: background 0.15s;
}
.tree-section-btn:hover { background: rgba(200,168,130,0.08); }

.tree-year-btn {
  width: 100%;
  text-align: left;
  padding: 7px 14px;
  font-size: 0.82rem;
  font-weight: 600;
  color: var(--text-primary);
  background: transparent;
  border: none;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 5px;
  transition: background 0.15s;
}
.tree-year-btn:hover { background: rgba(200,168,130,0.06); }

.tree-date-btn {
  width: 100%;
  text-align: left;
  padding: 5px 22px;
  font-size: 0.78rem;
  color: var(--text-primary);
  background: transparent;
  border: none;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 5px;
  transition: background 0.15s;
}
.tree-date-btn:hover { background: rgba(200,168,130,0.06); }

.tree-venue-label {
  padding: 4px 32px;
  font-size: 0.72rem;
  letter-spacing: 0.08em;
  color: var(--text-muted);
  text-transform: uppercase;
}

.tree-race-btn {
  width: 100%;
  text-align: left;
  padding: 5px 40px;
  font-size: 0.78rem;
  color: var(--text-muted);
  background: transparent;
  border: none;
  cursor: pointer;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  transition: color 0.15s, background 0.15s;
}
.tree-race-btn:hover { color: var(--text-primary); background: rgba(200,168,130,0.06); }
.tree-race-btn.active {
  color: var(--neon-cyan);
  background: rgba(200,168,130,0.10);
  border-left: 2px solid var(--neon-cyan);
  padding-left: 38px;
}

/* ─── サイドバー特別ボタン ─────────────────────────── */
.sidebar-special-btn {
  width: 100%;
  text-align: left;
  padding: 10px 16px;
  font-size: 0.8rem;
  font-weight: 600;
  letter-spacing: 0.06em;
  background: transparent;
  border: none;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 8px;
  transition: background 0.15s;
  border-bottom: 1px solid var(--border);
  color: var(--text-primary);
}
.sidebar-special-btn:hover  { background: rgba(200,168,130,0.08); }
.sidebar-special-btn.active { background: rgba(200,168,130,0.14); }

/* ─── 的中履歴 / HitHistory ────────────────────────── */
.hit-row-normal  { background: rgba(91,138,91,0.04);   border-left: 3px solid var(--neon-green); }
.hit-row-big     { background: rgba(184,134,11,0.05);  border-left: 3px solid var(--neon-gold); }
.hit-row-jackpot { background: rgba(184,134,11,0.09);  border-left: 4px solid var(--neon-gold); }

.roi-badge {
  display: inline-block;
  padding: 3px 10px;
  border-radius: 6px;
  font-size: 0.82rem;
  font-weight: 800;
  letter-spacing: 0.04em;
}
.roi-badge-normal  { background: rgba(91,138,91,0.12);  color: var(--neon-green); border: 1px solid rgba(91,138,91,0.3); }
.roi-badge-big     { background: rgba(184,134,11,0.12); color: var(--neon-gold);  border: 1px solid rgba(184,134,11,0.35); }
.roi-badge-jackpot { background: linear-gradient(135deg,var(--neon-gold),var(--neon-cyan)); color: #fff; }

.payout-table td { padding: 8px 12px; font-size: 0.85rem; border-bottom: 1px solid rgba(229,227,219,0.6); }
.payout-table th {
  padding: 9px 12px;
  font-size: 0.72rem;
  letter-spacing: 0.08em;
  color: var(--text-muted);
  text-transform: uppercase;
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
}

/* ─── EV推奨馬バッジ ────────────────────────────────── */
.row-hot td { background: rgba(200,168,130,0.06) !important; }
.row-hot:hover td { background: rgba(200,168,130,0.10) !important; }

.badge-hot {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.05em;
  background: rgba(200,168,130,0.15);
  color: var(--neon-cyan);
  border: 1px solid rgba(200,168,130,0.4);
}

/* ─── 調教グレードバッジ ────────────────────────────── */
.eval-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 20px;
  border-radius: 4px;
  font-size: 0.72rem;
  font-weight: 900;
  cursor: default;
}
.eval-badge-A { background: rgba(196,64,64,0.12);  color: var(--neon-red);   border: 1px solid rgba(196,64,64,0.3); }
.eval-badge-B { background: rgba(184,134,11,0.12); color: var(--neon-gold);  border: 1px solid rgba(184,134,11,0.3); }
.eval-badge-C { background: rgba(200,168,130,0.12);color: var(--neon-cyan);  border: 1px solid rgba(200,168,130,0.3); }
.eval-badge-D { background: rgba(107,105,96,0.10); color: var(--text-muted); border: 1px solid rgba(107,105,96,0.2); }

/* ─── バイアスメーター ──────────────────────────────── */
.bias-meter-bar { height: 6px; border-radius: 3px; transition: width 0.6s ease; }

/* ─── モデルタブバー ────────────────────────────────── */
.model-tabs {
  display: flex;
  background: var(--bg-surface);
  border-bottom: 1px solid var(--border);
  position: sticky;
  top: 0;
  z-index: 5;
}
.model-tab-btn {
  flex: 1;
  min-height: 44px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 2px;
  font-size: 0.78rem;
  font-weight: 600;
  letter-spacing: 0.04em;
  color: var(--text-muted);
  background: transparent;
  border: none;
  border-bottom: 2px solid transparent;
  margin-bottom: -1px;
  cursor: pointer;
  transition: color 0.15s, border-color 0.15s, background 0.15s;
  padding: 6px 4px;
}
.model-tab-btn:hover { color: var(--text-primary); background: rgba(200,168,130,0.06); }
.model-tab-btn.tab-active-alpha  { color: var(--neon-cyan);  border-bottom-color: var(--neon-cyan); }
.model-tab-btn.tab-active-manzai { color: var(--neon-gold);  border-bottom-color: var(--neon-gold); }
.model-tab-btn.tab-active-honmei { color: var(--neon-green); border-bottom-color: var(--neon-green); }
.model-tab-btn.tab-active-other  { color: var(--neon-cyan);  border-bottom-color: var(--neon-cyan); }

.model-tab-count {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 18px;
  height: 16px;
  padding: 0 4px;
  border-radius: 9px;
  font-size: 0.62rem;
  font-weight: 900;
  background: rgba(45,44,40,0.07);
  color: var(--text-muted);
}
.tab-active-alpha  .model-tab-count { background: rgba(200,168,130,0.18); color: var(--neon-cyan); }
.tab-active-manzai .model-tab-count { background: rgba(184,134,11,0.18);  color: var(--neon-gold); }
.tab-active-honmei .model-tab-count { background: rgba(91,138,91,0.18);   color: var(--neon-green); }

/* ─── 予想カード ────────────────────────────────────── */
.pred-card {
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-left: 3px solid var(--neon-cyan);
  border-radius: 10px;
  padding: 12px 14px;
  position: relative;
  overflow: hidden;
  transition: border-color 0.2s, box-shadow 0.2s;
}
.pred-card-alpha  { border-left-color: var(--neon-cyan); }
.pred-card-manzai { border-left-color: var(--neon-gold); }
.pred-card-honmei { border-left-color: var(--neon-green); }
.pred-card-hit {
  background: rgba(184,134,11,0.04);
  border-color: rgba(184,134,11,0.25);
  box-shadow: 0 1px 8px rgba(184,134,11,0.08);
}

/* EV 大型表示 */
.ev-display-large {
  font-family: system-ui, sans-serif;
  font-size: 2rem;
  font-weight: 900;
  line-height: 1;
  letter-spacing: -0.02em;
}
.ev-display-cyan  { color: var(--neon-cyan); }
.ev-display-gold  { color: var(--neon-gold); }
.ev-display-red   { color: var(--neon-red); }
.ev-display-muted { color: var(--text-muted); }

/* 馬番サークル */
.horse-num-lg {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 34px;
  height: 34px;
  border-radius: 50%;
  background: rgba(200,168,130,0.12);
  color: var(--neon-cyan);
  font-weight: 900;
  font-size: 1rem;
  border: 1.5px solid rgba(200,168,130,0.4);
  flex-shrink: 0;
}
.horse-num-lg.medal-1 { background: #B8860B; color: #fff; border: none; }
.horse-num-lg.medal-2 { background: #A0A0A0; color: #fff; border: none; }
.horse-num-lg.medal-3 { background: #8B5E3C; color: #fff; border: none; }

/* 馬カード（モバイル） */
.horse-row-card {
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px 12px;
  display: grid;
  grid-template-columns: 42px 1fr;
  gap: 10px;
  align-items: center;
  position: relative;
  overflow: hidden;
}
.horse-row-card::before { content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 2px; }
.horse-row-card.rank-1 { background: rgba(184,134,11,0.04); border-color: rgba(184,134,11,0.2); }
.horse-row-card.rank-1::before { background: var(--neon-gold); }
.horse-row-card.rank-2::before { background: #A0A0A0; }
.horse-row-card.rank-3::before { background: #8B5E3C; }
.horse-row-card.hot-card { background: rgba(200,168,130,0.06); border-color: rgba(200,168,130,0.25); }
.horse-row-card.hot-card::before { background: var(--neon-cyan); }

/* 大口シグナル */
@keyframes fire-flicker {
  0%, 100% { opacity: 1;   transform: scale(1); }
  50%       { opacity: 0.8; transform: scale(1.1); }
}
.signal-fire { display: inline-block; animation: fire-flicker 0.8s ease-in-out infinite; font-size: 0.9rem; line-height: 1; }

/* ─── モバイル（スマホ縦向き）────────────────────────── */
@media (max-width: 768px) {
  html { font-size: 100%; }

  .app-shell {
    grid-template-columns: 1fr;
    grid-template-rows: auto auto 1fr;
    height: 100dvh;
    max-height: 100dvh;
    overflow: hidden;
  }
  .app-navbar  { grid-column: 1; grid-row: 1; min-width: 0; max-width: 100vw; overflow: hidden; }
  .app-sidebar {
    grid-column: 1;
    grid-row: 2;
    min-width: 0;
    max-width: 100vw;
    display: flex;
    flex-direction: row;
    overflow-x: auto;
    overflow-y: hidden;
    border-right: none;
    border-bottom: 1px solid var(--border);
    background: #F0EDE6;
    -webkit-overflow-scrolling: touch;
    scrollbar-width: none;
    min-height: 52px;
    max-height: 52px;
    padding: 0 4px;
    gap: 0;
  }
  .app-sidebar::-webkit-scrollbar { display: none; }
  .app-main {
    grid-column: 1;
    grid-row: 3;
    overflow-y: auto;
    overflow-x: hidden;
    min-width: 0;
    max-width: 100vw;
  }

  .app-sidebar .py-1:last-child { display: none; }

  .app-sidebar .border-b {
    display: flex;
    flex-direction: row;
    border: none;
    flex-shrink: 0;
    padding: 0;
    overflow-x: visible;
  }
  .sidebar-special-btn {
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 6px 10px;
    font-size: 0.68rem;
    letter-spacing: 0.04em;
    white-space: nowrap;
    border-bottom: none;
    border-right: 1px solid var(--border);
    min-width: 60px;
    flex-shrink: 0;
    gap: 2px;
  }
  .sidebar-special-btn .ml-auto { display: none; }
  .table-scroll { max-height: 60vh; }

  @media (max-width: 480px) {
    .app-sidebar { max-height: 48px; min-height: 48px; }
    .sidebar-special-btn { min-width: 56px; font-size: 0.62rem; padding: 4px 6px; }
  }
}
```

- [ ] **Step 2: ビルドエラーが出ないことを確認**

```bash
cd web && npx tsc --noEmit 2>&1 | head -20
```

Expected: エラー出力なし

- [ ] **Step 3: コミット**

```bash
cd .. && git add web/src/app/globals.css
git commit -m "style: globals.cssをClaud風ウォームデザインに全面刷新（Tailwind v4 @theme）"
```

---

## Task 3: NavBar.tsx — ハードコード neon 値を削除

**Files:**
- Modify: `web/src/components/NavBar.tsx`

NavBar は2箇所にハードコードされた neon 値がある。

- [ ] **Step 1: ロゴアイコンの boxShadow を削除し、border を CSS 変数に変更**

変更前:
```tsx
<div className="w-8 h-8 rounded border border-[rgba(0,200,255,0.5)] flex items-center justify-center"
  style={{ boxShadow: '0 0 12px rgba(0,200,255,0.4)' }}>
  <span className="text-xs neon-text font-bold">U</span>
</div>
<span className="text-lg font-bold tracking-[0.25em] neon-text scanlines">
  UMALOGI
</span>
```

変更後（`web/src/components/NavBar.tsx` の該当箇所を以下に置換）:
```tsx
<div className="w-8 h-8 rounded border border-[var(--border)] flex items-center justify-center"
  style={{ background: 'var(--bg-muted)' }}>
  <span className="text-xs neon-text font-bold">U</span>
</div>
<span className="text-lg font-bold tracking-[0.15em] neon-text"
  style={{ fontFamily: 'Georgia, serif' }}>
  UMALOGI
</span>
```

- [ ] **Step 2: header の border-b を CSS 変数に変更**

変更前:
```tsx
<header className="relative z-10 flex items-center justify-between px-3 py-3 sm:px-6 sm:py-4 border-b border-[rgba(0,200,255,0.15)]">
```

変更後:
```tsx
<header className="relative z-10 flex items-center justify-between px-3 py-3 sm:px-6 sm:py-4 border-b border-[var(--border)]" style={{ background: 'var(--bg-surface)' }}>
```

- [ ] **Step 3: ナビリンクのホバー色を変更**

変更前:
```tsx
<span className="hover:text-[var(--neon-cyan)] transition-colors cursor-pointer">PEDIGREE</span>
<span className="hover:text-[var(--neon-cyan)] transition-colors cursor-pointer">WIN5</span>
<span className="hover:text-[var(--neon-cyan)] transition-colors cursor-pointer">ANALYTICS</span>
```

変更後（`var(--neon-cyan)` はすでに `#C8A882` に変数マップされているためクラス変更不要。ただしトラッキングを調整）:
```tsx
<span className="hover:text-[var(--text-primary)] transition-colors cursor-pointer">PEDIGREE</span>
<span className="hover:text-[var(--text-primary)] transition-colors cursor-pointer">WIN5</span>
<span className="hover:text-[var(--text-primary)] transition-colors cursor-pointer">ANALYTICS</span>
```

- [ ] **Step 4: ステータスドットの boxShadow を削除**

変更前:
```tsx
<span className="w-2 h-2 rounded-full bg-[var(--neon-green)] pulse-neon"
  style={{ boxShadow: '0 0 8px var(--neon-green)' }} />
```

変更後:
```tsx
<span className="w-2 h-2 rounded-full bg-[var(--neon-green)] pulse-neon" />
```

- [ ] **Step 5: コミット**

```bash
cd .. && git add web/src/components/NavBar.tsx
git commit -m "style: NavBarのneonインラインスタイルをウォームデザインに更新"
```

---

## Task 4: AppShell.tsx — インライン neon スタイルを削除

**Files:**
- Modify: `web/src/components/AppShell.tsx`

AppShell の読み込み画面とサイドバーボタンにハードコードされた neon rgba 値がある。

- [ ] **Step 1: ローディング画面を更新**

変更前（AppShell.tsx の loading return 内）:
```tsx
<div className="neon-text text-xl tracking-[0.3em] animate-pulse">UMALOGI</div>
<div className="text-sm text-[var(--text-muted)] mt-3 tracking-widest">Loading...</div>
```

変更後:
```tsx
<div className="text-xl tracking-[0.2em] animate-pulse neon-text"
  style={{ fontFamily: 'Georgia, serif' }}>UMALOGI</div>
<div className="text-sm mt-3 tracking-widest" style={{ color: 'var(--text-muted)' }}>データを読み込み中...</div>
```

- [ ] **Step 2: サイドバーボタンの textShadow を削除**

AppShell.tsx の `的中実績` ボタン内の `textShadow` スタイルを削除:

変更前:
```tsx
<span style={{
  color: 'var(--neon-gold)',
  textShadow: '0 0 8px rgba(255,215,0,0.7)',
  fontSize: '1rem',
}}>★</span>
```

変更後:
```tsx
<span style={{ color: 'var(--neon-gold)', fontSize: '1rem' }}>★</span>
```

- [ ] **Step 3: `的中実績` バッジの背景色を更新**

変更前:
```tsx
<span
  className="ml-auto text-xs font-bold px-1.5 py-0.5 rounded"
  style={{
    background: 'rgba(255,215,0,0.15)',
    color: 'var(--neon-gold)',
    border: '1px solid rgba(255,215,0,0.3)',
  }}
>
```

変更後:
```tsx
<span
  className="ml-auto text-xs font-bold px-1.5 py-0.5 rounded"
  style={{
    background: 'rgba(184,134,11,0.12)',
    color: 'var(--neon-gold)',
    border: '1px solid rgba(184,134,11,0.3)',
  }}
>
```

- [ ] **Step 4: ガチ実績バッジの背景色を更新**

変更前:
```tsx
<span className="ml-auto text-xs font-bold px-1.5 py-0.5 rounded"
      style={{ background: 'rgba(255,71,87,0.15)', color: '#ff4757', border: '1px solid rgba(255,71,87,0.3)' }}>
```

変更後:
```tsx
<span className="ml-auto text-xs font-bold px-1.5 py-0.5 rounded"
      style={{ background: 'rgba(196,64,64,0.12)', color: 'var(--neon-red)', border: '1px solid rgba(196,64,64,0.3)' }}>
```

- [ ] **Step 5: サイドバーの border-b クラスを CSS 変数に変更**

変更前:
```tsx
<div className="border-b border-[rgba(0,200,255,0.1)] py-1">
```

変更後:
```tsx
<div className="border-b border-[var(--border)] py-1">
```

- [ ] **Step 6: コミット**

```bash
cd .. && git add web/src/components/AppShell.tsx
git commit -m "style: AppShellのインラインneonスタイルをウォームデザインに更新"
```

---

## Task 5: RaceDetail.tsx — neon ボーダーを CSS 変数に変更

**Files:**
- Modify: `web/src/components/RaceDetail.tsx`

RaceDetail の border とヘッダーにハードコードされた neon rgba がある。

- [ ] **Step 1: EV推奨馬セクションの border-t を更新**

変更前:
```tsx
<div className="mt-4 pt-3 border-t border-[rgba(0,200,255,0.12)]">
```

変更後:
```tsx
<div className="mt-4 pt-3 border-t border-[var(--border)]">
```

- [ ] **Step 2: RACE ID テキストカラーを更新**

変更前:
```tsx
<div className="font-mono text-xs text-[var(--neon-cyan)] opacity-60">{race.race_id}</div>
```

変更後（`var(--neon-cyan)` はすでに remapped で問題ないが、opacity を少し変える）:
```tsx
<div className="font-mono text-xs opacity-40" style={{ color: 'var(--text-muted)' }}>{race.race_id}</div>
```

- [ ] **Step 3: タブの border-b を更新**

RaceDetail 内のタブ部分を検索し `border-[rgba(0,200,255,` パターンを `border-[var(--border)]` に置換する。

```bash
grep -n "rgba(0,200,255" web/src/components/RaceDetail.tsx
```

出力されたすべての行を `var(--border)` または `var(--neon-cyan)` に置換する。

- [ ] **Step 4: コミット**

```bash
cd .. && git add web/src/components/RaceDetail.tsx
git commit -m "style: RaceDetailのneonボーダーをCSS変数に変更"
```

---

## Task 6: RaceTable.tsx — テーブルヘッダーとボーダーを更新

**Files:**
- Modify: `web/src/components/RaceTable.tsx`

- [ ] **Step 1: テーブルヘッダーの border-b を更新**

変更前:
```tsx
<div className="flex items-center justify-between px-4 py-3 border-b border-[rgba(0,200,255,0.12)]">
  <span className="text-sm neon-text tracking-[0.2em] font-semibold">RACE RESULTS</span>
```

変更後:
```tsx
<div className="flex items-center justify-between px-4 py-3 border-b border-[var(--border)]">
  <span className="text-sm font-semibold tracking-[0.12em]" style={{ color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.1em' }}>レース結果</span>
```

- [ ] **Step 2: ソートアクティブカラーを CSS 変数に変更**

変更前:
```tsx
style={{ color: active ? 'var(--neon-cyan)' : undefined }}
```

変更後（`var(--neon-cyan)` は remapped なので実質変更不要。明示的に accent 変数を使う）:
```tsx
style={{ color: active ? 'var(--neon-cyan)' : 'var(--text-muted)' }}
```

- [ ] **Step 3: コミット**

```bash
cd .. && git add web/src/components/RaceTable.tsx
git commit -m "style: RaceTableのneon値をウォームデザインに更新"
```

---

## Task 7: PredictionsPanel.tsx — ハードコード rgba を更新

**Files:**
- Modify: `web/src/components/PredictionsPanel.tsx`

- [ ] **Step 1: 残存するハードコード neon rgba を一括確認**

```bash
grep -n "rgba(0,200,255\|rgba(255,215,0\|rgba(255,51,102\|rgba(0,255,136" \
  web/src/components/PredictionsPanel.tsx
```

出力がある行をすべて以下のマッピングで置換する:
- `rgba(0,200,255,...)` → `rgba(200,168,130,...)`（テラコッタ）
- `rgba(255,215,0,...)`  → `rgba(184,134,11,...)`（ダークゴールデンロッド）
- `rgba(0,255,136,...)`  → `rgba(91,138,91,...)`（ミュートグリーン）
- `rgba(255,51,102,...)`  → `rgba(196,64,64,...)`（ミュートレッド）

- [ ] **Step 2: コミット**

```bash
cd .. && git add web/src/components/PredictionsPanel.tsx
git commit -m "style: PredictionsPanelのハードコードneon値を置換"
```

---

## Task 8: 残存 neon 値のスキャン & 修正

**Files:**
- Modify: 任意の残存ファイル

- [ ] **Step 1: 全コンポーネントで残存するネオン値をスキャン**

```bash
grep -rn "rgba(0,200,255\|rgba(255,215,0\|neon-cyan\|0 0 [0-9]*px rgba(0,200,255\|boxShadow.*200,255\|textShadow.*200,255" \
  web/src/components/ web/src/app/
```

- [ ] **Step 2: 検出された各ファイルを修正**

検出されたファイルのハードコード neon 値を以下のルールで置換する:
- `rgba(0,200,255, N)` → `rgba(200,168,130, N)`
- `rgba(255,215,0, N)` → `rgba(184,134,11, N)`
- `rgba(0,255,136, N)` → `rgba(91,138,91, N)`
- `rgba(255,51,102, N)` → `rgba(196,64,64, N)`
- `boxShadow: '0 0 ...px rgba(0,200,255,...)` → 削除
- `textShadow: '0 0 ...px rgba(...)'` → 削除

- [ ] **Step 3: コミット**

```bash
cd .. && git add web/src/components/
git commit -m "style: 残存ハードコードneon値の一括クリーンアップ"
```

---

## Task 9: ビルド検証

**Files:**
- なし（検証のみ）

- [ ] **Step 1: TypeScript 型チェック**

```bash
cd web && npx tsc --noEmit
```

Expected: エラー出力なし（0 errors）

- [ ] **Step 2: Next.js ビルド**

```bash
npm run build
```

Expected:
```
✓ Compiled successfully
✓ Collecting page data
✓ Generating static pages
```

エラーが出た場合は `error` 行を読み、該当ファイルを修正して再ビルド。

- [ ] **Step 3: 開発サーバーで動作確認**

```bash
npm run dev
```

ブラウザで `http://localhost:3000` を開き、以下を確認する:
1. 背景色がウォームベージュ（`#F9F8F6`）になっている
2. テキストがチャコールグレー（`#2D2C28`）になっている
3. サイドバーがサンドベージュ（`#F0EDE6`）になっている
4. アクセントカラーがテラコッタ（`#C8A882`）になっている
5. ネオングロー効果が消えている
6. モバイル幅でもレイアウト崩れがない

- [ ] **Step 4: 最終コミット**

```bash
cd .. && git add -A
git commit -m "feat: Claude風ウォームデザイン移植 完了（Task 1-9 all done）"
```

---

## クイックリファレンス: カラーマッピング表

| 旧 neon 色 | CSS 変数名（維持）| 新しい値 | 用途 |
|---|---|---|---|
| `#00c8ff` cyan | `--neon-cyan` | `#C8A882` | テラコッタアクセント |
| `#ffd700` gold | `--neon-gold` | `#B8860B` | ダークゴールデンロッド |
| `#00ff88` green | `--neon-green` | `#5B8A5B` | ミュートグリーン（的中） |
| `#ff3366` red | `--neon-red` | `#C44040` | ミュートレッド（外れ） |
| `#02060e` base | `--bg-base` | `#F9F8F6` | ページ背景 |
| `#060f1e` surface | `--bg-surface` | `#FDFCFB` | カード背景 |
| `#e0f4ff` text | `--text-primary` | `#2D2C28` | チャコールグレー |
| `rgba(0,200,255,0.18)` border | `--border` | `#E5E3DB` | ウォームボーダー |
