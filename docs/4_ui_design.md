# UMALOGI UI 設計書（Discord / ダッシュボード）

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-05-25 | 【当日購入タブ新規実装】各レース詳細画面に「当日購入」タブを追加。ケリー基準（f*=(EV-1)/(odds-1)、1/4 Kelly）で推奨購入金額を算出・表示。総資金（¥50,000〜¥500,000 プリセット）・ケリー係数（10%/25%/50%/100%）はUI上でドロップダウン変更可能。EV<1.0 の買い目は「購入見送り」グレー表示。オッズは race.results の win_odds から馬番で照合。合計推奨購入額を表示。影響: `web/src/components/TodayBuyPanel.tsx`（新規）, `web/src/lib/kelly.ts`（新規）, `web/src/components/RaceDetail.tsx` |
| 2026-05-25 | 【RaceDetail レース結果タブ払戻統合 + 的中結果タブAIフィルタリング修正】`RaceDetail.tsx` を2箇所修正。①`ResultsTable` に `payouts: RacePayout[]` prop を追加し、レース結果カード下部に払戻金セクションを統合表示（これまで別タブで分離していた払戻をレース結果タブに統合）。②「的中結果」タブを `race.payouts` 全件表示 → AI予想の `is_hit=1` 組み合わせとの照合でフィルタリング表示に変更（ヘルパー関数 `parsePayoutCombo` / `parsePredComboJson` / `comboMatches` / `getHitPayouts` 追加。馬単・三連単は順序一致、それ以外は馬番ソートで照合）。タブバッジを全払戻件数 → AI的中件数（0件は非表示）に変更。空状態メッセージを「的中なし」「予想データなし」で出し分け。影響: `web/src/components/RaceDetail.tsx` |
| 2026-05-24 | 【的中結果タブ新設 + 一括評価バッチ】`TabView.tsx` に第4タブ「的中結果」を追加（開催日/会場R/レース名/モデル/券種/AI推奨馬/#/確定着順1-3着/払戻/ROI/EV 表示。モデルフィルター+日付/ROI/払戻/EVソート対応）。`/api/hits/route.ts` に `actual_winners`（確定着順1-3着）を追加。`race.ts` に `ActualWinner` 型追加。`scripts/run_evaluate_all.py` 新設（`Evaluator.evaluate_race()` を全442レースに一括適用: 的中1,541/10,517 件 ROI=75.7%）。`/api/compare/[race_id]/route.ts` の Next.js 15 params 型を `Promise<>` に修正。影響: `web/src/components/TabView.tsx`, `web/src/app/api/hits/route.ts`, `web/src/types/race.ts`, `web/src/components/AppShell.tsx`, `scripts/run_evaluate_all.py`, `web/src/app/api/compare/[race_id]/route.ts` |
| 2026-05-24 | 【Kelly推奨額UI修正】`PredictionsPanel.tsx` 予想カードフッターに `💰 Kelly推奨 ¥X,XXX`（`predictions.recommended_bet` 由来・¥100,000バンクロール基準の1/4 Kelly実額）を表示。`RaceDetail.tsx` の出馬表ヘッダー「Kelly推奨」→「単勝Kelly推奨」・凡例を「100万円資金想定」→「このバンクロールでの単勝買い参考額（1/4 Kelly）」に修正。影響: `web/src/components/PredictionsPanel.tsx`, `web/src/components/RaceDetail.tsx` |
| 2026-05-24 | 【X投稿文 note誘導テンプレート化】`post_x_drafts_to_discord.py` の `_build_x_post()` を280字特化・note誘導型に全面刷新。「買い目詳細 → EV値 + note誘導リンク」へ変更。EV≥10.0 に `【重要】` タグ・🔥 ゴールド embed 付与。`NOTE_PROFILE_URL` 環境変数 / `--note-url` 引数対応。`.env.example` に `NOTE_PROFILE_URL` 追加。影響: `scripts/post_x_drafts_to_discord.py`, `.env.example` |
| 2026-05-24 | 【Note下書き一括投稿スクリプト新設】`scripts/batch_post_note_drafts_today.py` を新設。`data/drafts/YYYYMMDD/*.txt` から本日のEV上位N件（デフォルト5）を読み込み、Playwright `save_draft()` で note.com に下書き投稿。EV≥5.0 のレースには `【重要：期待値選別・推奨】` バナーを自動付与。投稿間隔15秒（Bot検知回避）。完了後 Discord#システムへ結果サマリーを送信。`--dry-run` / `--all` / `--top N` オプション対応。影響: `scripts/batch_post_note_drafts_today.py`（新設） |
| 2026-05-24 | 【RaceTree発走時刻+カウントダウン表示】`RaceTree.tsx` に `estimatePostTime()`（R1=10:00 JST +30分/R）・`formatCountdown()`・`todayJST()` を追加。各レースボタンに推定発走時刻（HH:MM形式・グレー）を右端に表示。当日レースで未発走のものにカウントダウン（X時間Y分後）を緑/黄色で表示（30分以内は黄色 `#FFD700`）。60秒ごとに `setInterval` で自動更新。マウスオーバーで「発走 HH:MM JST」をツールチップ表示。影響: `web/src/components/RaceTree.tsx` |
| 2026-05-24 | 【Kelly vs フラット比較パネル追加】`FinancialDashboard.tsx` に `KellyComparisonPanel` コンポーネントを追加。WF 2025年バックテスト実証値（Alpha-Payout: flat=64%/kelly=129.2%、本命: flat=79%/kelly=148.3%、卍: flat=100%/kelly=963%）を SVG 比較バー形式で表示。KPI カードと利益チャートの間に配置。バンクロールと現在モデルに連動して動的ハイライト。影響: `web/src/components/FinancialDashboard.tsx` |
| 2026-05-24 | 【AIウマスギフィルターUI統合】`PredictionsPanel.tsx` 先頭に「🤖 AIウマスギフィルター適用済み」バナーを追加（本命:単勝/複勝/三連単EV≥1.5、卍:三連単除外、Alpha:三連単除外）。本命×三連単(EV≥1.5)に「⚡条件付許可」バッジをモバイル・デスクトップ両対応で追加（ツールチップあり）。`buildXText()`/`buildNoteText()` に「🤖フィルター済」追記。`generate_sns_post.py` パターンA全バリアントにROIフィルター適用済みを追記。影響: `web/src/components/PredictionsPanel.tsx`, `web/src/components/RaceDetail.tsx`, `scripts/generate_sns_post.py` |
| 2026-05-24 | 【SNSコピーボタン分離強化 + 買い目マルチ/軸流し表記統一】`SnsShareButton` を X用/NOTE用 2ボタン（`SnsButtons`）に分割。X用: ≤280文字・動的ハッシュタグ（`#会場NR #UMALOGI #競馬AI #重賞名`）・最高EV買い目を1行集約。NOTE用: Markdown形式・モデル別グループ・`formatBetCompact()` で全買い目をマルチ/軸流し/ボックス表記に自動変換。クリップボードAPIフォールバック（`execCommand`）対応。`discord_notifier.py:_format_combo_card()` の三連単/馬単セクションを「マルチ・1着固定」自動判定に刷新。`note_generator.py:_fmt_combo()` も全面改修（ベタ展開廃止・軸流し/マルチ優先）。Next.jsクリーンビルド実施。影響: `web/src/components/RaceDetail.tsx`, `src/notification/discord_notifier.py`, `src/ops/note_generator.py` |
| 2026-05-24 | 【収支タブROI計算バグ修正】financial/condition/summary の全APIが `p.recommended_bet` を投資額として使用していたため、流し/マルチ等の複数組み合わせ買いで投資額が2〜3倍過少計上されROIが水増しされていた。`(COALESCE(pr.payout,0) - COALESCE(pr.profit,0))` 基準（evaluator.py が正確に記録済みの実投資額）に統一。修正後の真実ROI: 単勝446.9%・卍(暫定)378.2%・全体69.6%。影響ファイル: `web/src/app/api/financial/route.ts`, `web/src/app/api/condition/route.ts`, `web/src/app/api/summary/route.ts` |
| 2026-05-23 | 【RaceDetail 4サブタブ化 + SNSコピーボタン】RaceDetail.tsx を出馬表/レース結果/AI予想/的中結果の4サブタブに再編成。AI予想タブにSNS投稿テキスト一発コピーボタン（`SnsShareButton`）を追加。新規コンポーネント: `RaceCardTable`（出馬表）、`SnsShareButton`（コピーボタン）。影響: `web/src/components/RaceDetail.tsx` |
| 2026-05-21 | 【Discord ログラベル精緻化】`DiscordNotifier.__init__` に `channel_label` パラメータを追加（デフォルト "予想"）。`send_text()` ログを `[Discord:{channel_label}] 送信:` に動的化。`NotificationRouter._build_channels()` で各チャンネル（予想/システム/EV激熱/A/Bテスト/note下書き）のラベルを設定。全 10 テスト PASS 継続確認。影響: `src/notification/discord_notifier.py`, `src/notification/router.py` |
| 2026-05-20 | 【NotificationRouter 完成: notify_ev_alert/send_prediction_embed 追加 + テスト全PASS】`router.py` に `notify_ev_alert()` スタンドアロンメソッドを追加（`_channels.get("ev_alert")` 直接参照で prediction 二重送信を防止）。`discord_notifier.py` / `router.py` に `send_prediction_embed()` を追加（scheduler 週次サマリー用 raw embed 送信）。`__init__.py` に `NotificationRouter` を再エクスポート。テスト修正: `monkeypatch.delenv` → `setenv("")` に変更（`discord_notifier.py` モジュールレベル `load_dotenv(override=False)` が delenv 済み変数を .env から復元するバグを回避）。全10テスト PASS 確認。影響: `src/notification/router.py`, `src/notification/discord_notifier.py`, `src/notification/__init__.py`, `tests/notification/test_router.py` |
| 2026-05-20 | 通知ルーター新設: `NotificationRouter`（`src/notification/router.py`）による5チャンネル分離（prediction/system/ev_alert/ab_test/note_draft）。EV>=1.5 で ev_alert チャンネルへ `@everyone` 激熱アラート自動送信。フォールバック: 専用ch未設定時は prediction ch → None の順。後方互換: `DISCORD_SYSTEM_WEBHOOK_URL` を自動マッピング。`post_weekly_note_draft.py` に Discord note下書き転送ステップ追加（`ENABLE_PLAYWRIGHT_POST=True` 時のみ Playwright 投稿も併用）。影響: `src/notification/router.py`（新設）, `src/pipeline/prediction.py`, `scripts/today_auto_runner.py`, `scripts/scheduler.py`, `scripts/post_weekly_note_draft.py`, `scripts/generate_performance_report.py`（新設） |
| 2026-05-20 | 【UI横幅Blowout根治 + 4モデルフィルター + V2バリデーション修正】① `globals.css` `.neon-card` に `overflow:hidden; min-width:0` を追加（グリッド内で幅膨張する根本原因を封じ込め）。② `FinancialDashboard.tsx` `KpiCard` root div に `min-w-0` 追加。③ `HitHistory.tsx` モデルフィルターに「⚡卍V2」「🎯本命V2」を追加・カラーコード分離（卍系=cyan/本命系=gold）。④ `init_db.py` `insert_prediction()` の `_VALID_BASE_TYPES` に `卍V2`/`本命V2`/`OracleV2`/`HitFocusV2` を追加（V2モデル保存時のValueError根絶）。⑤ `reconcile.py` モデル成績再集計ループに `"卍V2"/"本命V2"` を追加。影響: `web/src/app/globals.css`, `web/src/components/FinancialDashboard.tsx`, `web/src/components/HitHistory.tsx`, `src/database/init_db.py`, `src/ml/reconcile.py` |
| 2026-05-20 | 【note.com自動投稿 Stealth化完全実装】`src/ops/note_draft_publisher.py` を全面改修。①`playwright-stealth v2`の`Stealth().use_sync()`でPlaywright全体をラップし`navigator.webdriver`/`chrome runtime`等を隠蔽。②`headless=False`を全操作で強制（`save_draft`の`headless`引数は受け付けるが常にFalseへ上書き）。③Chrome 134 UA・ja-JP locale・ランダム viewport・`--disable-blink-features=AutomationControlled`を設定。④ブロック検知：Cloudflare/Imperva/Akamai/noteブロックページを`_detect_block()`で分類し、ブロック種別ごとに詳細ログを出力。⑤証拠保全：全ステップでスクリーンショット＋HTMLダンプを`outputs/debug/`に保存。⑥ログイン完了検知を`page.url`→JS`window.location.href`に変更（SPA URL追跡バグ修正）。⑦`requirements.txt`に`playwright-stealth>=2.0.0`追加。テスト結果：reCAPTCHA未出現・3秒でログイン完了・「障害ページ」消滅・3827文字の下書き保存に成功。影響: `src/ops/note_draft_publisher.py`, `requirements.txt` |
| 2026-05-19 | 【Discord通知クオンツ推奨セクション追加】`scripts/notify_discord.py` の `build_messages()` を改修。EV推奨馬が2頭以上の場合、上位2頭から「★QF推奨 ワイド N1-N2 / 馬連 N1-N2」を各レースメッセージ先頭に強調表示するよう追加（WFバックテスト実証済み戦略）。`src/ml/bet_generator.py` の `HonmeiStrategy.generate()` で馬連・ワイドの notes に「★QF推奨」プレフィックスを追加し、result.bets をワイド→馬連→複勝→単勝→馬単→三連複→三連単の優先順序でsortするよう変更。影響: `scripts/notify_discord.py`, `src/ml/bet_generator.py` |
| 2026-05-19 | 収支管理テーブル切れ根本修正: ① AppShell ビューラッパーから `overflow-x-hidden` を除去（`.app-main` が既に外側を守るため二重clipping不要）。② FinancialDashboard/HitHistory/ConditionAnalysis/TabView/RaceTable の `neon-card overflow-hidden` を `neon-card` に変更（`overflow:hidden` が子の `overflow-x:auto` スクロールを遮断していた根本原因）。③ 各テーブルを `<div className="w-full overflow-x-auto">` で直接ラップ（`table-scroll` の `max-height:72vh` 垂直スクロールバーがテーブル幅を圧迫する副作用も解消）。④ FinancialDashboard `RaceList` の `rounded overflow-hidden` → `overflow-x-auto` に変更。⑤ 全テーブル th/td に `whitespace-nowrap` + 各列に `min-w-[Npx]` 付与。`.next` クリーンビルド。影響: `web/src/components/AppShell.tsx`, `web/src/components/FinancialDashboard.tsx`, `web/src/components/HitHistory.tsx`, `web/src/components/ConditionAnalysis.tsx`, `web/src/components/TabView.tsx`, `web/src/components/RaceTable.tsx`, `web/src/app/globals.css` |
| 2026-05-19 | PC/スマホ完全レスポンシブ対応・Blowout根本治療: ① AppShell の win5/gachi/condition/analytics ビューに `min-w-0 w-full max-w-full overflow-x-hidden` ラッパーを追加（これが根本原因・ラッパーなしで直接 app-main に配置されていた）。② HitHistory ルート div の `max-w-[1400px]` → `w-full min-w-0` に修正。③ FinancialDashboard/TabView/Win5Panel/GachiHits/ConditionAnalysis の各ルート div に `w-full min-w-0` 追加。④ DrillDownAnalytics のルートコンテナに `width:100%; minWidth:0; overflowX:hidden` + 内側に maxWidth:900 を移動。⑤ KPI カードグリッドを `grid-cols-2 xl:grid-cols-4` → `grid-cols-1 sm:grid-cols-2 xl:grid-cols-4` にレスポンシブ改善。⑥ TableScroll に `min-width:0; width:100%` 追加。⑦ AllRacesTable/ConditionAnalysis テーブルに `minWidth:640px` 設定（内部スクロール対応）。`.next` クリーンビルド実施。影響: `web/src/components/AppShell.tsx`, `web/src/components/HitHistory.tsx`, `web/src/components/FinancialDashboard.tsx`, `web/src/components/TabView.tsx`, `web/src/components/DrillDownAnalytics.tsx`, `web/src/components/Win5Panel.tsx`, `web/src/components/GachiHits.tsx`, `web/src/components/ConditionAnalysis.tsx`, `web/src/app/globals.css` |
| 2026-05-18 | Race Explorer全期間表示バグ修正: `/api/race-list` 新規エンドポイント作成（results/payoutsなし・limit=20000・全6年分18,624件を返却）。AppShell.tsx に `raceList: Race[]` 状態を追加し RaceTree へ渡す（従来の `races: RaceEntry[]` は TabView 用として分離維持）。歴史レース選択時は `meta.date` で `/api/races?date=XX` をオンデマンドフェッチする `handleSelectRace` に変更。RaceTree.tsx の Props 型を `RaceEntry[]` → `Race[]` に変更（results/payoutsに依存しないため互換）。html要素に `overflow-x: hidden` 追加（Blowoutバグ根治）。影響: `web/src/app/api/race-list/route.ts`(新規), `web/src/components/AppShell.tsx`, `web/src/components/RaceTree.tsx`, `web/src/app/globals.css` |
| 2026-05-18 | 横幅Blowoutバグ修正: `grid-template-columns: 260px 1fr` → `minmax(0,1fr)`（CSS Grid 1frが子要素で押し広げられる根本問題）。`.app-main`に`overflow-x:hidden; min-width:0`追加。AppShell各ビューコンテナに`max-w-full`追加。`ProfitHeatmap` SVGを固定ピクセル幅(`width:Math.min(W,900)`)→`width:100% max-width:`に修正。RaceTreeのuseState初期化バグ修正: `races=[]`時の初期化で年・日付が展開されない問題を`useEffect`+`useRef`で解決し最新年・日付を自動展開。`useMemo`内の副作用を`useEffect`に移行。影響: `web/src/app/globals.css`, `web/src/components/AppShell.tsx`, `web/src/components/RaceTree.tsx`, `web/src/components/FinancialDashboard.tsx` |
| 2026-05-18 | Race Explorer過去データ表示バグ修正: `/api/predictions` のLIMIT 1000→50000（5/17当日1,176件でLIMIT消化→5/16以前の予想がゼロになる致命バグ）。`/api/races` のLIMIT 500→2000（将来的なデータ増加対応）。`RaceTree.formatDate()` を正規表現`/[-/]/`で分割しYYYY-MM-DD形式に対応（スラッシュ期待バグ）。.nextクリーンビルド実施。影響: `web/src/app/api/predictions/route.ts`, `web/src/app/api/races/route.ts`, `web/src/components/RaceTree.tsx` |
| 2026-05-13 | WIN5タブ予実比較実装: Win5Panel.tsx にactual_numbers/per_race_hit表示追加。各レース行に「AI予想バッジ(SABC)＋確定1着馬番★」「的中✓/外れ✗バッジ」。全体サマリー（的中回数・累計払戻）追加。影響: `web/src/components/Win5Panel.tsx`, `web/src/app/api/win5/route.ts` |
| 2026-05-13 | モバイルスクロール完全修正: `.app-main/.app-sidebar` に `min-width:0; overflow-x:hidden; max-width:100vw` を追加してCSS Grid min-widthバグを根本解決。AppShellのコンテンツラッパーに `min-w-0 overflow-x-hidden` 追加。影響: `globals.css`, `AppShell.tsx` |
| 2026-05-12 | モバイルUIレイアウト崩壊修正: NavBar nav links を `hidden md:flex` でスマホ非表示化・FinancialDashboard モデルボタンを `text-xs px-2 py-1 sm:text-sm sm:px-4 sm:py-2` でレスポンシブ化・TabViewタブバーに `overflow-x-auto shrink-0 whitespace-nowrap`・HitHistory モデルフィルターの `ml-auto` を削除して `flex-wrap` 化・globals.css に `flex-shrink: 0` 追加。影響: `NavBar.tsx`, `FinancialDashboard.tsx`, `TabView.tsx`, `HitHistory.tsx`, `globals.css` |
| 2026-05-12 | モバイル完全最適化: PredictionsPanel/RaceDetail/RaceTable の全テーブルをモバイルカード化。モデルタブ（ALPHA/卍/本命）・ベットスリップカード・EV大型表示(2rem)・44px タップ領域。影響: `web/src/components/PredictionsPanel.tsx`, `RaceDetail.tsx`, `RaceTable.tsx`, `web/src/app/globals.css` |
| 2026-05-10 | 初版作成。Discord 3セクション Embed レイアウト・Next.js ダッシュボード仕様記述 |
| 2026-05-10 | Hit Flash（的中速報）追加: `fetch_race_result.py:_send_hit_flash()` — 評価完了直後に予想チャンネルへ Embed 送信。的中あり=🎉予想ch/なし=🏁システムch |
| 2026-05-11 | PWA化: manifest.json / Service Worker / SwRegister.tsx / offline.html / アイコン4サイズ。影響: `web/public/` 全体・`web/src/app/layout.tsx` |
| 2026-05-11 | モバイルアクセス基盤: Tailscale VPN方式に変更。Next.js を 0.0.0.0:3000 バインド / Firewall開放スクリプト (`open_firewall_3000.ps1`) / 自動起動 (`install_autostart.ps1`) / Cloudflare関連スクリプト削除 |

---

## 1. Discord 通知設計

### 1-1. チャンネル構成

| 環境変数 | 用途 |
|---------|------|
| `DISCORD_WEBHOOK_URL` | **予想チャンネル**: 直前予想・結果・週次レポート |
| `DISCORD_SYSTEM_WEBHOOK_URL` | **システムチャンネル**: 起動/停止・エラー・自己修復ログ |

SYSTEM チャンネル未設定時は WEBHOOK_URL へ fallback。

---

### 1-2. 直前予想 Embed（3セクション分離形式）

**実装**: `src/notification/discord_notifier.py` — `notify_prerace_result()`

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 🏇 東京11R  ＮＨＫマイルカップ
  最大EV: 2.34  推奨投資合計: ¥4,500
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🟦 __ALPHA 予想  (期待値特化)__
   ​
🔥 三連複  EV=2.34  ¥1,500
  ▶ 軸: 5番 ダイヤモンドノット
    相手: 3番 / 9番
    計3点

🔥 複勝  EV=1.82  ¥1,000
  ⬛ 5番 ダイヤモンドノット

🟩 __卍 予想  (回収率特化)__
   ​
🔥 三連単  EV=1.95  ¥600
  ▶ 5番 → 9番 → 3番
  ▶ 5番 → 3番 → 9番
    (+1組)

🟥 __本命 予想  (勝率特化)__
   ​
🔥 馬連  EV=1.45  ¥800
  ⬛ 5番 ダイヤモンドノット
  ⬛ 9番 カヴァレリッツォ
```

**Embed カラーコード**:
- EV >= 3.0: `0xFFD700` (ゴールド / JACKPOT)
- EV >= 1.5: `0xFF6B35` (オレンジ / BIG)
- EV >= 0.0: `0x4ECDC4` (ティール / NORMAL)

**カードフォーマット** (`_format_combo_card`):
- 単勝/複勝: `⬛ N番 馬名`
- 三連複: `▶ 軸: N番 馬名\n  相手: A番 / B番\n  計X点`
- 三連単: `▶ A番 → B番 → C番\n  (+N組)`

**表示件数上限** (per section):
- ALPHA: 3件
- 卍: 3件
- 本命: 3件
- Oracle/HitFocus: 2件 (オプション表示)

**スキップ条件**: 全モデル EV <= 0 の場合、Discord 通知を送信しない。

---

### 1-3. Hit Flash（的中速報）— レース単位リアルタイム通知

**実装**: `scripts/fetch_race_result.py:_send_hit_flash(result, race_name)`  
**送信先**: 予想チャンネル (`DISCORD_WEBHOOK_URL`)  
**タイミング**: `fetch_single_race()` 内の `Evaluator.evaluate_race()` 完了直後

```
🎉 的中速報！  東京 11R ＮＨＫマイルカップ
**三連複**  5-9-3  ¥28,400  (投資¥1,500 / 利益+¥26,900)
**複勝**    5      ¥380    (投資¥500 / 利益-¥120)
─────────────────────────────────────
投資合計 ¥2,000  払戻合計 ¥28,780  ROI 1439.0%
```

外れた場合:
```
🏁 完走速報  東京 11R ＮＨＫマイルカップ
的中なし
─────────────────────────────────────
投資合計 ¥3,500
```

**カラー閾値** (Embed color):
- 払戻合計 ≥ ¥100,000: `0xFF4500` (赤橙 / 万馬券)
- 払戻合計 ≥ ¥10,000: `0xFFD700` (金 / 高配当)
- 払戻合計 < ¥10,000: `0x43B581` (緑 / 通常)
- 外れ: `0x555555` (グレー)

---

### 1-4. 結果速報 / 的中サマリー

**実装**: `notify_hit_summary()` / `notify_ror_warning()`

```
✅ 的中！ 東京11R ＮＨＫマイルカップ
  三連複 5-9-3: ¥28,400 (EV=1.89 → 実際2.52)
  損益: +¥26,900

⚠️ ROI 警告: 直近20件の回収率 68.3% (<80% 閾値)
```

---

### 1-4. システム通知（SYSTEM チャンネル）

| イベント | メッセージ例 |
|---------|------------|
| 起動 | `🚀 [UMALOGI] 週次オートパイロット 起動` |
| heartbeat (毎時) | `💚 [heartbeat] scheduler 正常稼働中` |
| 自己修復発動 | `⚠️ [自己修復] メタデータ異常を検知: 3レース → repair 実行` |
| エラー/クラッシュ | `❌ [エラー] prerace 失敗: {race_id}` |
| 週次スリープ | `💤 [スリープ] 次の起動: 2026-05-15 20:00 (金曜夜間バッチ)` |

---

## 2. Next.js ダッシュボード設計

**ディレクトリ**: `web/`  
**フレームワーク**: Next.js (App Router)  
**スタイル**: Tailwind CSS (ダークテーマ)  
**データ形式**: 静的 JSON (`web/src/data/`)

### 2-1. データファイル構成

```
web/src/data/
  races.json              # 全レース一覧 (meta情報 + predictions summary)
  races/
    {race_id}.json        # レース別詳細 (全モデルの買い目 + 結果)
```

**生成**: `web/generate_data.py` で `umalogi.db` から生成  
**更新タイミング**: 各 prerace/postrace 完了後 / 週次バッチ後

### 2-2. ページ構成

| ページ | パス | 説明 |
|-------|------|------|
| トップ | `/` | 本日のレース一覧・予想サマリー |
| レース詳細 | `/race/[race_id]` | 3モデル予想・買い目・結果 |
| 予想パネル | `PredictionsPanel` コンポーネント | レース別の予想一覧 |

### 2-3. PredictionsPanel コンポーネント

**ファイル**: `web/src/components/PredictionsPanel.tsx`

表示内容:
- モデルタイプ別タブ (ALPHA / 卍 / 本命)
- 買い目テーブル (馬番・馬名・EV・推奨投資額)
- 的中/外れ結果バッジ (postrace 後)
- 損益サマリー

**モバイルレスポンシブ仕様** (768px 未満):

| 要素 | デスクトップ | モバイル |
|------|------------|--------|
| 予想一覧 | 横テーブル（9列） | モデルタブ + ベットスリップカード（縦積み） |
| EV表示 | `0.9rem` インライン | `2rem` 大型 LED 数字（右上固定）|
| 馬番 | `24px` サークル | `34px` 大型サークル |
| タブ高さ | N/A | 44px 以上（タップ領域確保）|
| 出走表 | 横テーブル（14列）| 着順カード（左メダル + 右詳細）|
| AI直前 | 横テーブル（9列）| 馬ごとEVカード（EV右上・指標行）|

**カラーコード（モデルタブ左ボーダー）**:
- ALPHA: `--neon-cyan` (#00c8ff)
- 卍: `--neon-gold` (#ffd700)
- 本命: `--neon-green` (#00ff88)
- 的中カード: ゴールドボーダーに昇格 + 薄ゴールド背景

---

## 3. 外部アクセス

### 3-1. ローカル開発サーバー

```bash
cd web && npm run dev   # http://localhost:3000
```

### 3-2. 本番公開 (予定)

- Vercel または GitHub Pages へのデプロイ
- データは静的 JSON を配信 (DB は直接公開しない)
- トンネル経由のローカル公開: `scripts/start_tunnel.py` (ngrok/cloudflared)

---

## 4. note / SNS 連携

| スクリプト | 出力 |
|-----------|------|
| `scripts/generate_note_article.py` | レース別 note 記事 (Markdown → HTML) |
| `scripts/generate_sns_post.py` | X (Twitter) 投稿文 (パターンA/B) |
| `scripts/generate_result_card.py` | 的中カード画像 (Pillow 製) |

**生成条件**:
- 注目レース (重賞・G1 or EV >= 5.0) の prerace 完了後に記事を先行生成
- 的中時に結果カード画像を自動生成して Discord に添付
