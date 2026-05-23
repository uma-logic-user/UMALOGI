# 設計書: Discord 通知アーキテクチャ刷新 & note下書き転送ワークフロー

- **作成日**: 2026-05-20
- **ステータス**: 承認済み
- **担当フェーズ**: 平日改修（条項2適用）

---

## 概要

2つの機能を同一実装サイクルで実装する。

1. **通知ルーター層の新設** (`src/notification/router.py`): 用途別マルチWebhookルーティングを担う `NotificationRouter` クラスを新設し、`DiscordNotifier` を純粋な送信機として整理する。
2. **note下書き Discord 転送ワークフロー**: `post_weekly_note_draft.py` に Discord 転送ロジックを追加し、Playwright 自動投稿を `ENABLE_PLAYWRIGHT_POST` フラグで制御する。

---

## セクション①: NotificationRouter アーキテクチャ

### 責務分割

| クラス | ファイル | 責務 | 変更種別 |
|---|---|---|---|
| `DiscordNotifier` | `src/notification/discord_notifier.py` | 1つの Webhook URL への HTTP 送信 + Embed/テキスト構築 + ビジネスロジックメソッド | 既存維持・軽微調整 |
| `NotificationRouter` | `src/notification/router.py` | チャンネル選択・フォールバック・新機能（EV激熱/A/B比較/note下書き） | **新設** |

`DiscordNotifier` は現在のメソッド（`notify_prerace_result`, `send_system_embed` 等）を維持する。
`NotificationRouter` は複数の `DiscordNotifier` インスタンスを保持し、チャンネルへの振り分けとフォールバック制御を担う。

### チャンネルマップと環境変数

```python
CHANNEL_ENV: dict[str, str] = {
    "prediction":  "DISCORD_WEBHOOK_URL",          # 既存・フォールバック基準
    "system":      "DISCORD_WEBHOOK_SYSTEM",        # 新設（旧: DISCORD_SYSTEM_WEBHOOK_URL から移行）
    "ev_alert":    "DISCORD_WEBHOOK_EV_ALERT",      # 新設: EV>=1.5 の激熱レース専用
    "ab_test":     "DISCORD_WEBHOOK_AB_TEST",       # 新設: V1/V2 週次成績比較レポート
    "note_draft":  "DISCORD_WEBHOOK_NOTE_DRAFT",    # 新設: note下書き出力用
}
```

**後方互換**: `DISCORD_SYSTEM_WEBHOOK_URL`（旧変数）が設定されている場合は `system` チャンネルとして読み込む。

### フォールバック規則

```
チャンネル設定あり   → そのチャンネルへ送信
チャンネル未設定     → "prediction" チャンネルへフォールバック
"prediction" も未設定 → WARNING ログのみ、例外なし
```

### `NotificationRouter` の公開インターフェース

```python
class NotificationRouter:
    # ── チャンネル取得 ──────────────────────────────────────────────
    def _get(self, channel: str) -> DiscordNotifier | None: ...

    # ── prediction チャンネル（既存ロジックを委譲） ────────────────
    def notify_prerace_result(self, race_id, honmei_bets, manji_bets, **kwargs) -> None:
        """prediction へ通知。max_ev >= 1.5 なら ev_alert へも追加送信。"""

    def notify_hit_summary(self, ...) -> None: ...
    def notify_skip(self, race_id, reason) -> None: ...
    def send_text(self, text: str) -> None: ...

    # ── system チャンネル ──────────────────────────────────────────
    def send_system_text(self, text: str) -> None: ...
    def send_system_embed(self, title, description, **kwargs) -> None: ...
    def notify_scraping_alert(self, race_id, detail) -> None: ...
    def notify_intervention_required(self, step, error, action, screenshot_path=None) -> None: ...
    def notify_ror_warning(self, warning_text) -> None: ...

    # ── ev_alert チャンネル（新機能） ──────────────────────────────
    def notify_ev_alert(self, race_id: str, max_ev: float, bets_summary: str) -> None:
        """EV >= EV_ALERT_THRESHOLD の激熱レースを @everyone 付きで通知する。"""

    # ── ab_test チャンネル（新機能） ───────────────────────────────
    def send_ab_report(self, report_md: str) -> None:
        """V1 vs V2 週次 A/B 成績比較レポートを ab_test チャンネルへ送信する。"""

    # ── note_draft チャンネル（新機能） ────────────────────────────
    def send_note_draft(self, title: str, body: str, x_post: str | None = None) -> bool:
        """
        note下書きをチャンク分割して note_draft チャンネルへ順番送信する。
        その後 x_post があれば X告知ポストとして追加送信する。
        """
```

### EV激熱アラートのトリガー条件

`notify_prerace_result()` 内で以下の両方を満たす場合に `ev_alert` チャンネルへ追加送信する:
- `max_ev >= 1.5`（`_COLOR_BIG` 相当以上）
- `prediction` チャンネルと異なる URL が `ev_alert` に設定されている

`ev_alert` 未設定の場合はスキップ（フォールバックして prediction に2回送らない）。

### 呼び出し元の変更方針

| ファイル | 変更内容 |
|---|---|
| `src/pipeline/prediction.py` | `DiscordNotifier()` → `NotificationRouter()` に置換 |
| `scripts/today_auto_runner.py` | ローカル `_send_discord()` → `NotificationRouter().send_system_text()` |
| `scripts/scheduler.py` | ローカル `_send_discord()` / `_send_discord_embed()` → `NotificationRouter()` 経由に統一 |

---

## セクション②: note下書き Discord 転送ワークフロー

### 処理フロー

```
post_weekly_note_draft.py main()
  ├── [Step 1] セッション確認（既存・変更なし）
  ├── [Step 2] generate_weekly_note() → (title, body)
  ├── [Step 3-A] router.send_note_draft(title, body, x_post)  ← 新規追加
  │     ├── chunk_for_discord(body)  → list[str]
  │     ├── 各チャンク: 【note下書き (N/M)】 + ```markdown\n...\n```
  │     ├── x_post = _generate_x_post(title, body)
  │     └── 末尾: X告知ポストを ```markdown ... ``` で送信
  └── [Step 3-B] publish_via_playwright(title, body)  ← ENABLE_PLAYWRIGHT_POST=True 時のみ
```

### チャンク分割アルゴリズム (`_chunk_text`)

- **最大コンテンツ長**: 1800文字（2000文字制限から ```markdown ヘッダー・ページング番号分を引いたマージン）
- **分割優先順位**:
  1. `\n\n`（段落区切り）での分割
  2. `\n`（行区切り）での分割
  3. ハードカット（上記2つで 1800 文字以内に収まらない場合）

### ページング表記

各チャンクメッセージの先頭行にページング番号を付与する:

```
【note下書き (1/3)】
```markdown
# 🏇【UMALOGI週次レポート】...
...
```
```

最終チャンクの末尾: `\n_（以上）_` を付与して終端を明示する。

### X告知ポスト自動生成 (`_generate_x_post`)

note記事 Markdown から以下を抽出してポストテキストを構築する:
- `title` から先頭40文字（号数を含む）
- 本文先頭の `##` ヘッドラインからサブタイトル
- 140文字以内に収まるよう末尾をトリム
- 固定ハッシュタグ: `#競馬 #AI予想 #UMALOGI #JRA`

出力例:
```
🏇 UMALOGI週次レポート2026-05-18号
万馬券3本的中！ALPHAモデルROI203%達成

noteで全モデル成績公開中📊

#競馬 #AI予想 #UMALOGI #JRA
```

### フィーチャートグル (`ENABLE_PLAYWRIGHT_POST`)

| `.env` 値 | 動作 |
|---|---|
| 未設定（デフォルト） | Discord 転送のみ（Playwright スキップ） |
| `False` / `0` | Discord 転送のみ（Playwright スキップ） |
| `True` / `1` | Discord 転送 + Playwright 投稿の両方実行 |

`publish_via_playwright(title, body)` 関数に既存の `save_draft()` 呼び出しをカプセル化する。

### 最終ログ出力（処理モード明示）

```
[INFO] Discord note-draft 送信完了: 4チャンク + X告知ポスト1件 → DISCORD_WEBHOOK_NOTE_DRAFT
[INFO] Playwright投稿: 設定によりスキップ (ENABLE_PLAYWRIGHT_POST=False)
```

または:

```
[INFO] Discord note-draft 送信完了: 4チャンク + X告知ポスト1件 → DISCORD_WEBHOOK_NOTE_DRAFT
[INFO] Playwright投稿: 実行中 (ENABLE_PLAYWRIGHT_POST=True)
[INFO] note.com 下書き保存 完了
```

---

## セクション③: 環境変数テンプレート (`2.env`) 更新

```
# ── Discord Webhook URLs ────────────────────────────────────────
DISCORD_WEBHOOK_URL=              # 予想・結果・週次レポート（必須・フォールバック基準）
DISCORD_WEBHOOK_SYSTEM=           # システムログ・エラー（旧: DISCORD_SYSTEM_WEBHOOK_URL）
DISCORD_WEBHOOK_EV_ALERT=         # EV>=1.5 激熱レース専用（未設定時はpredictionへfallback）
DISCORD_WEBHOOK_AB_TEST=          # V1/V2 週次A/Bテスト比較レポート（未設定時はpredictionへfallback）
DISCORD_WEBHOOK_NOTE_DRAFT=       # note下書き出力（未設定時はシステムログ出力のみ）

# ── 旧変数（後方互換のため継続サポート） ─────────────────────────
# DISCORD_SYSTEM_WEBHOOK_URL=     # → DISCORD_WEBHOOK_SYSTEM として読み込み

# ── 通知有効フラグ ─────────────────────────────────────────────
NOTIFY_DISCORD=1
NOTIFY_LINE=1
NOTIFY_TWITTER=0

# ── note下書き投稿モード ──────────────────────────────────────
ENABLE_PLAYWRIGHT_POST=           # True にすると Playwright 自動投稿も実行（デフォルトOFF）
```

---

## セクション④: テスト計画

### 新設テストファイル: `tests/notification/test_router.py`

| テストケース | 検証内容 |
|---|---|
| `test_fallback_to_prediction` | ev_alert 未設定時に prediction チャンネルへフォールバック |
| `test_ev_alert_routes_separately` | max_ev >= 1.5 かつ ev_alert 設定済みで ev_alert チャンネルへ別送 |
| `test_send_note_draft_chunking` | 3000文字の本文が複数チャンクに分割されページング付きで送信される |
| `test_send_note_draft_x_post` | x_post が指定されたとき末尾メッセージとして送信される |
| `test_enable_playwright_toggle` | ENABLE_PLAYWRIGHT_POST=False でも Discord 転送は正常完了する |
| `test_all_channels_unset` | 全 URL 未設定でも例外が発生しない（ログのみで安全スキップ） |

---

## 変更ファイル一覧

| ファイル | 種別 | 変更内容 |
|---|---|---|
| `src/notification/router.py` | 新設 | `NotificationRouter` クラス |
| `src/notification/discord_notifier.py` | 軽微修正 | `_COLOR_*` 定数をモジュール公開、後方互換は維持 |
| `scripts/post_weekly_note_draft.py` | 改修 | Discord 転送 + フィーチャートグル追加 |
| `src/pipeline/prediction.py` | 改修 | `DiscordNotifier()` → `NotificationRouter()` |
| `scripts/today_auto_runner.py` | 改修 | ローカル `_send_discord` → `NotificationRouter` |
| `scripts/scheduler.py` | 改修 | ローカル `_send_discord*` → `NotificationRouter` |
| `2.env` | 更新 | 新規環境変数を追記 |
| `tests/notification/test_router.py` | 新設 | ルーター単体テスト |

---

## 制約・注意事項

- `DiscordNotifier` の既存メソッドシグネチャは変更しない（後方互換維持）
- `predictions` テーブルへの書き込みは一切行わない（予測不変性条項1）
- 土日は大規模リファクタ禁止のため、本実装は平日のみ実施（条項2）
- DB 操作は一切含まない

---

## 【絶対厳守】UI実装の制約事項（2026-05-20 社長指令）

> この制約はUMALOGI全体の永続ルールであり、今回の Discord 通知改修に限らず、
> 将来のあらゆる実装フェーズにも適用される。

### ルール1: 既存4大モデルUIの完全凍結

以下のコンポーネント・APIエンドポイントは **1行も変更してはならない**。

| 保護対象 | 対応ファイル |
|---|---|
| 本命予想タブの表示・レイアウト | `web/src/components/RaceTable.tsx` 等 |
| 卍予想タブの表示・レイアウト | 同上 |
| ALPHA予想タブの表示・レイアウト | 同上 |
| oracle予想タブの表示・レイアウト | 同上 |
| 予想タブ全体のレイアウト | `web/src/components/TabView.tsx` |
| 予想系 API エンドポイント | `web/src/app/api/predictions/` 配下 |

**禁止事項**:
- 既存タブの中に新モデルの表示要素を「追加」すること
- 既存APIのレスポンス形式を変えること
- 既存コンポーネントに props を追加してロジック分岐を混ぜ込むこと

### ルール2: 新規予想方法は「完全独立タブ」に隔離

FukushoElite・X シグナル統合・Phase C モデルなど、新しい予想方法を UI に追加する場合は:

1. `web/src/components/` に **新規コンポーネントファイルを作成**（既存ファイルは変更しない）
2. `web/src/components/TabView.tsx` には**タブの追加エントリのみ**を加える（既存エントリは変更しない）
3. 新規タブの API は **新規エンドポイント**として `web/src/app/api/` 配下に独立して作成する

**実装パターン（例: FukushoElite 本番統合時）**:
```
新設: web/src/components/FukushoElitePanel.tsx   ← 完全新規
変更: web/src/components/TabView.tsx             ← タブ追加エントリのみ（1〜2行）
新設: web/src/app/api/fukusho-elite/route.ts     ← 完全新規
禁止: RaceTable.tsx など既存コンポーネントへの変更  ← 絶対禁止
```

### ルール3: 今回の Discord 通知改修は UI 無関係

本スペック（Discord ルーター + note下書き転送）は**フロントエンドコードへの変更をゼロとする**。
`web/` 配下のいかなるファイルも変更しないこと。

---

## 追加機能仕様（2026-05-20 第2次承認）

### 機能A: 「組」→「点」 表記統一（バグフィックス）

`src/notification/discord_notifier.py:493` の1行を修正する。

```python
# 変更前
lines.append(f"  (+{n_total - 4}組)")
# 変更後
lines.append(f"  (+{n_total - 4}点)")
```

影響箇所: `_format_combo_card()` 内の馬単・三連単の「溢れ組表示」のみ。他の「組」出現はコメント・docstring のみで影響なし。

### 機能B: IS_PREMIUM_NOTE フラグ（週次記事有料/無料出し分け）

**環境変数**: `NOTE_IS_PREMIUM=0`（デフォルト False、`1` または `true` で有料版）

`generate_weekly_note(conn, *, is_premium: bool = False)` にパラメータ追加。

| フラグ | 生成コンテンツ |
|---|---|
| `False`（無料版） | モデル実績・万馬券・注目的中・軸馬1頭・単複基本買い目 + セパレーター |
| `True`（有料版） | + oracle EV上位3買い目・三連複フォーメーション詳細・全QFピック（馬番+馬名+騎手+人気） |

**セパレーター文字列**（無料版の末尾・有料版コンテンツ先頭に挿入）:
```
---
【有料エリア設定箇所：ここから下はnoteの有料ブロックへ貼り付けてください】
---
```

`post_weekly_note_draft.py` で `os.getenv("NOTE_IS_PREMIUM", "0").lower() in ("1", "true")` を読んで `generate_weekly_note(..., is_premium=val)` に渡す。

### 機能C: 買い方テンプレート（Discord 直前予想への追加）

`src/notification/router.py` に `_format_buying_guide(honmei_bets, manji_bets, alpha_bets)` 関数を追加。`notify_prerace_result` の embed 送信後、prediction チャンネルへ別メッセージとして送信する。

**抽出ロジック（優先順位）:**
- 単勝・複勝: `honmei_bets.bets` から `bet_type in ("単勝", "複勝")` の最高EV買い目の馬番+馬名
- 馬連: `manji_bets.bets` から `bet_type == "馬連"` の最高EV買い目の軸+相手
- 三連複: `alpha_bets` が None なら `manji_bets`、`bet_type == "三連複"` の最高EV から軸+相手

**出力フォーマット:**
```
【💡 推奨される買い方サマリー】

■ 単複で手堅く行くなら
・単勝：5番 アーバンシック（1点）
・複勝：5番 アーバンシック（1点）

■ 馬連で中穴・好配当を狙うなら
・馬連 軸流し：5番 アーバンシック → 相手：3番 レガシー、7番 サクセス、9番 キタノ（計3点）

■ 三連複で高配当（万馬券）を狙うなら
・三連複 軸1頭流し：5番 アーバンシック → 相手：3番 レガシー、7番 サクセス、9番 キタノ、12番 ホープ（計6点）
```

セクションが取れない場合（該当モデルが None または bet_type が存在しない）はそのセクションのみ省略。全セクション空の場合は送信スキップ。

---

## 商用化フェーズ ロードマップ（Task 17以降・2026-05-20 追記）

> 以下は現行実装スプリント完了後のフェーズ（Task 17+）として計画する。

### Task 17: 2カ年厳選黒字化シミュレーター（`scripts/run_2year_backtest.py`）

**目的**: 卍・本命・ALPHA・oracle の予測値と確定払戻を突き合わせ、ROI 100%+を保証する「厳選購入条件」の閾値を自動炙り出し。

**データソース**: 既存 `simulate_year.py` のインフラを流用し、`predictions` + `prediction_results` + `race_payouts` + `races` テーブルを使用。

**検証パターン（自動グリッドサーチ対象）:**

| パターン | 条件 | 目的 |
|---|---|---|
| A | oracle EV ≥ 1.5 かつ ALPHA EV ≥ 1.5 | コンセンサス銘柄 |
| B | 単勝オッズ 5〜25倍 かつ 卍スコア ≥ 閾値 | 中穴狙い |
| C | 全モデル EV ≥ 1.0 の超厳選（1日3〜5レース） | 聖杯探索 |

**出力**: `data/backtest_{YYYYMMDD}.csv` + `docs/backtest_2year_report.md`

### Task 18: 万馬券特化的中報告（`scripts/generate_result_note_draft.py`）

**抽出条件**: `prediction_results.payout >= 10000` OR `prediction_results.payout / 100 >= 3.0`（回収率 300%以上）

**生成物**: Markdown「万馬券炸裂レポート」→ `DISCORD_WEBHOOK_NOTE_DRAFT` チャンネルへ転送

**フォーマット:**
```markdown
# 🎰 【万馬券炸裂】UMALOGI AI 神的中レポート

## 🏆 {日付} {レース名} — {払戻金額}
- モデル: {model_type}
- 券種: {bet_type}
- 軸馬: {馬番}番 {馬名}
- 払戻: ¥{payout:,}（回収率 {roi:.0f}%）
```

### Task 19: `docs/commercialization_roadmap.md` 更新

2カ年シミュレーター・万馬券特化報告・有料/無料分離の進捗を商用化ロードマップに反映。
