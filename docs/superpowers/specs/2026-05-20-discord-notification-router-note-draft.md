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
