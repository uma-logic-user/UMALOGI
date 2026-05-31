# UMALOGI 収益化ロードマップ v2
**作成日**: 2026-05-05  
**ステータス**: 設計確定・実装待ち  
**戦略**: リスクゼロ情報コンテンツ販売 — ウマニティ × X × note 3本柱エコシステム

---

## 0. 戦略前提：なぜ「3本柱同時並走」なのか

### 0-1. 各プラットフォームの役割分担

```
ウマニティ（予想コロシアム）
  → 役割: 第三者機関による「公式実績」の積み上げ
  → KPI: ランキング上位 / 回収率公開
  → 集客への貢献: 低（クローズドユーザー向け）

X（旧 Twitter）
  → 役割: バイラル拡散・note への集客導線
  → KPI: フォロワー数 / インプレッション / note クリック率
  → 収益への直接貢献: なし（完全無料）

note
  → 役割: 唯一の収益源
  → KPI: 有料記事購読者数 / 月額マガジン購読者数
  → 目標: 月10万円（100名購読 × ¥1,000 相当）
```

### 0-2. エコシステムの流れ

```
[UMALOGIが予想生成]
      │
      ├─ ウマニティに予想登録 → 実績ランキング蓄積
      │        │
      │        └─（毎月）実績スクリーンショット → X に投稿
      │
      ├─ note 下書き保存 → [社長が公開ボタンを押す] → 有料記事公開
      │        │
      │        └─ note URL を X に自動投稿（集客）
      │
      └─ X に予想概要・的中報告 → フォロワー獲得 → note 誘導
```

---

## 1. アーキテクチャ概要：週末バッチ一括連動型

### 1-1. 設計原則

1. **既存の `scheduler.py` に乗せる** — 新規インフラは一切追加しない
2. **独立障害** — 各プラットフォームへの処理は `try/except` で独立。1つの失敗が他をブロックしない
3. **失敗即通知** — エラー時は Discord Webhook で「手動介入要請」を即時送信
4. **Pre/Post 分離** — レース前後でトリガーを明確に分割

### 1-2. バッチ処理の全体像

```
WeekendBatchOrchestrator（新規実装: scripts/weekend_batch.py）
│
├── [PRE-RACE] 土曜 07:00 / 日曜 07:00（発走前）
│   │
│   ├── Step 1: note 記事生成
│   │     generate_note_article.py → outputs/note/YYYYMMDD_R*.md
│   │
│   ├── Step 2: note 下書き保存（半自動）
│   │     note_draft_publisher.py（Playwright）
│   │     → ✅ 下書き保存完了 → Discord に「公開ボタン待ち」通知
│   │     → ❌ 失敗 → Discord に「手動投稿要請」通知
│   │
│   ├── Step 3: ウマニティ予想投稿
│   │     umanity_uploader.py（EV >= 1.0 の買い目のみ）
│   │     → ✅ 投稿成功
│   │     → ❌ 失敗 → Discord に「手動介入要請」通知 + スクリーンショット添付
│   │
│   └── Step 4: X 事前告知ツイート（1件/日）
│         generate_sns_post.py（パターン A）
│         → twitter_notifier.py で投稿
│         → ❌ 失敗 → Discord 通知（X API 制限確認を促す）
│
└── [POST-RACE] 土曜 18:30 / 日曜 18:30（最終レース後）
    │
    ├── Step 1: レース結果取得
    │     fetch_confirmed_se.py（確定 SE データ取得）
    │
    ├── Step 2: 的中評価 + 的中カード生成
    │     generate_result_card.py（Pillow 画像生成）
    │     → outputs/cards/YYYYMMDD_R*.png
    │
    ├── Step 3: X 実績報告ツイート（的中時のみ・最大2件/日）
    │     generate_sns_post.py（パターン B）
    │     → twitter_notifier.py で画像付き投稿
    │
    └── Step 4: Discord に日次損益レポート送信
          discord_notifier.py（既存）
          → 的中数・損益合計・月次 ROI を報告
```

---

## 2. Pre/Post トリガーの詳細仕様

### 2-1. Pre-race フェーズ（07:00）

| 処理 | 入力 | 出力 | 失敗時の挙動 |
|---|---|---|---|
| note 記事生成 | DB の当日予想（EV >= 1.0） | `outputs/note/YYYYMMDD_*.md` | Discord 通知 + スキップ |
| note 下書き保存 | 上記 Markdown | note.com の下書き | Discord 「公開ボタン待ち」通知 |
| ウマニティ投稿 | DB の当日予想（EV >= 1.0） | ウマニティ予想コロシアム | Discord「手動介入要請」+ SS |
| X 事前告知 | EV 最上位レース情報 | X ポスト | Discord 通知（軽微） |

### 2-2. Post-race フェーズ（18:30）

| 処理 | 入力 | 出力 | 失敗時の挙動 |
|---|---|---|---|
| 結果取得 | JVLink SE データ | `race_results` テーブル更新 | Discord 通知（翌日補完） |
| 的中カード生成 | `prediction_results` | `outputs/cards/*.png` | Discord 通知 + スキップ |
| X 実績報告 | 的中レコード + カード画像 | X ポスト（画像付き） | Discord 通知（軽微） |
| 日次レポート | DB 集計 | Discord メッセージ | ログのみ |

### 2-3. タイムライン（土曜の例）

```
06:50  scheduler.py が weekend_batch.py を起動
07:00  [PRE] note 記事生成 → 下書き保存 → ウマニティ投稿 → X 告知
07:05  Discord: 「✅ Pre-race バッチ完了。note の公開ボタンをご確認ください。」
07:30  [社長が手動で] note の公開ボタンを押す（30秒）
~~（レース開催）~~
18:30  [POST] 結果取得 → 的中カード → X 実績報告 → Discord レポート
18:40  Discord: 「📊 本日損益: +¥X,XXX / 月次ROI: XXX%」
```

---

## 3. 技術的壁と突破方法

### 3-1. ウマニティ：DOM操作の壁

#### 現状
`umanity_uploader.py` は実装済みだが **未テスト**。DOM セレクタは推定値で書かれており、実際のページ構造と一致しない可能性が高い。

#### 壁と突破法

| 壁 | 難易度 | 突破法 |
|---|---|---|
| ログインモーダルの DOM 構造 | 中 | `--no-headless` で実際のモーダルを目視確認し、セレクタを修正 |
| 予想フォームの馬番入力 | 高 | 予想コロシアムページのチェックボックス/ラジオボタンを DevTools で特定 |
| レース ID とウマニティコードの対応 | 中 | UMALOGI 12桁 → ウマニティ 10桁変換ロジックの実動確認 |
| CAPTCHA | 低（おそらくなし） | ウマニティは一般的な競馬サイトのため CAPTCHA は想定しない |
| DOM 変更による突然の破損 | 高 | 毎回失敗時に `page.screenshot()` を Discord 添付 → 社長が確認 |

#### 実装指針
```python
# umanity_uploader.py の堅牢化方針
try:
    uploader.post_prediction(...)
except PlaywrightError as e:
    ss_path = save_screenshot(page)  # スクリーンショット保存
    notify_discord_error(
        title="⚠️ ウマニティ投稿失敗 — 手動介入要請",
        detail=str(e),
        screenshot_url=ss_path,
        action="https://umanity.jp/members/login.php にアクセスし、手動で予想を入力してください",
    )
```

#### 手動境界線
- **自動**: ログイン・予想フォーム入力・送信（成功時）
- **手動**: DOM 変更後のセレクタ修正（月1回程度を想定）

---

### 3-2. X（Twitter）：Free プランの壁

#### 制約
- **月 1,500 件投稿上限**（Read は別枠）
- 1,500件 ÷ 4週 ÷ 2日 = **約 187件/週末** → 十分な余裕
- ただし**スパム判定（アカウント凍結）** が最大リスク

#### 壁と突破法

| 壁 | 難易度 | 突破法 |
|---|---|---|
| 月1,500件上限 | 低 | 最大 6件/週末（Pre×1 + Post×2 + 月次×1）= 月24件で1%も使わない |
| スパム判定 | 中 | 下記の運用ルールを厳守 |
| 画像アップロード（v1.1 API） | 低 | `twitter_notifier.py` 実装済み |
| note URL の固定化 | 中 | 記事ごとに動的 URL を取得する仕組みが必要 |

#### スパム判定を防ぐ運用ルール（必須）

```
1. 同一文面を2回以上投稿しない
   → generate_sns_post.py の variants ランダム選択で対応済み ✅

2. 投稿間隔は最低5分以上
   → weekend_batch.py で time.sleep(300) を挿入

3. 1日の投稿上限: 最大4件（Pre×1 + Post×2 + 月次×1）
   → RateLimiter クラスで管理（DB に投稿ログを記録）

4. アカウントプロフィールを競馬予想専用に明示
   → 「AI競馬予想 / JRA-VAN データ使用 / 情報提供のみ」を明記

5. リプライ・フォロー・いいねの自動化は一切しない
   → ツイート投稿 API のみ使用
```

#### 投稿スケジュール（月間）

| タイミング | 件数 | 内容 |
|---|---|---|
| 土曜 07:00 | 1 | Pre-race 告知（パターン A） |
| 土曜 18:30 | 0〜2 | 的中報告（パターン B、的中時のみ） |
| 日曜 07:00 | 1 | Pre-race 告知（パターン A） |
| 日曜 18:30 | 0〜2 | 的中報告（パターン B、的中時のみ） |
| 月末 | 1 | 月次実績サマリー |
| **月間合計** | **約 16〜21件** | 上限 1,500件の **1.4%** |

---

### 3-3. note：APIなし問題の壁

#### 現状
note.com に公式の投稿 API は存在しない。Playwright によるブラウザ自動操作が唯一の自動化手段。

#### 壁と突破法

| 壁 | 難易度 | 突破法 |
|---|---|---|
| React ベースのエディタへの入力 | 高 | `page.fill()` ではなく `clipboard` 経由でのペースト |
| ログイン方法（SNS連携 vs メール） | 中 | メール/パスワードログインを使用（SNS OAuth はリダイレクトで制御困難） |
| 有料金額の自動設定 | 中 | `<select>` or 数値入力フィールドを特定して設定 |
| note の DOM 変更リスク | 高 | 失敗時は Discord 通知 + Markdown ファイルを添付して手動投稿誘導 |
| Markdown の「区切り線→有料エリア」の再現 | 中 | note は `---` 区切りで有料ラインを設定できる。Playwright でボタンをクリック |

#### 半自動フロー詳細

```python
# note_draft_publisher.py の処理フロー（新規実装）
#
# 1. note.com にログイン（メール/パスワード）
# 2. 「記事を書く」ボタンをクリック
# 3. タイトルを入力（race_name + 日付）
# 4. 本文エリアに Markdown をクリップボード経由でペースト
# 5. 有料金額を設定（¥300）
# 6. 「下書き保存」ボタンをクリック
# 7. 下書きURLを取得 → Discord に通知
# ※「公開する」ボタンは押さない（社長が手動で確認後に押す）
```

#### 手動境界線
- **自動**: ログイン → 記事内容入力 → 価格設定 → **下書き保存まで**
- **手動（社長）**: 内容を30秒確認 → 「公開する」ボタンを押す（週2回 × 30秒 = 週1分）

---

## 4. エラーハンドリングと監視設計

### 4-1. エラー分類と対応

| エラーレベル | 例 | 自動対応 | Discord 通知 |
|---|---|---|---|
| CRITICAL | DBが破損・予想なし | バッチ中断 | 🚨「緊急停止」全詳細 |
| HIGH | ウマニティ/note ログイン失敗 | ステップスキップ | ⚠️「手動介入要請」+ SS |
| MEDIUM | X API 制限超過 | ステップスキップ | 📢「X投稿スキップ」 |
| LOW | 画像生成失敗 | テキストのみで継続 | ℹ️「画像なしで続行」 |

### 4-2. Discord 通知の構造

```python
# src/notification/discord_notifier.py に追加するエラー通知関数
def notify_intervention_required(
    step: str,          # "ウマニティ投稿" など
    error: str,         # エラーメッセージ
    action: str,        # 社長がすべきこと
    screenshot: bytes | None = None,
) -> None:
    """
    Discord Webhook に「手動介入要請」を送信する。
    """
    payload = {
        "embeds": [{
            "title": f"⚠️ UMALOGI 手動介入要請 — {step}",
            "color": 0xFF4444,
            "fields": [
                {"name": "エラー内容", "value": f"```{error[:500]}```"},
                {"name": "対応アクション", "value": action},
                {"name": "発生時刻", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            ],
        }]
    }
    # screenshot は Discord file upload で添付
```

### 4-3. バッチ全体の冪等性保証

```python
# weekend_batch.py の処理記録テーブル（DB に追加）
CREATE TABLE IF NOT EXISTS batch_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date    TEXT NOT NULL,         -- "2026-05-03"
    phase       TEXT NOT NULL,         -- "pre" / "post"
    step        TEXT NOT NULL,         -- "note" / "umanity" / "x"
    status      TEXT NOT NULL,         -- "success" / "failed" / "skipped"
    message     TEXT,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_date, phase, step)      -- 同日同フェーズの二重実行防止
);
```

---

## 5. 手動と自動の境界線（確定版）

```
完全自動（毎週末、社長の操作不要）
  ✅ AI による予想生成（既存）
  ✅ note Markdown 記事生成
  ✅ note 下書き保存
  ✅ ウマニティへの予想投稿
  ✅ X 事前告知ツイート
  ✅ 的中カード画像生成
  ✅ X 的中報告ツイート
  ✅ Discord 日次損益レポート

半自動（社長が週2回 × 30秒）
  👆 note 記事の「公開する」ボタンを押す
     └─ Discord に「下書き完了。公開をお願いします。[URL]」通知が届いたら押すだけ

手動（月1回程度 / 問題発生時）
  🔧 ウマニティ DOM セレクタの修正（note.com のリニューアル時も同様）
  🔧 X API キーのローテーション
  🔧 note マガジンの月次設定（タイトル・説明文の更新）
  🔧 Discord アラートを受けての緊急対応
```

---

## 6. Phase 別実装計画

### Phase 1：基盤構築（〜2026年5月末、工数 6〜8日）

**目標**: 3プラットフォームのドライランが通る状態にする

| タスク | ファイル | 工数 | 優先度 |
|---|---|---|---|
| ウマニティ DOM セレクタ実動確認・修正 | `src/ops/umanity_uploader.py` | 2日 | 🔴 最高 |
| `note_draft_publisher.py` 新規実装 | `src/ops/note_draft_publisher.py` | 2日 | 🔴 最高 |
| `WeekendBatchOrchestrator` 実装（Pre/Post 分離） | `scripts/weekend_batch.py` | 1日 | 🔴 最高 |
| Discord エラー通知の統合 | `src/notification/discord_notifier.py` | 0.5日 | 🟠 高 |
| `batch_runs` テーブル追加・冪等性保証 | `src/database/init_db.py` | 0.5日 | 🟠 高 |
| X RateLimiter 実装（投稿ログ DB 記録） | `src/notification/twitter_notifier.py` | 0.5日 | 🟡 中 |
| E2E ドライラン（全ステップ `--dry-run`） | — | 0.5日 | 🟠 高 |

**Phase 1 完了基準**: `python scripts/weekend_batch.py --dry-run` が全ステップ SUCCESS で完走する

---

### Phase 2：本番稼働と実績蓄積（〜2026年6月末）

**目標**: 初めての有料 note 記事を公開し、最初の1円を稼ぐ

| タスク | 内容 | 工数 |
|---|---|---|
| Phase 1 ドライラン結果の修正 | DOM ズレ・認証エラーの対処 | 1〜2日 |
| 初回ライブ実行（1レースのみ） | 社長立ち会いで週末1回試験稼働 | — |
| note 有料マガジン設定 | ¥1,980/月のマガジン作成 | 0.5日 |
| 月次実績レポート自動生成 | `scripts/generate_monthly_report.py` | 1日 |
| SHAP 根拠文の品質向上 | `src/ml/narrative_generator.py` の出力チューニング | 1日 |

**Phase 2 完了基準**: note 有料記事が1本以上公開され、売上が1件以上発生する

---

### Phase 3：収益最大化（〜2026年9月末）

**目標**: 月10万円（マガジン購読者 50〜100名）の安定達成

| タスク | 内容 | 工数 |
|---|---|---|
| 動的価格付けロジック | EV・回収率に応じた記事価格自動調整（¥300〜¥500） | 1日 |
| 過去実績の note 記事化 | `prediction_results` の高額的中を遡及記事化 | 1日 |
| LINE 公式アカウント連携 | プレミアム会員向け直前通知（`src/notification/line_broadcast.py`） | 2日 |
| Optuna 自動チューニング | 月次モデル改善（`src/ml/auto_tune.py`） | 3日 |
| ウマニティ実績の X 月次投稿 | ランキング画像 + 回収率の月次バイラル投稿 | 0.5日 |

---

## 7. 収益モデル試算

### 7-1. 最小達成シナリオ（Phase 2 完了時、購読者20名）

```
note 単品購入（¥300/記事 × 8記事/月 × 20名） = ¥48,000
note マガジン（¥1,980/月 × 5名）              = ¥  9,900
------------------------------------------------------
月収合計                                        ¥57,900
```

### 7-2. 目標達成シナリオ（Phase 3、購読者100名）

```
note マガジン（¥1,980/月 × 80名）   = ¥158,400
note プレミアム（¥4,980/月 × 20名） = ¥ 99,600
------------------------------------------------------
月収合計                              ¥258,000
```

### 7-3. 価格設定の判断基準

```
月次回収率 >= 120%  → 翌月の記事価格: ¥500（強気）
月次回収率 100-120% → 翌月の記事価格: ¥300（標準）
月次回収率 < 100%   → 翌月の記事価格: ¥0（無料）+ 反省レポートを公開
```

> ⚠️ 「負けた月も正直に公開する」姿勢が長期信頼の根幹。マイナス月を隠すと購読離脱率が急上昇する。

---

## 8. 倫理・法務チェックリスト

- [ ] note 記事すべてに「本予想はバックテスト結果であり的中を保証しません」を明記
- [ ] 「必ず儲かる」「100%的中」等の表現を一切使用しない（景品表示法）
- [ ] 突出した1日の利益（+87万円等）を「平均的成績」として提示しない
- [ ] 月次・年次の正直な回収率を常に添付する
- [ ] X の投稿文に「広告」「PR」等の表示（ステルスマーケティング規制対応）
- [ ] note 規約：競馬予想販売は許可されているが誇大広告は規約違反

---

## 9. 新規実装ファイル一覧（Phase 1）

| ファイル | 役割 | ベースコード |
|---|---|---|
| `scripts/weekend_batch.py` | Pre/Post 2段階オーケストレーター | 新規 |
| `src/ops/note_draft_publisher.py` | note 下書き自動保存（Playwright） | 新規 |
| `src/notification/discord_notifier.py` | エラー通知拡張（`notify_intervention_required`） | 既存に追加 |
| `src/database/init_db.py` | `batch_runs` テーブル追加 | 既存に追加 |
| `src/notification/twitter_notifier.py` | RateLimiter 追加 | 既存に追加 |

**修正対象（DOM 調整）**

| ファイル | 修正内容 |
|---|---|
| `src/ops/umanity_uploader.py` | セレクタ実動確認・修正・エラー通知統合 |

---

## 10. 次のアクション（Naofumi社長へ）

### 今週中に社長が実施すること（所要時間：合計30分）

1. **ウマニティアカウントの確認**（5分）
   - `.env` に `UMANITY_EMAIL` / `UMANITY_PASSWORD` が設定されているか確認
   - ブラウザで実際にログインできることを確認

2. **noteアカウントの確認**（5分）
   - `.env` に `NOTE_EMAIL` / `NOTE_PASSWORD` が設定されているか確認
   - メール/パスワードでログインできることを確認（Google/Twitter連携は使わない）
   - マガジン（¥1,980/月）の設定ページを確認

3. **X API 認証情報の確認**（5分）
   - `.env` の `X_API_KEY` / `X_API_SECRET` / `X_ACCESS_TOKEN` / `X_ACCESS_TOKEN_SECRET` が有効か確認
   - Free プランの月間使用量ダッシュボードで残り投稿枠を確認

4. **Discord Webhook URL の取得**（5分）
   - 通知を受け取るサーバー/チャンネルで Webhook URL を発行
   - `.env` に `DISCORD_WEBHOOK_URL` として設定

5. **Phase 1 実装の開始承認**（1分）
   - 上記確認が完了したら「Phase 1 実装開始」を指令

### Claude Code が実施すること（Phase 1 承認後）

```
Priority 1: weekend_batch.py の骨格実装（Pre/Post 分離 + エラーハンドリング）
Priority 2: umanity_uploader.py の --no-headless テスト + DOM 修正
Priority 3: note_draft_publisher.py の新規実装
Priority 4: E2E ドライラン実行
```

---

*UMALOGI Monetization Roadmap v2 — 作成: 2026-05-05*
