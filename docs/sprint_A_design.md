# UMALOGI Sprint A 詳細設計書

## メタデータ

| 項目 | 内容 |
|------|------|
| 作成日 | 2026-05-15 |
| 担当スプリント | Sprint A（2〜3週間） |
| 対象機能 | A1: X シグナル統合 / A2: FukushoElite 本番統合 |
| ステータス | **設計完了・実装待ち** |

---

## A1: X 凄腕予想家シグナル統合

### 1-1. 全体アーキテクチャ

```
【毎週金曜 19:30〜土日 08:00】

X Timeline / Search
    │  Playwright + stealth-mode
    ▼
src/scraper/x_scraper.py
    │  raw_text / tweet_id / posted_at
    ▼
x_signals テーブル (raw)
    │  未構造化テキストを保存
    ▼
src/ml/x_signal_parser.py
    │  Claude Haiku API (structured_outputs)
    ▼
x_signals テーブル (構造化フィールド更新)
    │  horse_number / signal_type / confidence
    ▼
src/ml/alpha_model.py
    │  FEATURE_COLS に x_consensus_score 追加
    ▼
EV 計算 → 予想強化
```

---

### 1-2. スクレイピング戦略（X の Bot 対策突破）

#### 課題の整理

X は 2023年以降、非ログインユーザーへの API・HTML アクセスを大幅に制限している:
- 非ログイン: タイムライン不可、検索は数件のみ
- ログイン済み: 速度制限あり（15分ごと）、JS チャレンジあり
- Twitter API v2: 無料枠では月500ツイート読み取りのみ（実用不可）

#### 採用アプローチ: Playwright + Stealth + Cookie 永続化

```python
# src/scraper/x_scraper.py の設計方針

# ① playwright-stealth で Bot 検知を回避
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async

# ② Cookie を永続化（毎回ログイン不要）
# data/x_cookies.json に保存・読み込み
# ブラウザを "persistent context" で起動 (ユーザープロファイル維持)

# ③ レート制限対策
# - 1アカウントあたり 15分で最大 15リクエスト厳守
# - リクエスト間: random.uniform(3.0, 8.0) 秒スリープ
# - 1セッション終了後: 20分以上のクールダウン

# ④ 対象: 検索 + タイムライン
# - 検索クエリ: f"from:{account} (◎ OR 本命 OR ○ OR ▲) filter:links -filter:replies"
# - 対象時間帯: レース開催日の 前日 18:00 〜 当日 09:00
```

#### ブラウザ起動設計

```python
async def _launch_browser(playwright, cookie_path: Path):
    """
    永続コンテキストで Chromium を起動し、Cookie を復元する。
    headless=True だが viewport / user-agent は実ブラウザに偽装。
    """
    context = await playwright.chromium.launch_persistent_context(
        user_data_dir=str(cookie_path.parent / "chrome_profile"),
        headless=True,
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="ja-JP",
        timezone_id="Asia/Tokyo",
        args=["--disable-blink-features=AutomationControlled"],
    )
    page = await context.new_page()
    await stealth_async(page)        # playwright-stealth で自動化フラグを消す
    return context, page
```

#### 初回ログインフロー（手動 1 回のみ）

```
1. headless=False で起動
2. x.com にアクセス → ログインフォームへ
3. 人間が手動でログイン（2FA 含む）
4. ログイン成功後に Cookie を data/x_cookies.json に保存
5. 以降は headless=True + Cookie 復元で自動実行
```

#### ターゲットアカウント管理

```json
// scripts/x_targets.json
[
  {
    "screen_name": "target_account_1",
    "display_name": "◯◯競馬予想家",
    "weight": 1.0,
    "past_hit_rate": null,
    "note": "重賞専門・的中率公開"
  }
]
```

`weight` は「過去的中率」から動的更新。初期値 1.0、的中で +0.1、外れで -0.05。

---

### 1-3. Claude Haiku 構造化プロンプト設計

#### 設計方針

- モデル: `claude-haiku-4-5-20251001`（低コスト・高速）
- API: Anthropic Messages API with `tool_use` で JSON を強制
- 1ツイートあたりのコスト: 約 ¥0.02（Haiku は入出力合わせて非常に安価）

#### システムプロンプト

```
あなたは競馬予想ポストを解析する専門家です。
与えられたツイートテキストから、以下のルールで情報を抽出してください。

【抽出ルール】
- 「レース名」: 「◯◯R」「◯◯競馬」「◯◯特別」などの表現。不明なら null。
- 「馬番/馬名」: 数字（「5番」「⑤」）か馬名（カタカナ）で表現される。
- 「印の種類」:
    ◎ = "honmei"（本命）
    ○ = "taikou"（対抗）
    ▲ = "tanana"（単穴）
    △ = "renpuku"（連複）
    × = "keshi"（消し・消去推奨）
    穴 = "ana"（穴馬推奨）
- 「確信度」: 言語的確信度を 0.0〜1.0 で推定。「絶対」→0.95、「かも」→0.40。
- 不明・抽出不可な場合は空リストを返す。複数の馬が言及された場合はすべて抽出。
```

#### ユーザープロンプトテンプレート

```python
user_prompt = f"""
以下のツイートを解析してください:

投稿者: @{screen_name}
投稿日時: {posted_at}
本文:
{raw_text}

JSON形式で出力してください。
"""
```

#### Tool 定義（強制 JSON スキーマ）

```python
tools = [{
    "name": "extract_prediction",
    "description": "競馬予想ポストから予想情報を抽出する",
    "input_schema": {
        "type": "object",
        "properties": {
            "race_name": {
                "type": ["string", "null"],
                "description": "レース名（例: '東京11R', 'NHKマイルC'）"
            },
            "predictions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "horse_name":   {"type": ["string", "null"]},
                        "horse_number": {"type": ["integer", "null"]},
                        "signal_type":  {
                            "type": "string",
                            "enum": ["honmei", "taikou", "tanana", "renpuku", "ana", "keshi"]
                        },
                        "confidence": {
                            "type": "number",
                            "minimum": 0.0,
                            "maximum": 1.0
                        }
                    },
                    "required": ["signal_type", "confidence"]
                }
            },
            "is_race_day_post": {
                "type": "boolean",
                "description": "当日の予想ポストか、前日/回顧か"
            }
        },
        "required": ["predictions"]
    }
}]
```

#### race_id 突合ロジック

```python
def _resolve_race_id(conn, race_name: str | None, posted_at: datetime) -> str | None:
    """
    抽出した race_name から races テーブルの race_id を逆引きする。

    1. 投稿日 or 翌日の races.race_name に対して fuzzy match
    2. 会場名（例: "東京"）+ レース番号（"11R"）の組み合わせで絞り込み
    3. 唯一確定できない場合は None（保存はするが race_id = NULL）
    """
    ...
```

---

### 1-4. x_signals テーブル スキーマ

```sql
CREATE TABLE IF NOT EXISTS x_signals (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,

    -- 投稿メタデータ
    tweet_id      TEXT    NOT NULL UNIQUE,       -- X の tweet ID（重複防止）
    screen_name   TEXT    NOT NULL,              -- @screen_name
    posted_at     TEXT    NOT NULL,              -- ISO 8601 UTC

    -- レース突合
    race_id       TEXT    REFERENCES races(race_id),  -- NULL = 突合失敗
    race_name_raw TEXT,                          -- 抽出した生レース名文字列

    -- 予想内容（1ツイート1馬1レコード）
    horse_name    TEXT,                          -- 馬名（抽出成功時）
    horse_number  INTEGER,                       -- 馬番（抽出成功時）
    signal_type   TEXT    NOT NULL,              -- honmei/taikou/tanana/renpuku/ana/keshi
    confidence    REAL    NOT NULL DEFAULT 0.5,  -- 0.0〜1.0

    -- アカウント重み（過去的中率由来）
    account_weight REAL   NOT NULL DEFAULT 1.0,

    -- 生データ保存（再解析用）
    raw_text      TEXT    NOT NULL,
    fetched_at    TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),

    -- 指標: 総合シグナルスコア = confidence × account_weight × signal_strength
    -- signal_strength: honmei=1.0, taikou=0.8, tanana=0.6, renpuku=0.4, ana=0.7, keshi=-0.5
    signal_score  REAL    GENERATED ALWAYS AS (
        confidence * account_weight *
        CASE signal_type
            WHEN 'honmei'  THEN 1.0
            WHEN 'taikou'  THEN 0.8
            WHEN 'tanana'  THEN 0.6
            WHEN 'renpuku' THEN 0.4
            WHEN 'ana'     THEN 0.7
            WHEN 'keshi'   THEN -0.5
            ELSE 0.0
        END
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_xsig_race_id    ON x_signals(race_id);
CREATE INDEX IF NOT EXISTS idx_xsig_posted_at  ON x_signals(posted_at);
CREATE INDEX IF NOT EXISTS idx_xsig_horse_num  ON x_signals(race_id, horse_number);
```

---

### 1-5. FEATURE_COLS への統合

```python
# src/ml/alpha_model.py への追加特徴量

def _build_x_consensus_features(conn, race_id: str, horse_numbers: list[int]) -> pd.DataFrame:
    """
    x_signals テーブルから各馬の X コンセンサス特徴量を生成する。

    Returns:
        horse_number をインデックスとする DataFrame:
          x_consensus_score  : 全シグナルの加重平均 (signal_score)
          x_honmei_count     : ◎ シグナルの件数
          x_keshi_flag       : × シグナルが1件以上あれば 1
          x_account_count    : シグナルを出したアカウント数
    """
    rows = conn.execute("""
        SELECT horse_number,
               AVG(signal_score)                      AS x_consensus_score,
               SUM(CASE WHEN signal_type='honmei' THEN 1 ELSE 0 END) AS x_honmei_count,
               MAX(CASE WHEN signal_type='keshi'  THEN 1 ELSE 0 END) AS x_keshi_flag,
               COUNT(DISTINCT screen_name)            AS x_account_count
        FROM x_signals
        WHERE race_id = ?
          AND posted_at >= datetime('now', '-48 hours')
        GROUP BY horse_number
    """, (race_id,)).fetchall()
    ...
```

追加する `FEATURE_COLS`（既存 39 列に 4 列追加 → 43 列）:
```
"x_consensus_score", "x_honmei_count", "x_keshi_flag", "x_account_count"
```

---

### 1-6. スケジューラー統合

```python
# scripts/scheduler.py に追加するジョブ

def job_x_signal_fetch() -> None:
    """
    金曜 19:30〜土曜 08:30: X 凄腕予想家のポストを収集・構造化する。

    開催前日夜〜当日朝の投稿を対象。
    FridaySync 完了後・today_auto_runner 起動前に実行。
    """
    ...

schedule.every().friday.at("19:30").do(job_x_signal_fetch)
schedule.every().saturday.at("06:00").do(job_x_signal_fetch)  # 追加収集
schedule.every().sunday.at("06:00").do(job_x_signal_fetch)
```

---

### 1-7. 実装優先順位

```
Phase A（最小実装・1週間）:
  [x] x_signals テーブル DDL 追加
  [x] x_targets.json 作成（初期3〜5アカウント）
  [ ] x_scraper.py: ログイン + Cookie 保存 + ツイート取得
  [ ] x_signal_parser.py: Haiku API 呼び出し + DB 保存
  [ ] scheduler.py: job_x_signal_fetch() 登録

Phase B（統合・1週間）:
  [ ] alpha_model.py: x_consensus_features 追加
  [ ] モデル再訓練 + バックテスト（ROI 改善幅の測定）
  [ ] account_weight の動的更新ロジック

Phase C（改善・継続）:
  [ ] 複数アカウント対応・weight チューニング
  [ ] keshi シグナルのネガティブフィルター効果検証
```

---

---

## A2: FukushoElite 本番パイプライン統合

### 2-1. 現状の実装状況

```
【実装済み ✅】
  src/ml/bet_generator.py:
    - FukushoEliteFilter.evaluate()     完全実装済み
    - generate_elite_fukusho_bets()     完全実装済み
    - FukushoEliteResult dataclass      定義済み
    - _log_elite_bet() → CSV ログ       実装済み

【未接続 ❌】
  src/pipeline/prediction.py:
    - prerace_pipeline() 内に呼び出しなし
    - notify_prerace_result() に FukushoElite セクション未追加
    - predictions テーブルへの保存ロジックなし
```

**ギャップ: 関数は完成しているが、パイプラインへの配線が 2 箇所だけ未完了。**

---

### 2-2. フィルター条件（再確認）

```python
# bet_generator.py より（変更不要）

_FUKUSHO_ELITE_VENUES:     frozenset = {"新潟", "東京", "福島", "京都"}
_FUKUSHO_ELITE_MIN_HORSES: int   = 13         # 13頭以上の多頭数レース
_FUKUSHO_ELITE_EDGE:       float = 1.1        # edge = model_prob / market_implied_prob

# 通過条件: ① venue 一致 ② 頭数 >= 13 ③ 上位3頭中2頭以上で edge >= 1.1
```

---

### 2-3. prediction.py への統合シーケンス

```
prerace_pipeline(race_id, provisional=False)
│
├── Step 1: エントリー取得 (_fetch_entries)
├── Step 2: 特徴量生成 (FeatureBuilder)
├── Step 3: モデル予測 (honmei / manji)
├── Step 4a: 買い目生成 (BetGenerator)
├── Step 4b: Alpha-Payout 複勝シグナル [既存]
│
├── Step 4c: FukushoElite フィルター ← 【追加箇所①】
│     │
│     │  generate_elite_fukusho_bets(
│     │      race_id  = race_id,
│     │      venue    = df["venue"].iloc[0],
│     │      n_horses = len(df),
│     │      horse_numbers = df["horse_number"].tolist(),
│     │      horse_names   = df["horse_name"].tolist(),
│     │      ev_scores     = ev_scores.tolist(),       # 卍モデルのEVスコア流用
│     │      implied_probs = df["implied_prob"].tolist()
│     │  )
│     │  → RaceBets | None
│     │
│     └── (passed=False) → logger.debug でスキップ、elite_bets = None
│
├── Step 5: DB 保存
│     └── elite_bets が None でなければ _save_predictions に追加
│
├── Step 6: JSON 出力 (変更不要)
│
└── Step 7: Discord 通知
      └── notify_prerace_result(..., elite_bets=elite_bets) ← 【追加箇所②】
```

---

### 2-4. 複勝 EV 計算ロジック（詳細）

```python
# alpha_place_model.py の式を FukushoElite に適用

# 市場 implied probability（複勝オッズから計算）
# 複勝オッズ min/max の中央値を使用
implied_prob_place = 1.0 / (place_odds_mid / FUKUSHO_PAYOUT_RATE)

# モデル予測確率（卍モデルの ev_score を正規化して使用）
model_prob_place = ev_score / sum(ev_scores)  # per-race 正規化

# Edge = モデル優位性
edge = model_prob_place / max(implied_prob_place, 1e-6)

# EV (期待値)
ev = edge * FUKUSHO_PAYOUT_RATE  # = model_prob / market_prob * 0.775

# Kelly 推奨額
# f* = (EV - 1) / (払戻倍率 - 1) × Kelly分率 × バンクロール
kelly_f = (ev - 1.0) / (place_odds_mid - 1.0) * KELLY_FRACTION
bet = max(MIN_BET, min(MAX_BET, int(kelly_f * bankroll / 100) * 100))
```

---

### 2-5. Discord Embed 通知デザイン

#### 追加セクション（既存の ALPHA/卍/本命 セクションの後）

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 🏇 東京11R  NHKマイルカップ
  最大EV: 2.34  推奨投資合計: ¥4,500
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🟦 __ALPHA 予想__  ... (既存)
🟩 __卍 予想__     ... (既存)
🟥 __本命 予想__   ... (既存)

🟣 __FukushoElite__ (複勝エリート)   ← 【新規追加】
   ​
📌 複勝  EV=1.43  ¥600
  ⬛ 5番 ダイヤモンドノット  edge=1.31
  ⬛ 9番 カヴァレリッツォ   edge=1.18
```

**表示条件:**
- `elite_bets is not None`（フィルター通過時のみ）
- EV が 1.0 未満なら Embed 全体をスキップ（既存ルールと同じ）

**Embed カラー:**
- `0xA855F7`（パープル）— FukushoElite 専用

**discord_notifier.py への変更:**
```python
# notify_prerace_result() のシグネチャに elite_bets 追加
def notify_prerace_result(
    self,
    race_id: str,
    honmei_bets: RaceBets | None,
    manji_bets: RaceBets | None,
    *,
    oracle_bets: RaceBets | None = None,
    hit_focus_bets: RaceBets | None = None,
    alpha_bets: RaceBets | None = None,
    elite_bets: RaceBets | None = None,   # ← 追加
) -> None:
    ...
    if elite_bets and elite_bets.bets:
        sections.append(self._format_elite_section(elite_bets))
```

---

### 2-6. predictions テーブルへの保存

```python
# prediction.py: _save_predictions() への追加

if elite_bets and elite_bets.bets:
    for bet in elite_bets.bets:
        insert_prediction(
            conn,
            race_id      = race_id,
            model_type   = "FukushoElite(直前)",
            bet_type     = "複勝",
            horses       = [{"horse_number": h.horse_number, "horse_name": h.horse_name,
                             "ev_score": h.ev_score, "predicted_rank": i+1}
                            for i, h in enumerate(bet.horses)],
            confidence   = bet.confidence,
            expected_value   = bet.expected_value,
            recommended_bet  = bet.recommended_bet,
            notes        = f"FukushoElite: edge={[round(e,2) for e in elite_result.edges]}",
        )
```

---

### 2-7. 実装手順（コード変更 2 ファイルのみ）

```
① src/pipeline/prediction.py
   - prerace_pipeline() の Step 4c として generate_elite_fukusho_bets() 呼び出し追加
   - _save_predictions() に elite_bets の保存ロジック追加
   - notify_prerace_result() 呼び出しに elite_bets=elite_bets を渡す

② src/notification/discord_notifier.py
   - notify_prerace_result() のシグネチャに elite_bets 追加
   - _format_elite_section() メソッド追加（パープルセクション）

変更規模: ~60行（2ファイル）
既存テスト影響: なし（elite_bets はデフォルト None で後方互換）
本番影響: フィルター通過時のみ追加出力（既存予想は無変更）
```

---

### 2-8. バックテスト・検証計画

**実装前に確認すべき指標:**

| 指標 | 現状（バックテスト） | 目標 |
|------|---------------------|------|
| 複勝 ROI (in-sample 2025) | 97.6% → フィルター後 119.9% | — |
| 複勝 ROI (OOS 2024, F3モデル) | 65.6% | — |
| 複勝 ROI (ライブ 2026) | **未計測** | **≥ 100%** |

**実装後 4 週間の計測方法:**
```python
# scripts/evaluate_elite_results.py（既存スクリプト）を活用
# 毎週月曜 job_monday_masters で自動評価
python scripts/evaluate_elite_results.py --weeks 4
```

---

## 実装優先度マトリクス

| 機能 | 工数 | ROI 改善期待 | 実装難易度 | 推奨順 |
|------|------|------------|----------|--------|
| A2: FukushoElite | **小（2日）** | +中（複勝ライン安定）| **低** | **1番目** |
| A1-α: Xスクレイパー | 大（1週間） | +大（穴馬発見）| 高（Bot対策） | 2番目 |
| A1-β: Haiku 構造化 | 中（3日） | +大 | 中 | 3番目 |
| A1-γ: FEATURE統合 | 中（3日） | +大 | 中 | 4番目 |

**推奨: A2（FukushoElite）を今すぐ実装 → A1-α（Xスクレイパー）で初回手動ログインを行うことで週末までに動作させる。**

---

## 次回セッションへの引き継ぎチェックリスト

```
A2（FukushoElite）実装時:
  □ src/pipeline/prediction.py: Step 4c 追加（~20行）
  □ src/notification/discord_notifier.py: elite_bets 対応（~30行）
  □ 当日ライブテスト: 東京/新潟/福島/京都 の 13頭以上レースで動作確認
  □ evaluate_elite_results.py で 4週後に ROI を計測

A1（X シグナル）実装前に社長が準備すること:
  □ 対象 X アカウント URL を 3〜5 件ピックアップ（以下の基準で選定）
       - フォロワー 1万人以上
       - 印（◎〇▲）を数字または馬名で明示している
       - 過去的中実績を公開している（スクリーンショット等）
  □ X アカウントにログインできる状態のブラウザを用意
  □ Anthropic API キーを .env に ANTHROPIC_API_KEY として追加
```
