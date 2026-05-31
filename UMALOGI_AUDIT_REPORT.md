# UMALOGI システム全コードベース監査レポート

**監査実施日**: 2026-05-09  
**監査者**: Claude Sonnet 4.6 (UMALOGI AI Engineer)  
**対象バージョン**: UMALOGI Ver1.0 (commit: a49385d)

---

## エグゼクティブサマリー

UMALOGI の全コードベース（**61,284 行 / 187 ファイル**）を横断的に監査した結果、
以下の問題が特定されました。

| 深刻度 | 件数 | 主な内容 |
|:---:|:---:|:---|
| 🔴 **Critical** | 2 | WIN5 EV計算バグ、.env ロードパス誤り（修正済み） |
| 🟠 **High** | 6 | exception 握りつぶし多発、グローバルシングルトン順序依存、CWD 依存 load_dotenv |
| 🟡 **Medium** | 8 | マジックナンバー、N+1 クエリ、ファイルロック欠如、estimate_race_start 誤り |
| 🟢 **Low** | 5 | デッドコード、過剰なスクリプト、型ヒント欠落 |

**最重要発見**: WIN5の期待値 (EV) 計算が数学的に `_WIN5_RETURN_RATE = 0.725` 固定となっており、
組み合わせ間の優劣判定が不可能な状態。全 WIN5 推奨が実質意味なし。

---

## 1. システム構成概要

### 1.1 コードベース規模

| ディレクトリ | ファイル数 | 行数 |
|:---|:---:|:---:|
| `src/` (バックエンド) | 50 | 21,565 |
| `scripts/` (スクリプト群) | 104 | 33,253 |
| `web/src/` (フロントエンド) | 33 | 6,466 |
| **合計** | **187** | **61,284** |

### 1.2 アーキテクチャ全体図

```
外部データソース
  ├─ JRA-VAN JVLink (32bit COM) ─── src/scraper/jravan_client.py
  │     ↓ RACE/SE/WOOD/RTD dataspec
  ├─ netkeiba スクレイパー ─────── src/scraper/entry_table.py
  │     ↓ 出馬表・オッズ API
  └─ RTD リアルタイムオッズ ─── src/scraper/rtd_reader.py

データパイプライン (src/pipeline/)
  scraping.py ──── 金曜バッチ・オッズ 3段階フォールバック
  prediction.py ── 直前/暫定予想・Alpha-Payout・HitFocus
  training.py ──── モデル訓練パイプライン
  win5.py ─────── WIN5 買い目バッチ

MLコア (src/ml/)
  features.py ──── 特徴量生成 (51列)
  models.py ─────  HonmeiModel / PlaceModel / ManjiModel
                    LightGBM + Isotonic キャリブレーション
  bet_generator.py  Harville確率・Kelly Criterion
  win5.py ─────── Win5Engine (EV計算に重大バグあり)
  alpha_*.py ───── Alpha-Payout / Alpha-Place / ALPHA 単勝

データベース (SQLite: data/umalogi.db)
  init_db.py ──── 12段階マイグレーション・UPSERT ヘルパー
  schema.py ────── DDL 定義

通知 (src/notification/)
  discord_notifier.py  デュアルチャンネル (予想 / システム)
  dispatcher.py ─────  LINE / X (Twitter) 通知

運用 (src/ops/ + scripts/)
  scheduler.py ─── 週次スケジューラー (64bit 常駐)
  watchdog.py ──── 10分ループ自己修復デーモン
  today_auto_runner.py  当日直前予想ループ
  weekend_batch.py ─── Pre/Post フェーズバッチ

フロントエンド (web/ Next.js 13+)
  AppShell.tsx + 12コンポーネント + 7 API ルート
```

### 1.3 主要データフロー

```
金曜 20:00
  JVLink RACE dataspec → races / race_results / entries テーブル
  → provisional_batch() → predictions テーブル
  → Discord 暫定予想通知

土日 レース当日
  watchdog.py [10分ループ] → realtime_odds チェック → 欠損時 netkeiba backfill
  today_auto_runner.py [発走30分前] → prerace_pipeline() → predictions 更新
    → notify_prerace_result() → Discord 予想チャンネル

レース後 17:30
  JVLink RTD / netkeiba → race_payouts 更新
  hit_evaluator.py → prediction_results 更新
  retrain_trigger.py → 増分学習チェック

月曜 07:00
  retrain_win_place.py → HonmeiModel / PlaceModel / ManjiModel 再学習
  Champion/Challenger 比較 → 旧モデルを history/ に退避
```

---

## 2. 重大問題 (Critical 🔴)

### C-01: WIN5 EV計算の数学的バグ — EV が常に定数 0.725

**ファイル**: `src/ml/win5.py:271-301`  
**深刻度**: 🔴 Critical  
**影響**: WIN5 推奨が全て無意味。EV による優劣比較が不可能。

#### 問題の詳細

```python
# 現在の実装 (src/ml/win5.py:288-294)
estimated_payout = (1.0 / max(combined_prob, 1e-10)) * _WIN5_RETURN_RATE * 100
ev = combined_prob * estimated_payout / 100.0
```

数学的に展開すると:

```
ev = combined_prob × (1/combined_prob × 0.725 × 100) / 100
   = 0.725  ← 常に定数！combined_prob の値に無関係
```

**実測確認**:

| combined_prob | estimated_payout | EV |
|:---:|:---:|:---:|
| 0.001 | 72,500 | **0.725000** |
| 0.010 | 7,250 | **0.725000** |
| 0.100 | 725 | **0.725000** |
| 0.500 | 145 | **0.725000** |

バッチ実行結果でも `ev: 0.7250000000000001` が常に出力される。

#### 根本原因

払戻推定に「市場確率（オッズの逆数）」を使わず、「モデル確率（blend_prob）」を
そのまま使っているため。市場が効率的であれば `1/combined_prob × 払戻率 × combined_prob = 払戻率` が恒等式になる。

#### 正しい実装

```python
def _enumerate_combinations(self, race_picks):
    combos = []
    for picks_combo in itertools.product(*race_picks):
        # モデル確率（真の勝率推定）
        model_prob = 1.0
        for pick in picks_combo:
            model_prob *= pick.blend_prob

        # 市場確率（払戻推定用）— win_odds から算出
        market_prob = 1.0
        for pick in picks_combo:
            # オッズ逆数を正規化した市場確率
            market_odds_prob = 1.0 / max(pick.win_odds, _MIN_ODDS)
            market_prob *= market_odds_prob

        # 払戻は市場確率で推定、真の確率はモデル確率
        estimated_payout = (1.0 / max(market_prob, 1e-10)) * _WIN5_RETURN_RATE * 100
        ev = model_prob * estimated_payout / 100.0
        # ev = (model_prob / market_prob) × _WIN5_RETURN_RATE
        # model_prob > market_prob のとき EV > 0.725 → エッジあり
        ...
```

#### 副次バグ: `predict_top_n` が EV 閾値フィルターを適用しない

`predict()` は `EV >= 1.0` でフィルターするが、`predict_top_n()` はフィルターなしで
最大 20 件を返す。`win5_batch()` は `predict_top_n` を使うため、EV < 1.0 でも
常に推奨が返り、DB に保存される（現在の EV は常に 0.725 < 1.0 なので全部保存）。

---

### C-02: discord_notifier.py の .env ロードパス誤り — 本日修正済み

**ファイル**: `src/notification/discord_notifier.py:29`  
**深刻度**: 🔴 Critical（本日修正済み）  
**影響**: Discord 通知が全て無音。モデル再学習後の確認バッチでも通知なし。

```python
# 誤り (修正前)
load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=False)
# parents[3] = C:\dev\ → C:\dev\.env (存在しない)

# 正解 (修正後)
load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)
# parents[2] = C:\dev\horse-racing-ai\ → C:\dev\horse-racing-ai\.env (正しい)
```

**教訓**: モジュールレベルの `load_dotenv` と module-level singleton の組み合わせは
パス計算ミスが検知しにくい。後述の「High: グローバルシングルトン」問題と連動。

---

## 3. 高優先度問題 (High 🟠)

### H-01: 7スクリプトが引数なし `load_dotenv()` を使用 — CWD 依存

**影響ファイル**:
```
scripts/expand_trifecta_combos.py:33
scripts/generate_result_card.py:31
scripts/generate_sns_post.py:35
scripts/notify_discord.py:30
scripts/reevaluate_predictions.py:25
scripts/repair_race_data.py:41
scripts/win5_strategy.py:40
```

`load_dotenv()` は引数なしだとカレントワーキングディレクトリの `.env` を探す。
プロジェクトルートから実行しない場合（CI・cron・VSCode ターミナル等）に
環境変数が読み込まれず、サイレント失敗する。

**修正**: `load_dotenv(_ROOT / ".env")` の絶対パス形式に統一。

---

### H-02: `data_sync.py` が `override=True` で環境変数を上書き

**ファイル**: `src/ops/data_sync.py:43`

```python
_load_dotenv(_PROJECT_ROOT / ".env", override=True)  # ← 危険
```

`override=True` は既に設定されている環境変数（システム環境変数・テスト用変数）を
`.env` の値で上書きする。他スクリプトが先に設定した値が意図せず変わる可能性がある。
特に `scheduler.py` 経由で実行する場合に問題になりうる。

**修正**: `override=False` に変更。

---

### H-03: モジュールレベル `DiscordNotifier()` — import 順序依存

**影響ファイル**:
```
src/pipeline/prediction.py:31    _discord = DiscordNotifier()
src/pipeline/win5.py:21          _discord = DiscordNotifier()
```

モジュールが import された時点でインスタンスが生成される。
この時点で `load_dotenv()` が未実行だと `DISCORD_WEBHOOK_URL` が空になる。
今回の C-02 バグはこの設計と複合して検知が遅れた。

```python
# 現在 (危険: import 時に生成)
_discord = DiscordNotifier()

# 改善案 1: 関数内でオンデマンド生成 (最小変更)
def _get_discord() -> DiscordNotifier:
    return DiscordNotifier()

# 改善案 2: 依存性注入 (推奨)
def prerace_pipeline(..., notifier: DiscordNotifier | None = None) -> dict:
    discord = notifier or DiscordNotifier()
```

---

### H-04: `except Exception:` / `except Exception: pass` による例外握りつぶし

コードベース全体で **20+ 箇所** の裸の例外捕捉が見つかった。

| ファイル | 行 | 内容 | リスク |
|:---|:---:|:---|:---:|
| `src/database/init_db.py` | 655 | `except Exception:` | DBマイグレーション失敗を無視 |
| `src/evaluation/evaluator.py` | 406 | `except Exception:` | 的中評価の部分失敗 |
| `src/ml/alpha_model.py` | 505 | `except Exception:` | Alpha予測失敗の無視 |
| `src/ml/features.py` | 1018,1032 | `except Exception:` | 特徴量生成失敗 |
| `src/ml/reconcile.py` | 157 | `except Exception:` | 目的変数整合チェック |
| `src/notification/twitter_notifier.py` | 44 | `except Exception:` | X投稿失敗 |
| `src/ops/note_draft_publisher.py` | 多数 | `except Exception:` | note投稿の各ステップ |
| `src/ops/umanity_uploader.py` | 多数 | `except Exception:` | ウマニティ投稿の各ステップ |

**影響**: エラーログに `logger.warning` が出るだけで根本原因追跡が困難。
`note_draft_publisher.py` と `umanity_uploader.py` は各ステップが独立した `try-except` で
囲まれており、どのステップが失敗してもスクリプト全体は「成功」と見なす。

**修正方針**:
```python
# 現在
except Exception:
    pass

# 改善: 最低でも traceback を記録
except Exception:
    logger.exception("処理失敗 (race_id=%s)", race_id)
    # 必要なら raise または return エラー状態
```

---

### H-05: `prerace_pipeline` の prediction.py で 8 箇所の連続例外捕捉

**ファイル**: `src/pipeline/prediction.py`

```python
# 行106: 締め切りチェック失敗は Warning で続行
except Exception as exc:
    logger.warning(...)

# 行171: 予想保存の部分失敗 → UI 表示が欠ける
except Exception as exc:
    logger.error(...)

# 行235: 全馬スコア保存失敗 → 馬分析タブが空
except Exception as exc:
    logger.warning(...)

# 行415: Alpha-Payout 完全失敗 → EV買い目なし
except Exception as exc:
    logger.warning(...)

# 行509: DataFrame 再ビルド失敗 → 古いオッズで推論
except Exception as e:
    logger.warning(...)
```

ほぼ全てが「警告ログのみ → 続行」。最終出力に影響する失敗が
ユーザーには見えない形でスキップされている。

---

### H-06: watchdog.py の `_SYNC_CMD_32` が無効 + `_discord()` がシステムチャンネルを使わない

**ファイル**: `scripts/watchdog.py:53`

```python
_SYNC_CMD_32 = [sys.executable.replace("python.exe", "python.exe"), "-3-32"]
# → sys.executable が "python.exe" でない環境 (py.exe 等) では置換失敗
```

`_find_py32()` 関数が別途用意されているため `_SYNC_CMD_32` は実際には未使用だが、
混乱を生む。

また、`watchdog.py:87` の `_discord()` 関数は `DISCORD_WEBHOOK_URL` のみを参照し、
システムチャンネル (`DISCORD_SYSTEM_WEBHOOK_URL`) を無視する。
watchdog の通知はシステムチャンネルに届くべきで、現状は予想チャンネルに混在。

---

## 4. 中優先度問題 (Medium 🟡)

### M-01: マジックナンバーが 15+ 箇所に散在

| ファイル | 行 | 値 | 意味 |
|:---|:---:|:---:|:---|
| `prediction.py:58` | 58 | `0.8` | オッズ欠損率閾値 80% |
| `prediction.py:67` | 67 | `10:00` / `30分` | R1発走時刻・間隔推定 |
| `models.py:37` | 37 | `10_000` | 払戻外れ値上限 (payout_tansho > 10,000 円) |
| `models.py:504` | 504 | `0.85` | AUC Suspicious 閾値 |
| `bet_generator.py:35` | 35 | `0.25` | Kelly 上限キャップ |
| `bet_generator.py:37` | 37 | `0.25` | フラクション Kelly (1/4) |
| `bet_generator.py:39` | 39 | `1.1` | 卍EV閾値 |
| `win5.py:38` | 38 | `0.725` | WIN5 払戻率 |
| `win5.py:41-42` | 41-42 | `0.50 / 0.50` | モデル:市場 ブレンド比 |
| `watchdog.py:64-70` | 64-70 | `120,180,10,300,0.5` | 修復待機時間・閾値 |
| `watchdog.py:399` | 399 | `6` | 再予想上限レース数 |
| `features.py:39-44` | 39-44 | 距離バンド境界値 | sprint/mile/intermediate/long 区切り |

**修正案**: `src/config/thresholds.py` に一元化。

```python
# src/config/thresholds.py
from dataclasses import dataclass

@dataclass(frozen=True)
class BetThresholds:
    kelly_cap: float = 0.25
    kelly_fraction: float = 0.25
    manji_ev_threshold: float = 1.1
    odds_nan_tolerance: float = 0.80

@dataclass(frozen=True)
class Win5Config:
    return_rate: float = 0.725
    model_blend: float = 0.50
    ev_threshold: float = 1.0
    max_bets: int = 20

THRESHOLDS = BetThresholds()
WIN5 = Win5Config()
```

---

### M-02: N+1 クエリ — watchdog.py のレース別 DB アクセス

**ファイル**: `scripts/watchdog.py:447-456`

```python
# 現在: N レース × 1 クエリ = N 回の DB アクセス
conn_tmp = sqlite3.connect(str(_DB_PATH))
for rid in race_ids:              # race_ids が 36 本なら 36 回の SELECT
    rtd = conn_tmp.execute(
        "SELECT COUNT(*) FROM realtime_odds WHERE race_id=?", (rid,)
    ).fetchone()[0]
    if rtd == 0:
        races_nan.append(rid)
```

```python
# 改善: 1クエリで全件取得
placeholders = ",".join("?" * len(race_ids))
covered = {
    r[0] for r in conn_tmp.execute(
        f"SELECT DISTINCT race_id FROM realtime_odds WHERE race_id IN ({placeholders})",
        race_ids,
    ).fetchall()
}
races_nan = [rid for rid in race_ids if rid not in covered]
```

---

### M-03: `_estimate_race_start_jst` — R1=10:00・30分固定が実態と乖離

**ファイル**: `src/pipeline/prediction.py:66-69`

```python
def _estimate_race_start_jst(race_number: int, race_date: str) -> datetime:
    """R1=10:00 JST、以降 30 分間隔で発走時刻を推定する。"""
    base = datetime.strptime(race_date, "%Y%m%d").replace(hour=10, minute=0)
    return base + timedelta(minutes=(race_number - 1) * 30)
```

実際のレース発走時刻は `races.post_time` に格納されており、
10:00 起点・30 分間隔は一部のレース（特に最終レース・障害）で大きく外れる。

この関数は締め切りチェック（15分前警告）に使われており、
実際の発走前なのに「過ぎた」と誤判定するリスクがある。

```python
# 改善: DB の post_time を優先利用
def _estimate_race_start_jst(race_number: int, race_date: str, 
                               post_time: str | None = None) -> datetime:
    if post_time:
        h, m = map(int, post_time.split(":"))
        return datetime.strptime(race_date, "%Y%m%d").replace(hour=h, minute=m)
    # フォールバックのみ推定
    base = datetime.strptime(race_date, "%Y%m%d").replace(hour=10, minute=0)
    return base + timedelta(minutes=(race_number - 1) * 30)
```

---

### M-04: `_log_elite_bet` — 並列書き込みリスク（ファイルロック未実装）

**ファイル**: `src/ml/bet_generator.py:56-80`

```python
with _ELITE_CSV.open("a", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=_ELITE_CSV_COLS)
    if write_header:
        writer.writeheader()
    writer.writerow(row)
```

`today_auto_runner.py` が複数レースを並列処理する場合、
複数プロセスが同時に `fukusho_elite_monitor.csv` を open("a") するとデータ破壊の可能性。

```python
# 改善: fcntl (Linux) / msvcrt.locking (Windows) を使うか、
# 単純にスレッドロックで保護
import threading
_CSV_LOCK = threading.Lock()

def _log_elite_bet(...):
    with _CSV_LOCK:
        with _ELITE_CSV.open("a", ...) as f:
            ...
```

---

### M-05: Harville 確率計算の浮動小数点不安定性

**ファイル**: `src/ml/bet_generator.py` (Harville 確率計算部分)

```
P(A→B 馬単) = p_A × p_B / (1 - p_A)
P(A→B→C 三連単) = p_A × p_B/(1-p_A) × p_C/(1-p_A-p_B)
```

`1 - p_A - p_B` が浮動小数点誤差で負になる場合がある（出走馬が多くスコアが均等な場合）。
`if denom <= 0` の保護はあるが、`1e-8` 以下の正値を 0 扱いしないと
除算結果が爆発する。

```python
# 改善: ガード強化
denom = max(1.0 - sum(probs_accumulated), 1e-8)
```

---

### M-06: `save_entries_to_db` で 1件エラー = 1トランザクション

**ファイル**: `src/pipeline/scraping.py:99-138`

```python
for entry in tbl.entries:
    try:
        conn.execute("INSERT OR REPLACE INTO entries ...", ...)
        conn.commit()  # ← 1頭ずつコミット（低速）
    except Exception as exc:
        logger.warning(...)
```

出馬頭数分（最大 18 頭）の個別コミットが発生。バルクインサートを使えば
10倍以上高速化できる。

---

### M-07: WIN5 全頭列挙モード（`predict()`）のメモリ爆発リスク

**ファイル**: `src/ml/win5.py:110-146`

`predict()` メソッドは各レースの全頭を `itertools.product` で列挙する:

```python
# 最大: 18頭 × 5レース = 18^5 = 1,889,568 組み合わせ
for picks_combo in itertools.product(*race_picks):
```

`predict_top_n()` (3頭×5レース=243通り) を基本として使っているため
現状は問題ないが、`predict()` を直接呼ぶと OOM の危険がある。
`predict()` はドキュメントで非推奨としておくべき。

---

### M-08: `_rerun_prerace_for_races` — 発走判定が post_time 文字列比較

**ファイル**: `scripts/watchdog.py:376-390`

```python
now_str = datetime.now().strftime("%H:%M")  # 例: "15:30"
if row[0] > now_str:  # 文字列比較 (!) — "09:30" > "08:00" は正しいが
    pending.append(rid) # "10:00" > "09:30" → "1" > "0" で偶然正しいが脆弱
```

文字列比較は HH:MM 形式なら偶然動くが、DB に NULL や不正値が混じると例外。
`datetime` オブジェクトでの比較を推奨。

---

## 5. 低優先度問題 / 技術的負債 (Low 🟢)

### L-01: デッドコード — `_PlattModel` クラス (models.py)

`src/ml/models.py` に `_PlattModel` (ロジスティック回帰キャリブレーション) が残存。
`IsotonicRegression` に置き換え済みで未使用。後方互換のためと推測されるが
pkl ファイルに混入するリスクがある。

### L-02: debug スクリプト 10+ 本が scripts/ に残存

```
scripts/hexdump_jvlink_raw.py
scripts/hexdump_se_bytes.py
scripts/debug_feature_gaps.py
scripts/investigate_data.py
scripts/inspect_raw_records.py
scripts/probe_se_full.py
scripts/probe_se_rank.py
...
```

本番環境の `scripts/` に開発・デバッグ用スクリプトが多数残存。
`scripts/debug/` または `tools/` に移動推奨。

### L-03: 型ヒントの不完全性

`src/pipeline/prediction.py` の内部関数群で `-> dict` のような曖昧な型が残っている。
CLAUDE.md では `mypy strict` を目標としているが未実現。

### L-04: `scheduler.py` のログハンドラ設定が煩雑

```python
logging.StreamHandler(
    open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", closefd=False)
)
```

`sys.stdout.fileno()` は一部の環境（IDEの仮想ターミナル等）で例外。
`sys.stdout` を直接渡す方がシンプルで安全。

### L-05: `today_auto_runner.py` の `_send_discord()` がシステムチャンネルを未使用

`watchdog.py` と同じ問題。実行ログが予想チャンネルに流れる。
`DISCORD_SYSTEM_WEBHOOK_URL` への振り分けが必要。

---

## 6. アーキテクチャ評価

### 良い設計（維持すべき点）

| 項目 | 評価 |
|:---|:---|
| Champion/Challenger モデル管理 | ✅ AUC比較による自動世代交代は優秀 |
| 3段階フォールバック (JVLink→netkeiba→DB既存) | ✅ `scraping.py` の `fetch_and_save_odds` は堅牢 |
| 時系列CV分割 | ✅ 未来情報リーク防止が正しく実装済み |
| Isotonic Regression キャリブレーション | ✅ 確率の単調性保証 |
| parameterized SQL クエリ | ✅ SQLインジェクション対策済み |
| Unicode/CP932 ハイブリッド処理 | ✅ JVLink 文字化け対策が適切 |
| Kelly Criterion (1/4 Kelly) | ✅ 破産確率の数学的最小化 |
| Dual Discord channel routing | ✅ 予想 vs システム分離で視認性向上 |

### 構造的問題

| 問題 | 詳細 |
|:---|:---|
| **密結合すぎる prediction.py** | 671行に全ロジックが集中。スクレイピング・推論・保存・通知を1関数で処理 |
| **subprocess 32bit/64bit 分離** | scheduler→subprocess→py32 の多段呼び出しでデバッグが困難 |
| **グローバル状態の多用** | `_MODEL_CACHE`, `_discord`, `_sire_map` がプロセス全体で共有。並列テスト不可 |
| **scripts/ の肥大化** | 104ファイル/33,253行。本番・デバッグ・旧バージョンが混在 |

---

## 7. 改善ロードマップ

### Phase 0 — 即座対応（今週中）

| # | タスク | 工数 | 効果 |
|:---:|:---|:---:|:---|
| 0-1 | WIN5 EV計算修正（market_prob 使用） | 2h | WIN5 推奨の精度回復 |
| 0-2 | `predict_top_n` に EV 閾値フィルター追加 | 30m | 無意味な推奨排除 |
| 0-3 | 7スクリプトの `load_dotenv()` を絶対パス化 | 30m | CWD 依存バグ防止 |
| 0-4 | `data_sync.py:override=True` → `False` | 5m | 環境変数上書き防止 |
| 0-5 | `watchdog.py` N+1 クエリ修正 | 30m | DB負荷削減 |

### Phase 1 — 品質改善（2週間以内）

| # | タスク | 工数 | 効果 |
|:---:|:---|:---:|:---|
| 1-1 | `src/config/thresholds.py` 作成・全マジックナンバー移行 | 4h | 設定管理の一元化 |
| 1-2 | `except Exception:` を `logger.exception()` に置換 | 3h | デバッグ効率大幅改善 |
| 1-3 | `watchdog.py` の `_discord()` をシステムチャンネルに変更 | 30m | 通知混在解消 |
| 1-4 | `_estimate_race_start_jst` → DB の `post_time` 優先利用 | 2h | 締め切りチェック精度向上 |
| 1-5 | `_log_elite_bet` にスレッドロック追加 | 30m | ファイル破壊防止 |
| 1-6 | `_PlattModel` 削除 | 30m | デッドコード除去 |

### Phase 2 — アーキテクチャ改善（1ヶ月以内）

| # | タスク | 工数 | 効果 |
|:---:|:---|:---:|:---|
| 2-1 | `prediction.py` を責務別に分割 | 8h | 保守性向上・テスト容易化 |
| 2-2 | `DiscordNotifier` の DI 化（モジュールレベルシングルトン廃止） | 4h | テスト容易化・順序依存解消 |
| 2-3 | `scripts/debug/` ディレクトリ作成・デバッグスクリプト移動 | 1h | scripts/ の整理 |
| 2-4 | `load_dotenv` を `src/config/env_loader.py` に一元化 | 2h | 全スクリプトから `.env` 管理を分離 |
| 2-5 | `Harville` 確率計算の数値安定性強化 | 2h | エッジケース保護 |
| 2-6 | `save_entries_to_db` バルクインサート化 | 2h | 金曜バッチ高速化 |

### Phase 3 — 品質保証（継続）

| # | タスク | 効果 |
|:---:|:---|:---|
| 3-1 | `mypy strict` 導入・型ヒント完全化 | 型安全性保証 |
| 3-2 | pytest カバレッジ 60%+ 目標 | リグレッション防止 |
| 3-3 | `ruff` + `black` CI 統合 | コードスタイル自動化 |
| 3-4 | `scheduler.py` ログハンドラ整理 | IDEターミナル互換性向上 |

---

## 8. 修正優先度マトリクス

```
影響度
  高 │ [C-01 WIN5 EV]  [H-03 シングルトン]
     │ [C-02 .env ✅]  [H-01 load_dotenv]
     │                  [H-04 例外握りつぶし]
     ├───────────────────────────────────────
  中 │ [M-01 マジック数]  [H-02 override=True]
     │ [M-03 race_start] [M-02 N+1クエリ]
     │ [M-04 ファイルロック]
     ├───────────────────────────────────────
  低 │ [L-01 PlattModel] [M-05 Harville]
     │ [L-02 debug scripts]
     └─────────────────────────────────────
        緊急      通常      計画的
         ↑対応速度
```

---

## 9. 付録: ファイル別問題点一覧

| ファイル | 問題 | 深刻度 |
|:---|:---|:---:|
| `src/ml/win5.py:271-301` | EV計算が常に定数0.725 | 🔴 |
| `src/notification/discord_notifier.py:29` | parents[3]→parents[2] (修正済み) | 🔴 |
| `src/pipeline/prediction.py:31` | モジュールレベルシングルトン | 🟠 |
| `src/pipeline/win5.py:21` | モジュールレベルシングルトン | 🟠 |
| `src/ops/data_sync.py:43` | override=True | 🟠 |
| `src/pipeline/prediction.py` (8箇所) | 例外握りつぶし | 🟠 |
| `src/ops/note_draft_publisher.py` (5箇所) | 例外握りつぶし | 🟠 |
| `src/ops/umanity_uploader.py` (7箇所) | 例外握りつぶし | 🟠 |
| `scripts/watchdog.py:53` | `_SYNC_CMD_32` 未使用・無効 | 🟠 |
| `scripts/watchdog.py:87` | システムチャンネル未使用 | 🟠 |
| `scripts/7ファイル` | `load_dotenv()` 引数なし | 🟠 |
| `src/pipeline/prediction.py:58,67` | マジックナンバー | 🟡 |
| `src/ml/bet_generator.py:35-39` | マジックナンバー | 🟡 |
| `src/ml/win5.py:38-42` | マジックナンバー | 🟡 |
| `scripts/watchdog.py:447-456` | N+1クエリ | 🟡 |
| `src/pipeline/prediction.py:66-69` | 発走時刻推定の不精度 | 🟡 |
| `src/ml/bet_generator.py:56-80` | ファイルロック欠如 | 🟡 |
| `src/ml/win5.py:110-146` | 全列挙OOMリスク | 🟡 |
| `scripts/watchdog.py:376-390` | 文字列比較による発走判定 | 🟡 |
| `src/ml/models.py:_PlattModel` | デッドコード | 🟢 |
| `scripts/hexdump_*.py` 等 | デバッグスクリプト残存 | 🟢 |
| `src/pipeline/prediction.py` 全体 | 型ヒント不完全 | 🟢 |
| `scripts/scheduler.py:71-75` | ログハンドラ設定 | 🟢 |
| `scripts/today_auto_runner.py` | システムチャンネル未使用 | 🟢 |

---

*本レポートは UMALOGI Ver1.0 (2026-05-09) の状態を基に作成されました。*  
*Phase 0 の修正後に再監査を推奨します。*
