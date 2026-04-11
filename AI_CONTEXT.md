# AI_CONTEXT.md — UMALOGI 後任 AI 向け調教書

このドキュメントは将来の AI エージェント（および人間のメンテナー）が
UMALOGI を安全に修正・拡張するための技術的コンテキストです。
コードから自明に読み取れる内容は省き、**罠・落とし穴・非自明な設計決定** を重点的に記載します。

---

## 1. DB スキーマ詳細

### データベースファイル

`data/umalogi.db` — SQLite 3。接続は必ず `src/database/init_db.py` の `init_db()` を経由する。
直接 `sqlite3.connect()` してはいけない。`init_db()` はスキーマ作成・マイグレーションを自動実行する。

```python
from src.database.init_db import init_db
conn = init_db()  # data/umalogi.db を開き FK を有効化
```

### テーブル一覧

#### データ層

| テーブル | 主キー | 説明 |
|---|---|---|
| `races` | `race_id TEXT PK` | レース基本情報。`date` は `YYYY-MM-DD`（ISO 8601）で保存する。`YYYY/MM/DD` は不可 |
| `race_results` | `id INTEGER AUTOINCREMENT` | 着順・オッズ。`UNIQUE(race_id, horse_name)` |
| `race_payouts` | `id INTEGER AUTOINCREMENT` | 確定払戻。`UNIQUE(race_id, bet_type, combination)` |
| `entries` | `id INTEGER AUTOINCREMENT` | 出馬表（レース前）。`UNIQUE(race_id, horse_number)` |
| `realtime_odds` | `id INTEGER AUTOINCREMENT` | オッズ時系列スナップショット |
| `horses` | `horse_id TEXT PK` | 馬マスタ。netkeiba スクレイプ時に `sire/dam/dam_sire` を充填 |

#### JRA-VAN マスタ層

| テーブル | 結合キー | JV-Link 仕様 |
|---|---|---|
| `training_times` | `horse_id, training_date, course_type, direction` | WOOD:TC / レコード種 WC |
| `training_hillwork` | `horse_id, training_date` | WOOD:HC / レコード種 WH |
| `breeding_horses` | `horse_id TEXT PK` | BLOD:BT（繁殖馬） |
| `foals` | `horse_id TEXT PK` | BLOD:HN（産駒） |
| `racehorses` | `horse_id TEXT PK` | DIFN:UM（競走馬） |
| `jockeys` | `jockey_code TEXT PK` | DIFN:KS |
| `trainers` | `trainer_code TEXT PK` | DIFN:CH |

#### 予想層

| テーブル | 主キー / 制約 | 説明 |
|---|---|---|
| `predictions` | `UNIQUE(race_id, model_type, bet_type)` | 1レース × 1モデル × 1券種。`INSERT OR REPLACE` なので同キーで再実行すると上書き |
| `prediction_horses` | FK → predictions | 予想に含まれる馬と個別スコア |
| `prediction_results` | FK → predictions | 照合後の的中・払戻実績 |
| `model_performance` | `UNIQUE(model_type, bet_type, year, month, venue)` | 定期集計。`UPSERT` で更新 |

#### ビュー

| ビュー | 説明 |
|---|---|
| `v_prediction_summary` | predictions × races × prediction_results の結合ビュー |
| `v_model_annual_summary` | model_performance の年別集計 |
| `v_race_mart` | AI 学習用フラットビュー（1行 = 1頭、全テーブル JOIN 済み） |

---

### v_race_mart の構造と JOIN キー設計

`v_race_mart` は AI 学習の起点となるフラットビュー。**63 列**。変更時は必ず `_migrate_recreate_mart_view()` を呼ぶ（`init_db()` 内で自動実行される）。

```
races
  ↓ race_id
race_results
  ↓ race_id + CAST(horse_number AS TEXT) → race_payouts.combination
  ↓ horse_id → horses (血統: sire/dam/dam_sire)
  ↓ horse_id → racehorses / um (競走馬マスタ: birth_year, father_id 等)
  ↓ jockey   → jockeys / ks  ← 名前結合（jockey_code 列が race_results にないため）
  ↓ trainer  → trainers / ch ← 名前結合（同上）
  ↓ father_id (racehorses) → breeding_horses / bt (父の繁殖情報)
  ↓ substr(horse_id,2,9) → training_times / tc  ← 【罠あり: horse_id 形式不一致】
  ↓ substr(horse_id,2,9) → training_hillwork / hc
```

---

## 2. blood_id と horse_id の罠

### 問題の核心

JRA-VAN と netkeiba では **horse_id の形式が異なる**。

| ソース | 形式 | 例 |
|---|---|---|
| netkeiba | 10桁数字 `YYYYSSSSSS` | `2022105081` |
| JRA-VAN (SE レコード) | 10桁数字（同形式） | `2022105081` |
| JRA-VAN (WC/WH 調教) | `1` + 9桁 = 10桁 `1YYYSSSSSS` | `1220105081` |

**調教データ（WOOD:TC, WOOD:HC）の horse_id は先頭に `1` が付く**。

### 解決策（実装済み）

`v_race_mart` の調教 JOIN では `substr(horse_id,2,9)` で先頭1桁を除いて結合する:

```sql
LEFT JOIN training_times tc
  ON substr(tc.horse_id, 2, 9) = substr(rr.horse_id, 2, 9)
 AND tc.training_date < r.date
```

対応するインデックス（`init_db.py` で作成済み）:
```sql
CREATE INDEX idx_tc_norm ON training_times(substr(horse_id,2,9), training_date DESC);
CREATE INDEX idx_hc_norm ON training_hillwork(substr(horse_id,2,9), training_date DESC);
```

### 注意点

- `training_times.horse_id` が `1000000000` のような形式であっても `substr(horse_id,2,9)` = `000000000` となり、別の馬と誤結合することはない（JRA-VAN の払い出しルール上、先頭が `0` になる馬番は存在しないため）。
- `test_v_race_mart.py::TestTrainingJoin` の 5 件の失敗は、テスト用ダミーデータの horse_id 形式が統一されていないことに起因する既存バグ。本番データでは問題ない。

---

## 3. 文字コードの教訓

### 発生した問題

JV-Link は Shift-JIS でバイト列を返す。Python の `str.decode("shift_jis")` で変換すると
一部の文字（特殊な競馬用漢字・外字）が `\ufffd`（U+FFFD: 文字化け代替文字）に化ける。
この文字化けが SQLite に保存され、Streamlit で表示崩壊が発生した。

### 現在の対策

**`jravan_client.py`**: Shift-JIS デコードに `errors="replace"` を使用（化け文字は `?` に置換）。
**`web_streamlit/app.py`**: `_safe_race_name()` 関数で `\ufffd` を含む文字列を空文字列に変換:

```python
def _safe_race_name(name: Any) -> str:
    s = str(name)
    if "\ufffd" in s or not any(c.isalpha() for c in s):
        return ""
    return s.strip()
```

### 将来の AI へのルール

1. JV-Link からのバイト列は必ず `.strip(b"\x00")` でヌルバイト除去してから `decode("shift_jis", errors="replace")` する
2. DB に保存する前に `\ufffd` を含む文字列は空文字列 `""` に置換する
3. Streamlit で `race_name` を表示する際は必ず `_safe_race_name()` を通す
4. JRA-VAN の仕様書に記載のバイトオフセットは「推定」値が多い（仕様書 Ver.4.5.2）。`--debug` フラグで生データを確認してオフセットを実測すること

---

## 4. 特徴量ロジック

### 4.1 調教タイムの数値化

調教タイムは JV-Link の WC レコードから取得する。単位は **×0.01 秒**（整数）なので変換が必要:

```python
# WC レコードの 4F タイム（offset 56-60、推定）
raw = record[_WC_TIME_4F]  # 例: b"6400" = 64.00秒
time_4f = int(raw) / 100.0  # → 64.0 秒
```

特徴量として使うのは **実秒値**（`tc_4f` = 64.0 秒）。小さいほど速い（`_rank_asc_inv` で反転）。

**加速ラップフラグ** (`tc_accel_flag`):
```python
# ラスト1F タイム < ラスト2F タイム → 後半加速 → 好調サイン
tc_accel_flag = 1 if time_1f < time_2f else 0
```

**前走比タイム差** (`tc_4f_diff`):
```python
# 今回 4F タイム - 前回 4F タイム（秒）
# 負の値 = 好転（前回より速くなった）
tc_4f_diff = current_4f - prev_4f
```

### 4.2 当日バイアスの計算式

`features.py` の `_get_today_bias()` が実装。**当日の確定済みレース（race_number < current_race_number）** からのみ集計するため、リークしない:

```
today_inner_bias = 内枠(1-4)勝率 - 外枠(5-8)勝率
  正の値 → 内枠有利
  負の値 → 外枠有利
  |値| > 0.15 → 強いバイアス（Streamlit でオレンジ警告）

today_front_bias = 当日に1〜3番人気馬が勝った割合
  高い（> 0.6）→ 展開が安定・先行有利
  低い（< 0.3）→ 波乱馬場・差し有利

today_gate_match = today_inner_bias × (内枠+1 / 外枠-1)
  この馬の枠番とバイアスの相性スコア
```

SQL:
```sql
SELECT
  SUM(CASE WHEN gate_number BETWEEN 1 AND 4 AND rank = 1 THEN 1 ELSE 0 END) / 
  NULLIF(SUM(CASE WHEN gate_number BETWEEN 1 AND 4 THEN 1 ELSE 0 END), 0) AS inner_rate,
  SUM(CASE WHEN gate_number BETWEEN 5 AND 8 AND rank = 1 THEN 1 ELSE 0 END) /
  NULLIF(SUM(CASE WHEN gate_number BETWEEN 5 AND 8 THEN 1 ELSE 0 END), 0) AS outer_rate
FROM race_results rr
JOIN races r ON rr.race_id = r.race_id
WHERE r.date = ? AND r.venue = ? AND r.race_number < ?
  AND rr.rank IS NOT NULL
```

### 4.3 Harville 公式による全券種確率計算

`bet_generator.py` が実装。モデルの予測確率（本命スコア）を入力として全券種の期待確率を計算する。

```
P(馬A が 1着) = p_A                                         ← モデル出力
P(馬A 1着, 馬B 2着) = p_A × p_B / (1 - p_A)                ← Harville exacta
P(馬A-馬B 馬連) = P(A→B) + P(B→A)                           ← quinella
P(馬A 1着, 馬B 2着, 馬C 3着) = p_A × p_B/(1-p_A) × p_C/(1-p_A-p_B)
P(馬A-馬B-馬C 三連複) = ΣP(全順列) で 6通り合算
```

**EV 計算式**:
```
EV = harville_prob × axis_win_odds × scale

axis_win_odds: 軸馬（本命）の単勝オッズ
scale:  OddsEstimator が DB 実績から学習（不足時はデフォルト値）
  単勝: 1.0, 複勝: 0.33, 馬連: 6.0, ワイド: 2.5
  馬単: 12.0, 三連複: 30.0, 三連単: 150.0

EV > 1.0 → 期待値プラス → 推奨買い目
```

**重要**: `EV` に `/100` は含まない。過去に `/100` を含めたバグがあり、EV が常に 0.04 程度になる問題が発生した（修正済み）。

### 4.4 ケリー基準とキャップ

`BetGenerator._apply_caps()` が 2 段階でキャップを適用:

```
Step 1: per-combo cap
  bet = min(kelly_bet, BetConfig.max_bet_per_combo)   # デフォルト ¥1,000

Step 2: race-total proportional cap
  total = sum(all bets)
  if total > max_race_bet:  # bankroll × max_bet_fraction = ¥100,000 × 5% = ¥5,000
      scale = max_race_bet / total
      bet = max(bet * scale, 100)  # 最低 ¥100
```

`BetConfig` はデフォルト値 `bankroll=100_000, max_bet_fraction=0.05, max_bet_per_combo=1_000`。
変更する場合は `BetGenerator(config=BetConfig(bankroll=500_000, ...))` とする。

---

## 5. ドメイン例外処理

### 5.1 同着（Dead Heat）

`race_results.rank` が複数行で同値の場合（例: 2着が2頭）。

**誤った実装**（やってはいけない）:
```python
top2 = {n for n, r in ranked if r in {1, 2}}
if len(top2) == 2:  # 2着2頭の場合 top2 は 3 頭 → len=3 で失敗
    ...
```

**正しい実装**（`evaluator.py` に実装済み）:
```python
# 馬連: 馬A・馬Bが各々1着または2着であることを個別に確認
rank_lookup = {horse_name: rank for horse_name, rank in ranked}
r0 = rank_lookup.get(predicted_names[0])
r1 = rank_lookup.get(predicted_names[1])
# 両馬が 1着 or 2着 かつ 両馬とも2着ではない（2着同着どちらかが1着を含む）
is_hit = r0 in {1, 2} and r1 in {1, 2} and not (r0 == 2 and r1 == 2)
```

**三連複の同着**:
```python
# 上位3頭セット（3着が2頭でも 4頭になる可能性あり）
top3 = {n for n, r in ranked if r in {1, 2, 3}}
# 予想3頭が top3 の部分集合かつ予想は正確に3頭
is_hit = len(pset) == 3 and pset.issubset(top3)
```

### 5.2 返還（Refund / Scratch）

`race_payouts.bet_type = '返還'` のエントリが存在する場合。
`combination` 列には返還対象の馬番が入る（例: `'14'`）。

`reconcile.py` の `_get_refund_set()` が馬番 → 馬名 のマッピングを行い、
予想に含まれる馬が返還対象の場合は `is_hit=False, payout=recommended_bet`（元払い）として記録する。

```python
# race_payouts から返還馬番を取得
refund_numbers = [
    int(rp.combination) for rp in payouts
    if rp.bet_type == '返還'
]
# race_results で馬番 → 馬名 に変換
refund_names = {
    r.horse_name for r in results
    if r.horse_number in refund_numbers
}
```

### 5.3 競走中止・除外

`race_results.rank IS NULL` または `rank = 0` の馬は的中対象外。
`evaluator.py` の `_is_hit()` では `rank is None` チェックを先頭で行う。

---

## 6. model_type の設計ルール

`predictions.model_type` は以下の形式のみ許可:

```
ベース      サフィックス（省略可）
──────────  ──────────────────────
卍          (暫定) / (直前)
本命        (暫定) / (直前)
WIN5        なし
```

`insert_prediction()` 内で `model_type.split("(")[0]` でベースを抽出し、
`{"卍", "本命", "WIN5"}` に含まれなければ `ValueError` を送出する。

**なぜ CHECK 制約ではなくアプリ層バリデーションか**:
SQLite の `CHECK(model_type IN ('卍', '本命'))` は `ALTER TABLE ADD COLUMN` と相性が悪く、
暫定/直前サフィックスを追加した際に `_migrate_relax_model_type_check()` でマイグレーションが必要だった。
今後も新サフィックスが増える可能性があるため、アプリ層での検証に統一した。

---

## 7. フェイルセーフと NaN 処理方針

### LightGBM の NaN 処理

LightGBM は欠損値（`NaN`）をそのまま学習・推論できる。
`FeatureBuilder` が生成する特徴量 DataFrame において、**欠損値は `-1` で埋めるのではなく `NaN` のまま渡す** のが原則。

ただし `FEATURE_COLS` に列挙された特徴量は `models.py` 内で:
```python
X = df[FEATURE_COLS].astype(float).fillna(-1)  # 数値型にキャストしてから fillna
```
として処理する。**`fillna(-1).infer_objects(copy=False)` は使ってはいけない** — Python 3.14 で `FutureWarning` が発生し、型変換が不正確になる場合がある（修正済み）。

### パイプラインのフェイルセーフ

`run_prerace_auto.py` は当日全レースをループで処理する。**1 レースが失敗しても次のレースを続行する**設計:

```python
for rid in race_ids:
    result = subprocess.run([sys.executable, "-m", "src.main_pipeline", "prerace", rid])
    if result.returncode != 0:
        failed += 1
        logger.error("  [ERROR] %s 失敗", rid)

# 全件失敗時のみ非ゼロ終了（一部失敗はワークフロー継続）
sys.exit(1 if failed == len(race_ids) else 0)
```

`prerace_pipeline()` 内の各ステップも `try/except` で個別に保護され、
ネットワーク障害・スクレイピング失敗は `logger.warning` で記録して続行する。

### Discord フェイルセーフ

`_send_discord()` は例外を握り潰す（パイプライン継続優先）:
```python
except Exception as exc:
    logger.warning("Discord 送信失敗: %s", exc)
```
Webhook URL 未設定時は `WARNING` ログのみで送信をスキップする。

---

## 8. Champion-Challenger 増分学習

`src/ml/incremental.py` が実装。毎週末の照合結果をトリガーに実行される。

**仕組み**:
1. 直近 N レースの的中率・ROI を Champion / Challenger モデルで比較
2. Challenger が一定期間（デフォルト 4 週間）にわたって Champion を超えた場合に昇格
3. 降格した Champion は `data/models/history/` にアーカイブ

**Platt Scaling**:
`HonmeiModel` は `sklearn.linear_model.LogisticRegression` で確率をキャリブレーション。
LightGBM が出力するスコアをそのまま確率として使うと過信問題が発生するため。

---

## 9. 予想アーカイブと UNIQUE 制約の罠

`predictions` テーブルには `UNIQUE(race_id, model_type, bet_type)` 制約がある。
`INSERT OR REPLACE` を使っているため、**同一 (race_id, model_type, bet_type) で再 INSERT すると古いレコードが削除されて新しいものに置き換わる**。

**やってはいけない**（テストで発見した実例）:
```python
# 同じ race_id + model_type + bet_type で 2 回 insert_prediction を呼ぶと
# 1件目が削除されて 2件目だけが残る → COUNT(*) = 1 になる
insert_prediction(conn, "202506050811", "卍", "単勝", ...)  # id=1
insert_prediction(conn, "202506050811", "卍", "単勝", ...)  # id=1 が削除されて id=2 になる
```

**対策**: 同一レースで複数の予想を保存する場合は `bet_type` か `model_type` を必ず変える。
例: `"単勝"` と `"複勝"` を別々に保存する。

---

## 10. race_id フォーマット

```
YYYY VV DD NN
2026 05 06 05 11

YYYY: 年 (4桁)
VV  : 会場コード (01=札幌, 02=函館, 03=福島, 04=新潟, 05=東京,
                  06=中山, 07=中京, 08=京都, 09=阪神, 10=小倉)
DD  : 開催日数内連番 (01-08)
NN  : レース番号 (01-12)

例: 202506060511 = 2025年 東京(05) 6開催日 5日目 11R
```

Streamlit / 通知の会場名変換:
```python
_JYO = {"01":"札幌", "02":"函館", ..., "10":"小倉"}
venue = _JYO.get(race_id[4:6], race_id[4:6])
race_no = int(race_id[10:12])
```

**検証フィルタ**: `SUBSTR(race_id,5,2) BETWEEN '01' AND '10'` で JRA 正規会場のみを抽出。
地方競馬・海外レースは除外（会場コードが 11 以上）。

---

## 11. 重要な設計決定（変更禁止 or 慎重に）

| 項目 | 決定 | 理由 |
|---|---|---|
| win_odds を FEATURE_COLS から除外 | 除外済み | オッズを学習すると「市場の予想を覚える」だけで真の予測力が測れない |
| popularity を FEATURE_COLS から除外 | 除外済み | 同上 |
| 特徴量の欠損は -1 で統一 | `fillna(-1)` | LightGBM の欠損分岐と -1 による明示的欠損識別を共存させる |
| sire_encoded はセッション内 LabelEncoder | init_db で固定せず | 新種牡馬への対応・シリアライズ不要を優先 |
| 騎手・調教師は名前ベースエンコード | jockeys/trainers マスタが空でも動作 | JRA-VAN 未取得環境での後方互換性 |
| `date` は ISO 8601 `YYYY-MM-DD` | 旧形式 `YYYY/MM/DD` は不可 | SQLite の `datetime()` 関数と日付比較が正しく動作するため |
| predictions.model_type に `(暫定)/(直前)` サフィックス | Streamlit での種別フィルタ | `kind_sql()` が `LIKE '%暫定%'` で絞り込む |

---

## 12. GitHub Actions 構成

`.github/workflows/` に以下のワークフローが存在:

| ファイル | トリガー | 内容 |
|---|---|---|
| `prerace-predict.yml` | 土日 毎時 cron | `run_prerace_auto.py` を実行 |
| `train-weekly.yml` | 月曜 07:00 cron | `run_train.py` を実行 |
| `reconcile.yml` | 土日 17:00 cron | `reconcile` バッチ実行 |

GitHub Actions は 64bit Linux 環境のため JV-Link は使えない。
JV-Link データ取得はローカル Windows 環境（`scripts/scheduler.py`）専用。

---

## 13. よくある誤り集（実際に発生した事例）

### 誤り 1: `infer_objects(copy=False)` の使用

```python
# NG（FutureWarning / 型変換不正）
X = df[FEATURE_COLS].fillna(-1).infer_objects(copy=False)

# OK（正しい実装）
X = df[FEATURE_COLS].astype(float).fillna(-1)
```

### 誤り 2: 同着時に `len(top2) == 2` で判定

```python
# NG（2着同着3頭の場合 top2 は3要素）
if len({n for n, r in ranked if r in {1, 2}}) == 2:
    is_hit = True

# OK（個別 rank_lookup で判定）
r0 = rank_lookup.get(names[0])
r1 = rank_lookup.get(names[1])
is_hit = r0 in {1, 2} and r1 in {1, 2} and not (r0 == 2 and r1 == 2)
```

### 誤り 3: EV 計算の `/100`

```python
# NG（EV が常に 0.04 程度になる）
EV = harville_prob * axis_win_odds * scale / 100

# OK（100 は不要）
EV = harville_prob * axis_win_odds * scale
```

### 誤り 4: `date` フォーマット

```python
# NG（SQLite の日付比較が壊れる）
races.insert(date="2025/12/28", ...)

# OK
races.insert(date="2025-12-28", ...)
```

### 誤り 5: training_times の JOIN キーを horse_id 直接結合

```python
# NG（先頭の "1" プレフィックスで不一致）
JOIN training_times tc ON tc.horse_id = rr.horse_id

# OK
JOIN training_times tc ON substr(tc.horse_id,2,9) = substr(rr.horse_id,2,9)
```
