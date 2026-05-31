# ML ガイドライン

特徴量設計・モデル選定・評価指標に関するガイドライン。
エージェントはモデル構築・改善タスクの前に必ずこのファイルを参照してください。

---

## モデル構成

| モデル | 目的変数 | 用途 |
|---|---|---|
| **本命モデル** (HonmeiModel) | `is_win` (1着=1, 他=0) | 的中率特化。馬連・三連複の本線的中 |
| **卍モデル** (ManjiModel) | `ev_target` (払戻/馬券代) | 回収率特化。EV > 1.0 の穴馬発掘 |

実装: `src/ml/models.py`

---

## 特徴量設計

### 現在の特徴量 (FEATURE_COLS)

```python
FEATURE_COLS = [
    "weight_carried",          # 斤量
    "horse_weight",            # 馬体重
    "win_odds",                # 単勝オッズ
    "popularity",              # 人気順
    "win_rate_all",            # 馬の通算勝率
    "win_rate_surface",        # 馬場別勝率
    "win_rate_distance_band",  # 距離帯別勝率
    "recent_rank_mean",        # 直近5走平均着順
    "surface_code",            # 馬場コード (芝=0, ダート=1, 障害=2)
    "sex_code",                # 性別コード (牡=0, 牝=1, セ=2)
    "venue_encoded",           # 開催場コード
    "sire_encoded",            # 父馬エンコード
    "distance",                # 距離
]
```

### 追加予定の特徴量（優先度順）

#### 高優先度（データ取得済み → 即実装可能）

| 特徴量 | ソース | 算出方法 |
|---|---|---|
| `horse_weight_diff` | `v_race_mart` | 馬体重増減（既存列） |
| `jockey_win_rate_90d` | `race_results` | 直近90日の騎手勝率 |
| `trainer_win_rate_90d` | `race_results` | 直近90日の調教師勝率 |
| `jockey_horse_combo_rate` | `race_results` | 騎手×馬の過去着順平均 |
| `days_since_last_race` | `race_results` | 前走からの間隔（日数） |
| `gate_win_rate` | `race_results` | 枠番別勝率（distance帯×surface） |

#### 中優先度（DIFN 取得後に有効）

| 特徴量 | ソース | 算出方法 |
|---|---|---|
| `jockey_code_encoded` | `v_race_mart.jockey_code` | LabelEncoder |
| `trainer_code_encoded` | `v_race_mart.trainer_code` | LabelEncoder |
| `horse_age` | `race_year - birth_year` | 出走時の馬齢（年齢） |
| `east_west_match` | `horse_east_west == jockey_east_west` | 美浦/栗東の一致フラグ |

#### 低優先度（BLOD/WOOD 取得後に有効）

| 特徴量 | ソース | 算出方法 |
|---|---|---|
| `last_tc_speed_index` | `v_race_mart.last_tc_*` | `200 / last_tc_4f * 100` |
| `father_sire_encoded` | `v_race_mart.father_sire_id` | 父の父系統エンコード |
| `bms_encoded` | `v_race_mart.grandsire_id` | 母父系統エンコード |

---

## 増分学習 (Incremental Learning)

実装: `src/ml/incremental.py`

```
増分更新 (毎レース後)
  ├─ new_races <= 100件  : LightGBM init_model で +50 rounds 追加
  └─ new_races > 100件   : 自動で full_retrain に切り替え

全件再学習 (毎週月曜)
  └─ GroupKFold(5分割) で CV AUC 計算 → data/models/ に保存
     └─ data/models/history/ に直近10世代をアーカイブ
```

---

## 評価指標

### 本命モデル

| 指標 | 目標値 | 説明 |
|---|---|---|
| CV AUC | > 0.70 | 馬連的中の識別能力 |
| 単勝的中率 | > 30% | 1着予想の正解率 |
| 馬連的中率 | > 40% | 2頭軸の正解率 |

### 卍モデル

| 指標 | 目標値 | 説明 |
|---|---|---|
| 単勝回収率 | > 110% | 年間単勝収支 |
| 三連複回収率 | > 120% | 年間三連複収支 |
| 最大ドローダウン | < 30% | 月別収支の最大下落幅 |

### バックテスト手順

```bash
# 年度シミュレーション
py -3 src/simulate_year.py --year 2024

# 会場別・距離別に分解して精度を評価
py -3 src/simulate_year.py --year 2024 --venue 東京 --surface 芝
```

---

## WIN5 エンジン

実装: `src/ml/win5.py`

```python
engine = Win5Engine(model=honmei_model, ev_threshold=1.0)

# 全組み合わせ（頭数^5 通り）
bets = engine.predict(conn, race_ids=[...5つのrace_id...])

# 上位3頭のみで絞る高速版（3^5 = 243 通り）
bets = engine.predict_top_n(conn, race_ids=[...], top_n_per_race=3)
```

期待値 = 勝率の積 × WIN5 返還率(72.5%) × 100 / 100

---

## LightGBM 実装パターン

```python
# 増分学習（既存モデルに追加 boosting）
import lightgbm as lgb

ds_new = lgb.Dataset(X_new, label=y_new)
new_booster = lgb.train(
    params,
    ds_new,
    num_boost_round=50,
    init_model=existing_booster,   # ← ウォームスタートのキー
    callbacks=[lgb.log_evaluation(-1)],
)

# GroupKFold クロスバリデーション（レース単位でリーク防止）
from sklearn.model_selection import GroupKFold
gkf = GroupKFold(n_splits=5)
for train_idx, val_idx in gkf.split(X, y, groups=race_ids):
    ...
```

---

## アンチパターン（禁止事項）

1. **ランダム分割の使用禁止**: 同一レースの馬が train/val に分かれるとリークする。
   必ず `GroupKFold(groups=race_id)` を使うこと。
2. **未来情報の混入禁止**: 特徴量に「当日オッズの確定値」「当日天候」は使わない
   （出馬表確定後のオッズは使用可）。
3. **生オッズの直接使用禁止**: オッズは市場確率に変換（`1/odds` 正規化）してから使う。
4. **外れ値の無処理禁止**: `win_odds` が 99.9 等の異常値は上限クリップ (`clip(max=80)`) を適用。
