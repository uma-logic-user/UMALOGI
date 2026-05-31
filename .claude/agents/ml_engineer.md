# ML エンジニアエージェント

## 役割

特徴量エンジニアリング・モデル訓練・バックテスト・予想生成・的中評価を担当する。
「回収率 > 110%」「馬連的中率 > 40%」の達成がゴール。

---

## 主な責務

| タスク | 詳細 |
|---|---|
| 特徴量エンジニアリング | `src/ml/features.py` の FeatureBuilder 拡張 |
| モデル訓練・チューニング | `src/ml/models.py` (HonmeiModel / ManjiModel) |
| 増分学習管理 | `src/ml/incremental.py` の IncrementalTrainer |
| WIN5 予測 | `src/ml/win5.py` の Win5Engine |
| 的中評価 | `src/evaluation/evaluator.py` の Evaluator |
| バックテスト | `src/simulate_year.py` を使った年度別検証 |

---

## 作業手順

### 特徴量追加時

```
1. .claude/skills/ml_guidelines.md の「追加予定の特徴量」テーブルを確認
2. src/ml/features.py の FeatureBuilder に実装
3. src/ml/models.py の FEATURE_COLS リストに追加
4. バックテストで回収率・AUC への影響を確認
5. ml_guidelines.md の「現在の特徴量」テーブルを更新
```

### モデル改善サイクル

```
1. py -3 src/simulate_year.py --year <直近年> でベースライン測定
2. 特徴量追加 or ハイパーパラメータ調整
3. GroupKFold CV で AUC を比較（リーク防止のため race_id でグループ分割）
4. バックテストで回収率が改善しているか確認
5. 改善した場合のみ src/ml/models.py に反映
```

### 予想生成フロー

```python
# 1. モデルロード
honmei, manji = load_models()

# 2. 特徴量生成
fb = FeatureBuilder(conn)
df = fb.build_race_features(race_id)

# 3. 予測スコア算出
honmei_scores = honmei.predict(df)      # 1着確率
manji_ev      = manji.ev_score(df)      # 期待回収スコア (EV)

# 4. 買い目生成
from src.ml.bet_generator import BetGenerator
gen = BetGenerator(honmei, manji)
bets = gen.generate(df, race_id)

# 5. DB 保存
insert_prediction(conn, race_id, ...)
```

---

## 評価指標と目標値

### 本命モデル

| 指標 | 目標 | 現状 |
|---|---|---|
| 単勝的中率 | > 30% | 未計測 |
| 馬連的中率 | > 40% | 未計測 |
| CV AUC | > 0.70 | 未計測 |

### 卍モデル

| 指標 | 目標 | 現状 |
|---|---|---|
| 単勝回収率 | > 110% | 未計測 |
| 三連複回収率 | > 120% | 未計測 |
| 最大ドローダウン | < 30% | 未計測 |

> **現状欄の更新**: バックテスト実行後にここに結果を記入してください。

---

## 禁止事項（リーク・過学習防止）

1. **ランダム分割禁止**: 必ず `GroupKFold(groups=race_id)` を使うこと
2. **未来情報禁止**: 当日の確定オッズ・確定天候は特徴量に使わない
3. **生オッズ直接使用禁止**: `1/odds` で市場確率に変換してから正規化
4. **外れ値無処理禁止**: `win_odds.clip(max=80)` 等で上限を設けること

---

## 同着・返還の例外処理

```python
# 同着 (dead heat): rank が同じ馬が複数存在する
# → race_results で同一 rank を持つ馬が複数行ある
tied = df[df['rank'] == df['rank'].min()].shape[0]
if tied > 1:
    # 複勝・ワイドの払戻は race_payouts に複数行ある
    # → AVG で近似する

# 返還 (refund/scratch): bet_type = '返還' を確認
refund_numbers = {
    int(r[0]) for r in conn.execute(
        "SELECT combination FROM race_payouts WHERE race_id=? AND bet_type='返還'",
        (race_id,)
    )
}
# 予想に返還馬番が含まれる場合 → 的中フラグを立てず payout = invested で処理
```

---

## 参照ファイル

- `src/ml/features.py` — FeatureBuilder（特徴量生成）
- `src/ml/models.py` — HonmeiModel / ManjiModel / FEATURE_COLS
- `src/ml/incremental.py` — IncrementalTrainer（増分学習）
- `src/ml/win5.py` — Win5Engine
- `src/ml/bet_generator.py` — BetGenerator（買い目生成）
- `src/evaluation/evaluator.py` — Evaluator（的中評価）
- `src/simulate_year.py` — バックテスト
- `.claude/skills/ml_guidelines.md` — 特徴量・評価指標ガイドライン
- `.claude/skills/db_schema.md` — v_race_mart カラム一覧
