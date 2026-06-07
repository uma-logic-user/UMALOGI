# UMALOGI 特徴量ロジックマップ（logic_map.md）

**生成**: 2026-06-07 / 担当: Claude（タスク1.1・並行セッションとの役割分担で本ファイル担当）
**根拠**: `src/ml/models.py:FEATURE_COLS`(69列)、`src/ml/features.py`(FeatureBuilder)、`src/ml/u_score.py`(UScoreEngine)。
**目的**: 推論モデルが使う全特徴量の「算出式・データ源・コード位置」を一元化する。

> ⚠️ **Feature Importance（的中率/期待値への寄与）の可視化はタスク1.2**で、並行セッションが
> `scripts/analyze_model_traits.py`（会場/馬場/クラス/距離帯/モデル/券種の多軸 条件別精度分析）として
> 実装済み。本書はその前提となる「特徴量カタログ＝算出式の正典」を担う。

---

## 0. リークフリーの大原則（最重要）

`FeatureBuilder` は **予測対象レースの結果（着順・走破タイム・着差・上がり3F）を特徴量に使わない**。
`src/ml/features.py:172` に明示:「race_results から安全なフィールドのみ取得（rank/finish_time/margin は取らない）」。
馬成績系（win_rate 等）は対象馬の **過去レース**（`rr.rank IS NOT NULL` かつ日付フィルタ）からのみ集計する。

- 当該レース結果由来の加速力系（pci/last_3f_sec 等）は **`backtest_v2.POSTRACE_LEAK_COLS`** として
  予測特徴量から分離済み（W-070監査・2026-06-07）。本番 `FEATURE_COLS`(69) には含まれない＝クリーン。

---

## 1. 基本数値・カテゴリ（馬・レース属性）

| 特徴量 | 算出式 / データ源 | コード |
|---|---|---|
| `weight_carried` | 斤量（kg）。entries/race_results 由来 | features.py |
| `horse_weight` | 馬体重（kg）。発走前発表値（W-069で直前取得） | features.py |
| `horse_weight_diff` | 馬体重増減（前走比 ±kg） | features.py |
| `distance` | レース距離（m） | races.distance |
| `race_number` | レース番号（1〜12R） | races.race_number |
| `gate_number` | 枠番 | entries |
| `surface_code` | 芝=0 / ダート=1 / 障害=2（未知-1） | `_SURFACE_CODE` |
| `sex_code` | 牡=0 / 牝=1 / セ=2（未知-1） | `_SEX_CODE` |
| `condition_code` | 馬場状態（良/稍重/重/不良）コード | `_CONDITION_CODE` |
| `venue_encoded` | 会場コード（JRA10場+地方/海外） | `_VENUE_CODE` |
| `sire_encoded` | 父サイアーのラベルエンコード（学習時 sire_map を pkl 永続化・推論時ロード） | `_SIRE_MAP_PKL` |
| `jockey_code_encoded` | 騎手のラベルエンコード | features.py |
| `trainer_code_encoded` | 調教師のラベルエンコード | features.py |

**距離バンド** `_DISTANCE_BANDS`: sprint<1400 / mile 1400-1800 / intermediate 1800-2200 / long>2200。

---

## 2. 馬成績（過去走集計・リークフリー）

すべて対象馬 `horse_id` の **過去レース** から集計（`WHERE rr.horse_id=? AND rr.rank IS NOT NULL` ＋日付フィルタ ＋ 当該レース除外）。

| 特徴量 | 算出式 | コード |
|---|---|---|
| `win_rate_all` | Σ(rank=1) / Σ(全出走)（全成績） | features.py ~885 |
| `win_rate_surface` | 同上を同馬場(`r.surface=?`)で限定 | features.py ~900 |
| `win_rate_distance_band` | 同上を同距離帯(下限〜上限)で限定 | features.py ~915 |
| `recent_rank_mean` | 直近5走の平均着順（小さいほど良） | features.py ~860 |
| `days_since_last_race` | 前走からの経過日数（休養度） | features.py |

---

## 3. レース内相対変換（_rank / _zscore）

各 base 特徴量を **レース内（その日の出走馬間）で正規化**したもの。発走前に全馬の値が揃うためリークではない。

| 特徴量 | 算出式 | コード |
|---|---|---|
| `win_rate_all_rank` 等 | レース内での当該特徴量の順位（昇順/降順） | features.py:804-819 |
| `*_zscore` | レース内での当該特徴量の z-score | features.py |
| `recent_rank_mean_rank` / `_zscore` | 平均着順のレース内順位/標準化（`_rank_asc_inv`） | features.py:831 |
| `tc_4f_rank` / `tc_4f_zscore` | 調教4Fタイムのレース内順位/標準化 | features.py |

---

## 4. 調教（追い切り）— `tc_*`(コース) / `hc_*`(坂路)

データ源: `training_times`(WOOD:TC) / `training_hillwork`(WOOD:HC)。
※ 充足率に課題あり（W-068）。netkeiba `oikiri.html` 補完は並行セッションが整備中。

| 特徴量 | 内容 |
|---|---|
| `tc_4f` / `hc_4f` | 直近追い切りの4Fタイム（コース/坂路） |
| `tc_lap` / `hc_lap` | ラップタイム |
| `tc_4f_diff` / `hc_4f_diff` | 標準比の差分 |
| `tc_accel_flag` / `hc_accel_flag` | 終い加速の有無フラグ |
| `tc_speed_index` / `hc_speed_index` | 速度指数 |

---

## 5. 当日バイアス・オッズ動態

| 特徴量 | 算出式 / 内容 | コード |
|---|---|---|
| `today_inner_bias` | 当日の内枠有利度（枠別1着率の偏り） | features.py:719 |
| `today_front_bias` | 当日の先行有利度（脚質バイアス） | features.py |
| `today_race_count` | 当日集計に使ったレース数（信頼度） | features.py |
| `today_gate_match` | 当該馬の枠が当日バイアスに合致するか | features.py |
| `odds_vs_morning` | 直前オッズ / 朝オッズ（市場の評価変化） | features.py |
| `odds_velocity` | オッズ変化速度（急騰=見限り/急落=大口） | features.py / odds_drift |

---

## 6. 騎手・調教師（90日ローリング）

| 特徴量 | 算出式 |
|---|---|
| `jockey_win_rate_90d` | 騎手の直近90日勝率 |
| `trainer_win_rate_90d` | 調教師の直近90日勝率 |
| `jockey_horse_combo_rate` | 騎手×当該馬コンビ成績 |
| `jockey_venue_win_rate` | 騎手×当該会場勝率 |
| `venue_win_rate` | 当該馬×会場勝率 |

---

## 7. U Score サブシステム（`src/ml/u_score.py`）

`UScoreEngine` が GROUP BY バッチSQLで18+因子を算出し、グループ加重和 `u_score` を出力。
各因子は `uf_*` 列として FEATURE_COLS にも個別供給される。

| グループ | 因子（`uf_*`） | 意味 |
|---|---|---|
| ability | win_rate_all/surface/distance, recent_rank, rank_trend, rest_days | 能力・調子 |
| human | jockey_win_rate, trainer_win_rate, jockey_horse_combo, jockey_venue | 人的要因 |
| course | gate_fit, venue_win_rate, east_west_match | コース適性 |
| training | tc_speed, hc_speed | 調教 |
| bloodline | sire_distance, bms_surface, father_sire | 血統適性 |
| crowd | crowd_bias [W-004] | 大衆心理乖離 |

- `u_score`: 上記グループスコアの重み付き合計（`_WEIGHTS`・u_score.py:721）。
- `crowd_bias_ratio`: 人気と実力の乖離比（W-004）。
- `x_consensus_score`: X(Twitter)専門家印のコンセンサス（W-065・現状0埋め＝収集未配線）。

> ⚠️ **W-003 不完全燃焼度は未採用**。2026-06-07 の実証調査（54,330サンプル）で
> 「上がり3F順位 vs 着順」の代替指標は今走複勝率と **相関 -0.099（逆相関）** ＝因子として機能せず。
> 真の実装には通過順位データ（W-073・現状DB未保有）が前提。`scripts/investigate_w003_combustion.py` 参照。

---

## 8. 特徴量の追加・再学習ルール

- 新特徴量は **`FEATURE_COLS`(69) を破壊せず** `backtest_v2.build_feature_cols_v2`（既定リークフリー）で
  別リスト化 → 新モデル世代として再学習（W-070ブリッジ設計 `docs/spec/W070_feature_bridge_design.md`）。
- リークフリー新特徴量: `src/features/prerun.py`（前走詳細）・`src/features/pedigree_te.py`（血統TE）。
  OOS実証: 単勝ROI 51.6%→74.8%（+23pt・黒字未達）。

---

## 9. 関連ドキュメント

- Feature Importance / 条件別精度（タスク1.2・報告事項）: `scripts/analyze_model_traits.py`（並行セッション）
- 弱点台帳: `docs/7_weakness_ledger.md`（W-001/002/070/071/073 等）
- ブリッジ設計: `docs/spec/W070_feature_bridge_design.md`
- 予測ロジック仕様: `docs/1_prediction_logic.md`
