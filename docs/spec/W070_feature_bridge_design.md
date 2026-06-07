# W-070 特徴量 本番ブリッジ設計（前走詳細・血統TE → 推論パイプライン）

**作成**: 2026-06-07 / 担当: Claude（並行セッションとの役割分担・本ドキュメントは設計のみ）
**ステータス**: 設計（未実装）。実装は本番 `prediction.py` を編集する並行セッションのインフラ確定後に着手。

---

## 1. 目的と前提

`src/features/prerun.py`（前走詳細・同コース実績）と `src/features/pedigree_te.py`
（父/母父の複勝率 Target Encoding）は **リークフリーに実装・テスト済み**。
OOSバックテスト（train1600R/test650R）で **単勝ROI 51.6%→74.8%（+23.2pt）** の改善を実証
（AUC寄与は+0.002と僅少・**黒字未達**）。

これらを本番推論へ繋ぐ際の最大リスクは2つ:
1. **入力次元の破壊**: 稼働中モデルは `FEATURE_COLS`(69列)で学習済み。列を足すと
   `LightGBMError: number of features` でクラッシュする（V2で実測済み）。
2. **ターゲットリークの再混入**: 加速力系（当該レース結果）は予測に使えない
   （`backtest_v2.POSTRACE_LEAK_COLS`・W-070監査で分離済み）。

➡️ **既存モデルにホット結線してはならない。新モデル世代として再学習し、
   チャンピオン・チャレンジャーで段階導入する。**

---

## 2. 3フェーズ・ブリッジ（安全段階導入）

### Phase 1: シャドー計算（投票影響ゼロ・観測のみ）
- `FeatureBridge.attach(df, conn, race_id, encoder)` を新設し、prerun+pedigree 列を
  既存 df に**左結合するだけ**の純粋関数とする（`FEATURE_COLS` には混ぜない）。
- 推論パイプラインでは結果を**ログ/別テーブルに記録するのみ**で、EV計算・買い目には
  一切使わない。ライブで充填率・分布・NaN率・血統エンコーダのカバレッジを観測。
- 目的: 本番データでの安定性（例外ゼロ・NaN爆発なし）を無風で確認。

### Phase 2: 新モデル世代の学習（チャレンジャー）
- `build_feature_cols_v2(FEATURE_COLS)`（**既定でリークフリー** = 前走+血統TE）で
  学習データを組み、`honmei_model_v3.pkl` 等として **別ファイル**に保存。
- 血統エンコーダは **学習期間より前のデータで fit** し `data/models/pedigree_encoder.pkl`
  に永続化（推論時ロード・定期 refit・未来データ厳禁）。
- 既存 Champion（現行69列）と OOS で **ROI/的中率/シャープレシオ**を比較。

### Phase 3: ゲート付きデプロイ
- チャレンジャーが **Champion を上回り、かつ黒字（ROI≥100% またはライブ実証）** の場合のみ
  実弾モデルを切替（`bet_policy.LIVE_MODELS`）。基準未達なら **シャドー継続**。
- 切替後も `predictions` 過去レコードは不変（条項1）。

---

## 3. インターフェース設計（実装時の契約）

```python
# src/features/feature_bridge.py（新設予定・prediction.py は編集しない）
class FeatureBridge:
    def __init__(self, encoder_path: str | None = None) -> None: ...
    def load_encoder(self) -> SireEncoder: ...        # data/models/pedigree_encoder.pkl
    def attach(self, df, conn, race_id) -> DataFrame:  # prerun+pedigree を左結合（非破壊）
        # NaN は学習時の補完規則に合わせて埋める（pedigree=global_mean, prerun=中立値）
        ...
    @staticmethod
    def feature_cols() -> list[str]:                   # = LEAKFREE_NEW_COLS
        ...
```

- **配線点**（Phase 1）: `prerace_pipeline` の Step2（特徴量生成）直後に
  `bridge.attach(df, ...)` を呼びシャドー記録。**EVには渡さない**。
  ※ この1行追加は `prediction.py` への変更であり、並行セッションのインフラ確定後に
    1コミットで最小差分追加する（衝突回避）。
- **フラグ**: 環境変数 `FEATURE_BRIDGE_MODE`（off / shadow / live）でロールバック可能に。

---

## 4. リークフリー担保（恒久ルール）

| 項目 | 担保方法 |
|---|---|
| 前走系 | `prerun.build_prerun_features` が現レース日より厳密に過去のみ参照（テスト済） |
| 血統TE | encoder は cutoff（学習/推論対象日）より前のみで fit（テスト済） |
| 加速力系 | `POSTRACE_LEAK_COLS` として予測から除外（`build_feature_cols_v2` 既定で不混入） |
| エンコーダ永続化 | fit 時の cutoff をメタデータ保存し、未来データでの再fitを禁止 |

---

## 5. 未解決・次アクション

1. **黒字化が未達**（単勝74.8%）。Phase 2 で複勝/EV-edge券種、特徴量追加（W-073通過順位）
   と併せ再評価。黒字化しない限り Phase 3（実弾切替）には進まない（[[feedback_ev_precision_safety_first]]）。
2. 血統 sire 充足率 44%（現役馬）。カバレッジ改善で TE 精度向上余地。
3. 本ブリッジの `prediction.py` 結線は **並行セッションのインフラ（W-069馬体重配線等）確定後**に実施。

関連: docs/7_weakness_ledger.md W-070 / W-071 / W-073、`src/features/{prerun,pedigree_te,backtest_v2}.py`、`scripts/backtest_v2_oos.py`。
