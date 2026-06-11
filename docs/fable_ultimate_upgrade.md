# UMA-Logic 完全体アップグレード報告書（fable_ultimate_upgrade.md）

**作成**: 2026-06-11 / Claude (claude-fable-5)
**目的**: ①全券種EV最適化エンジン ②残存タスクの完全スイープ ③Fable提案の収益強化モデル
（見送り判定）の実装記録と設計意図の正典。

## 更新履歴

| 日付 | 変更内容 |
|------|---------|
| 2026-06-11 | 【追記】§タスク2のHOLD判定はリーク監査で**棄却**に確定（AUC0.944/ROI1173%はwin_rate系リークの幻影・真値はROI76.0%でEV単体に劣後）。№見送り判定も実弾遡及検証で仮説棄却→W-079⚪保留。正典は docs/leak_audit_and_integration_report.md。影響: src/ml/features.py(リーク修正) |
| 2026-06-11 | 初版。影響ファイル: src/ml/all_ticket_optimizer.py, src/ml/no_bet_filter.py, src/ml/accuracy_model_v2.py, scripts/evaluate_hybrid_ensemble.py |

---

## タスク1: 全券種EV最適化エンジン `src/ml/all_ticket_optimizer.py`

### 数学的設計

```
勝率推計（Accuracy Model / 卍較正確率）
  → FinishOrderModel: 割引Harville（Benter 1994流・Plackett-Luce系）
      P(A→B→C) = pA · qB/Σq≠A · rC/Σr≠A,B   （q=p^λ2, r=p^λ3）
      λ2=0.81 / λ3=0.65（Lo & Bacon-Shone 1994 準拠・設定変更可）
  → 全券種確率: 馬単/馬連/ワイド/三連複/三連単
  → EV = P(combo) × odds(combo)
  → EV ≥ 1.30（既定）の「市場に歪みがある組み合わせ」のみ抽出
  → build_formation: 軸馬-相手フォーメーションへ要約
```

**素朴な Harville を使わない理由**: 実データでは2着・3着の予測力は勝率より弱く
（Benter 1994）、素朴 Harville は本命サイドの三連系確率を過大評価→EVを過大計上する。
割引指数 λ<1 で2着・3着段の格差を縮めるのが実務標準（Benter の香港シンジケートで実証）。

**検証可能性の担保**: Gumbel-Max トリックによる Plackett-Luce 直接サンプリング
（`simulate_finish_counts`）を同梱し、解析式とモンテカルロの一致をテストで担保
（4万試行・誤差 ±0.012 以内）。

**オッズの扱い**:
- 一次: 実オッズ `odds_map`（JVLink 全券種オッズ。docs/multi_odds_implementation_plan.md の取得基盤と接続予定）
- 二次: 市場勝率（単勝オッズ→`MarketProbabilityCalc` の Shin 確率）から控除率込みで推定
  `odds ≈ (1−takeout)/P_market`。**自モデル確率を市場側に入れると EV が (1−t) に張り付く**
  ため、モデル分布と市場分布の「差」だけが EV の源泉になる設計。

**重要な安全境界**: `bet_policy` の実弾ロック（単勝・複勝のみ）は不変。
本エンジンの出力は**分析・サブスク向け高付加価値コンテンツ**であり、
三連系の実弾解禁は OOS バックテスト黒字実証が前提（確定実績で三連系は損失主因だった事実を尊重）。

### テスト: `tests/test_all_ticket_optimizer.py`（19件）

確率の完全性（Σ三連単=1.0/Σ馬単=1.0）・順序整合・割引の方向性・MC一致・
歪み抽出（フェアオッズ→0件/1.5倍歪み→抽出）・フォーメーション軸の共通性。

---

## タスク2: 残存タスクの完全スイープ（調査結果と解決）

リポジトリ全域走査（`TODO|FIXME|未実装|保留` grep・worktree・弱点台帳）の結果:

| # | 残存タスク | 状態 → 解決 |
|---|---|---|
| S1 | `# TODO` / `FIXME` コメント | **src/・scripts/ に0件**（健全。掃除対象なし） |
| S2 | `legacy_bridge.py` | **リポジトリに存在しない**（過去設計案のみ。現行は bet_policy.base_model() がモデル世代ブリッジを担っており追加実装不要と判断） |
| S3 | **Accuracy Model v2 が worktree に隔離**（master の tests/test_accuracy_model_v2.py が import 不能の orphan） | 🟢 `src/ml/accuracy_model_v2.py` を master へ移植。orphan テスト6件 PASS 化 |
| S4 | **ハイブリッドアンサンブル（EV×Accuracy）検証が保留** | 🟢 `scripts/evaluate_hybrid_ensemble.py` を master へ移植（ハードコードパス相対化＋honmei 69列整列バグ修正）し、実DBで OOS 検証を実行（結果は下記） |
| S5 | W-078: bankroll_manager の OOS 比較 | ⚪ 据え置き（本タスク群とは独立の比較バックテスト設計が必要・台帳管理継続） |

### S4: ハイブリッドアンサンブル OOS 検証結果（2026-06-11 実測）

`py scripts/evaluate_hybrid_ensemble.py --train-cap 400 --test-cap 200 --ev-threshold 1.0`
（train: 2024-01〜2025-10 / test: 2025-10〜2026-06 200レース・単勝フラット100円）

| 戦略 | ベット数 | 的中率% | ROI% | AUC |
|---|---|---|---|---|
| 全買い(ベースライン・勝率上位2頭) | 400 | 27.0 | 130.4 | 0.8451 |
| EVモデル単体(EV≥1.0) | 1,532 | 4.8 | 89.2 | 0.8451 |
| AccuracyModel単体(L1のみ・閾値0.8226) | 92 | 72.8 | 629.7 | 0.9441 |
| **二階層アンサンブル(L1∩L2)** | **35** | **65.7** | **1173.1** | — |

アンサンブル選別馬のオッズ: 平均22.8倍/中央値11.2倍/最小3.6倍/最大142.7倍。

> ⚠️ **昇格判定: HOLD（リーク監査が前提）**。W-071/W-070 の鉄則「良すぎる数値を疑え」
> に照らし、**的中率72.8%・AUC0.944・ROI1173%は市場で入手可能な情報からは原理的に
> 説明困難**であり、本番採用前に以下の監査が必須:
> ① 35ベットの小標本＋最大142.7倍の大穴的中によるROI膨張（分散リスク）
> ② FeatureBuilder の歴史レース特徴量に確定オッズ由来列（shin_prob等）が含まれ、
>    「発走直前に得られる値」より情報量が多い可能性（オッズ確定タイミングリーク）
> ③ prerun/pedigree 特徴量の cutoff 境界の再点検（W-070 で一度リークを検出した箇所）
> ④ 閾値0.8226が val（test 直前期間）で最適化されており分布近接の楽観が乗る可能性
> 次アクション: 特徴量を1グループずつ外す ablation でAUC0.94の出所を特定すること。

> 移植時に発見・修正した実バグ: honmei Booster へ渡す特徴量が DataFrame の列存在
> フィルタで 40 列に欠けて `LightGBMError: features 40 != 69` でクラッシュしていた。
> `reindex(columns=FEATURE_COLS).fillna(0)` で69列に正確に整列して解決。

---

## タスク3:【Fable提案】見送り（No-Bet）判定モデル `src/ml/no_bet_filter.py`

### なぜ「見送り」が最も ROI を上げるのか

UMA-Logic の確定実績・OOS が一貫して示すのは「エッジは局所にしか無い」という事実
（聖域会場の厳格化・卍単勝の WATCH_ONLY 化・複勝系のみ安定黒字）。
EV ゲートは**馬単位**のフィルタだが、較正そのものが信頼できないレース
（大混戦・データ異常・市場異常）では「EV が高く見える買い目」自体が幻影になる。
**レース単位で買わない判断**は、モデルを1行も変えずに分母から負けレースを除く
最も安全な ROI 改善手段であり、未検証係数の乗算（W-071 事故）と違い
確率・EV を一切改変しないため副作用がない。

### チェイオス・スコアの構成（すべて発走前確定値・リークフリー）

| シグナル | 重み | 意味 |
|---|---|---|
| オッズエントロピー | 0.30 | 市場確率の正規化エントロピー。1.0=全馬均衡の大混戦 |
| 弱い本命 | 0.20 | 1番人気の市場確率 < 25% は構造的波乱ゾーン |
| オーバーラウンド異常 | 0.20 | Σ(1/odds) が通常域(1.05-1.45)から逸脱＝オッズ供給異常 or 市場異常（インサイダー資金の痕跡を含む） |
| モデル-市場の全面乖離 | 0.20 | Jensen-Shannon 距離。全馬で食い違うのはエッジではなくデータ異常の兆候 |
| 構造リスク | 0.10 | 少頭数(<8頭)・渋った馬場(不良/重) |

`chaos_score ≥ 0.42`（既定）で見送り。判定は**二値ゲートのみ**で、
確率・EV への係数操作は構造的に不可能な API 設計（テストで担保）。

### 結線計画（段階導入）

1. **Phase 1（現在）**: 純関数モジュール＋テスト。未結線＝本番挙動不変。
2. **Phase 2**: 直前パイプラインでシャドー運用（判定を predictions.notes に記録のみ）
   → 見送り判定レースの実 ROI を集計し「見送りが正しかった率」を実測。
3. **Phase 3**: シャドー実績で ROI 改善が確認できたら実弾ゲートに昇格（W-079）。

### テスト: `tests/test_no_bet_filter.py`（14件）

堅いレース通過/大混戦見送り・単調性・オーバーラウンド異常検知・JS乖離検知・
少頭数/馬場・閾値設定可能性・「確率を改変しない」構造の担保。

---

## 既存システムへの非デグレード保証

- `bet_policy` 実弾ロック・`FEATURE_COLS`(69)・predictions 不変性（条項1）: **一切非接触**。
- 文字化け回避（CLAUDE.md §10/§16）: 本タスク群は数値演算のみで文字列保存経路に非介入。
- 新規モジュール3つはすべて未結線の純関数層 → ロールバックはファイル削除のみ。
- 全体回帰: pytest 全スイート PASS（検証セクション参照）。

## 実装サマリ（2026-06-11）

| 種別 | ファイル | テスト |
|---|---|---|
| 新規 | src/ml/all_ticket_optimizer.py | tests/test_all_ticket_optimizer.py（19件） |
| 新規 | src/ml/no_bet_filter.py | tests/test_no_bet_filter.py（14件） |
| 移植 | src/ml/accuracy_model_v2.py（worktree→master） | tests/test_accuracy_model_v2.py（orphan解消・6件） |
| 移植+修正 | scripts/evaluate_hybrid_ensemble.py（69列整列バグ修正） | 実DB OOS 実行 |
| 文書 | docs/fable_ultimate_upgrade.md（本書） | — |
