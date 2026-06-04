# UMALOGI 投資戦略 ＆ プロジェクト・ハンドオーバーシート

> 生成日: 2026-06-04 ／ master VERSION: `1.4.3-dev`
> 作成: Claude（マックスプラン終了に向けた完全資産化タスク）
> 目的: 開発一時停止・無人運用に備え、現在の投資戦略（期待値最大化のルール）と
>       再開時の残タスクをコードから言語化し、永続的な引き継ぎ書として残す。
> システム構造は `docs/ARCHITECTURE_FINAL.md` を参照。

---

## 1. 現在の投資戦略（期待値最大化のルール）

### 1.1 大原則 — 「破綻モデルの実弾稼働を拒否する」安全第一

確定実績分析（2026-05-31）の結論：
**三連系・馬連・馬単・ワイドは控除率＋点数増で構造的に負け、卍が唯一の勝ち頭。**
これを受け、実弾（実際に投票する買い目）は厳格に縮退している。
すべての判定は `src/ml/bet_policy.py`（実弾の**単一真実源**）に集約され、
買い目フィルタ・ROI 会計・Discord 通知ラベルは全てここを参照する。

### 1.2 実弾モデル（`bet_policy.LIVE_MODELS`）

| 区分 | モデル | 状態 | 根拠 |
|---|---|---|---|
| **実弾（LIVE）** | **卍** | 複勝のみ実弾／単勝は WATCH_ONLY | 較正済み EV・唯一の黒字頭（直前 131.8% / 暫定 378.2%）。OOS で複勝が黒字傾向(Challenger 108.8%)・単勝は backtest で100%割れのため複勝のみ投票 |
| **実弾（LIVE）** | **Pure_EV_Edge** | 単複・EV>=1.15 バリアント | 黒字化専用。2年バックテストで ROI 137〜211%（out-of-sample） |
| **実弾（LIVE・選択的）** | **FukushoElite (W-020)** | 複勝特化・EV 最優先ゲート | 統計的複勝 EV>=しきい値。厳格なセグメント条件で多くの開催日に正当に0件 → サイレント障害アラート(W-064)対象から除外 |
| 退避（NON_LIVE_RETIRED） | 本命 / Alpha-Payout | 予想生成・表示は継続、投票対象外 | 確定実績 ROI<100%（本命 直前88%/暫定60%、Alpha-Payout 直前70%）。ROI 回復時に復帰しうる「保留」枠 |
| 観賞用（ORNAMENTAL） | Oracle / HitFocus | note/X 集客専用・実弾対象外 | 赤字（直前 ROI 21〜66%）。集客導線でのみ出力 |

### 1.3 実弾券種の単複限定化

- `bet_policy.LIVE_BET_TYPES = {単勝, 複勝}` — 既定の実弾券種。三連系/馬連/馬単/ワイドは実弾から**完全除外**。
- モデル別オーバーライド `MODEL_LIVE_BET_TYPES`:
  - **卍 → 複勝のみ**（`{複勝}`）。卍×単勝 は `WATCH_ONLY_MODELS` に退避（投票せず予想生成と ROI 監視のみ継続）。
- 判定関数 `is_live_bet(model_type, bet_type)` の条件:
  「観賞用でない」かつ「監視専用でない」かつ「`LIVE_MODELS` に属す」かつ「そのモデルの実弾券種に該当」。

### 1.4 Pure_EV_Edge の期待値最大化ルール（コードロック済み定数）

`src/ml/pure_ev_edge.py` に「結果非依存の規律」として固定:

| パラメータ | 値 | 意味 |
|---|---|---|
| `PURE_EV_THRESHOLD` | **1.15** | EV フィルタ下限（安全マージン。EV>1.0 より厳格） |
| `PURE_KELLY_FRACTION` | **0.10** | 1/10 Kelly（過大ベット防止） |
| `_KELLY_TYPE_CAP` | 単勝 0.02 / 複勝 0.03 | 券種別 Kelly 上限（バンクロール比） |
| `_PROB_FLOOR` | 0.06 | 較正確率がこれ未満の大穴は除外（実エッジは有力馬に在る） |
| `MAX_LIVE_WIN_ODDS` | 50.0 | W-066 大穴足切り。単勝オッズ50超は較正不可信頼として実弾除外 |
| `_MAX_BETS_PER_RACE` | 2 | 1レース最大2点 |
| `daily_loss_limit_pct` | 0.05 | サーキットブレーカー：1日損失上限（バンクロール比） |
| `weekly_loss_limit_pct` | 0.12 | サーキットブレーカー：1週損失上限 |

確率は卍の Isotonic 較正済み確率（`manji_calibration.calibrate_win_prob`）をベースに、
複勝は専用 Platt 較正器で P(複勝圏) を較正する。

### 1.5 ROI / EV の計算ロジック（`pnl_accounting` のコスト算出基準）

```
EV       = モデル確率 P × 推定払戻 / 100        （買い目基準は EV > 1.0）
真コスト = payout − profit                       （= ¥100 × 点数。会計の唯一の基準）
真ROI    = Σ payout / Σ 真コスト × 100 [%]
的中率   = Σ is_hit / N × 100 [%]
```

- **会計の単一真実源**: `src/ml/pnl_accounting.compute_live_roi()`。
  - `FLAT_UNIT_YEN = 100`（`flat_cost(n) = ¥100 × 点数`）を会計基準とし、
    **Kelly 実発注額 `recommended_bet` はコスト基準に使わない**（単価不統一バグ回避）。
    これにより賭け額の大小に依存しない stake-independent な ROI 比較を保証する。
  - `is_superseded=1`（直前再推論で論理無効化された旧予想）は**二重計上を避けるため除外**。
  - `live_only=True` のとき `is_live_bet()` で実弾のみに絞り込む。
- **シャドー A/B**（`compute_ab_variants` / W-057）: Pure_EV_Edge「適用」vs「非適用(従来単複=本命/卍/Alpha-Payout)」の確定 P&L を比較。
  昇格基準は `AB_MIN_RACES=100` 消化かつ ROI 差 `+10.0pt` 以上。
- **競馬ドメイン例外処理**（必須）: 同着＝払戻分割 / 返還＝`bet_type='返還'` で 100 円返還 / 競走中止＝`rank IS NULL/0` は的中対象外。

### 1.6 集客（Note 販売導線）の方針

実弾とは完全分離。`src/ops/money_management.py` は「¥10,000 で買うとしたらこう配分する」
参考額（EV 連動で 100/300/500 円ラベル）を**表示専用**で算出し、`bet_policy`・実弾投票・DB とは切り離されている。
観賞用モデル（Oracle/HitFocus）は集客記事でのみ露出し、実弾会計には一切含めない。
note 公開・ペイウォール（`IS_PREMIUM_NOTE`）の最終操作は社長が手動で行う（`note_draft_publisher` は下書き保存のみ）。

---

## 2. Next Steps（v1.5.0 NAR 統合に向けた残タスク）

> NAR 基盤は `feature/nar-support`（v1.5.0-dev）に隔離実装済み（`src/nar/`・NoteBet 互換アダプタ・15テスト PASS）。
> master へのマージには以下が未完。設計の正典は `docs/5_nar_integration_spec.md`。

### 2.1 datasource の永続化（最優先）
- `races` / `race_results` への `datasource`（`'jra'`/`'nar'`）・`region`・`grade` カラム追加。
- マイグレーション `init_db.py:_migrate_nar_support()` の実装と既存データの `datasource='jra'` バックフィル。
- 複合インデックス `idx_races_datasource_date (datasource, date)` の作成。
- **作業前バックアップ義務**（CLAUDE.md 条項4）: `data/backups/` に日付入りバックアップ後に着手。

### 2.2 RaceDataProvider の master 結線
- `src/scraper/base_provider.py`（抽象 ABC + DTO）・`jra_provider.py`（既存コードのラッパー化）を master へ。
- `src/pipeline/prediction.py:prerace_pipeline(race_id, provider=None)` を provider 引数化（provider 未指定＝JRADataProvider で後方互換維持）。

### 2.3 NAR 専用予想モデル
- `nar_provider.py`（nar.netkeiba スクレイピング、将来 NAR DATA Gateway API へ移行）。
- `nar_features.py`（`NARFeatureAdapter`：地方騎手勝率・同距離勝率・グレードランク・出走頭数比・前走間隔）。
- 過去1年分 NAR データで NAR 向けモデルを訓練・バックテスト（共通コア `src/ml/` は不変のまま）。

### 2.4 スケジューラの分離
- NAR は毎日開催・JVLink 不要（64bit 直接スクレイピング）→ JRA とは別系統の常駐ループ（`today_auto_runner_nar`）。
- 二重自動運転防止（オートパイロットと scheduler.py の排他ガード思想を NAR にも適用）。
- 週末凍結ルール（条項2）は NAR には「改修凍結」のみ適用、毎日の稼働は継続。

### 2.5 横断的な保留事項（master 既知）
- **W-001/W-002**: PCI/加速力スコアの FEATURE_COLS 正式統合 → 全モデル再訓練（現状は非破壊連結検証のみ・69列不変）。
- **X 予想シグナル（第4ファクター）**: `x_scraper` / `x_signal_parser` の本番配線（現状 `x_signals` 0件・未配線・W-065）。
- **WIN5 JVLink 化（Plan B）**: SID/32bit 制約解消後に netkeiba 依存をゼロ化。

---

## 3. 無人運用・再開のためのクイックリファレンス

| 操作 | コマンド |
|---|---|
| 一括起動 | `scripts/bat/start_umalogi.bat` |
| 一括停止 | `scripts/bat/stop_umalogi.bat` |
| DB 件数の直接確認 | `py -c "import sqlite3; con=sqlite3.connect('data/umalogi.db'); print(con.execute('SELECT COUNT(*) FROM predictions').fetchone())"` |
| テスト | `pytest` |
| 整形 | `ruff format <変更ファイルのみ>`（リポジトリ全体一括は禁止） |
| ダッシュボード | `py -m streamlit run web_streamlit/app.py --server.port 8501` |

> 「UI に出ない = データ消失」は誤り（CLAUDE.md 条項4 事故事例）。必ず DB を直接確認してから判断すること。
> 物理削除（DELETE/DROP）は原則禁止・大規模操作は事前承認＋バックアップ必須。

---

> 本書は読取専用解析に基づく逆コンパイル文書である（コード変更を伴わない）。
> 投資戦略の数値は確定実績分析時点のもの。実弾稼働の最新状態は常に `bet_policy.py` を正とする。
