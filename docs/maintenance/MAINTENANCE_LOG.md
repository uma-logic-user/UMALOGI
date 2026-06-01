# 🛠️ UMALOGI 保守報告書（MAINTENANCE LOG）

本ファイルは UMALOGI に対するすべての修正・保守作業の正式記録である。
Claude Code（および人間の保守担当）は、コードを変更してコミットするたびに、
**新しいエントリを本ファイルの先頭（最新が上）に追記**しなければならない。

> **記入の絶対ルール**（[`CLAUDE.md`](../../CLAUDE.md) バージョン運用フロー）
> 1. 1 コミット ＝ 1 エントリを原則とする（複数コミットにまたがる一連の作業は 1 エントリにまとめてよい）。
> 2. `VERSION` ファイルを更新したら、本ログの「バージョン」欄に新旧を必ず記載する。
> 3. 仕様書（`docs/spec/`）を更新した場合は「影響範囲」欄に対象ファイルを明記する。
> 4. ロールバック手段（コミットハッシュ・バックアップ）を「ロールバック」欄に残す。

---

## エントリ・フォーマット（コピーして使用）

```markdown
### YYYY-MM-DD — <作業タイトル（1行）>

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) / 担当者名 |
| **修正日** | YYYY-MM-DD |
| **バージョン** | x.y.z → x.y.(z+1)（変更がなければ「据え置き x.y.z」） |
| **種別** | 機能追加 / バグ修正 / リファクタ / ドキュメント / 運用基盤 / セキュリティ |
| **実施内容** | 何を・なぜ・どう変えたかを箇条書きで。 |
| **影響範囲** | 変更したファイル・テーブル・仕様書を列挙。 |
| **検証** | 実行したテスト・バックテスト・E2E と結果（例: `pytest` 1043 PASS）。 |
| **ロールバック** | 直前コミットハッシュ / バックアップ場所。 |
| **関連** | Issue / 弱点ID（W-NNN）/ 仕様書バージョン。 |
```

---

## 保守記録（最新が上）

### 2026-06-01 — 大穴EV暴騰（較正歪み）の安全装置（W-066）

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-01 |
| **バージョン** | `1.1.0` → **`1.1.1`**（PATCH・バグ修正＋安全装置。挙動互換: 人気馬の確率は不変、異常な大穴EVのみ頭打ち） |
| **種別** | バグ修正 / 安全装置（リスク低減） |
| **実施内容** | **真因**: 卍 Isotonic 較正器（`calibrate_win_prob`）は `ev_score` のみで `P(win)` を返し **`odds` を考慮しない**ため、大穴にも中位馬と同じ確率を付与し EV=P×odds が暴騰（odds=49.7 で EV=7.2、卍直前単勝の最大EV=32.5）。Kelly が大穴に張り付き実弾で致命的ドローダウンの恐れ。<br>**Layer1（核心）**: `src/ml/manji_calibration.py` に `EV_SANITY_CAP=2.0` と `_apply_ev_sanity_cap()` を追加し `P ≤ EV_SANITY_CAP/odds` で **EV を市場相対に頭打ち**。較正器・フォールバック両経路に適用し、卍単勝と Pure_EV_Edge の全消費側を一括保護。<br>**Layer2（足切り）**: `src/ml/pure_ev_edge.py` に `MAX_LIVE_WIN_ODDS=50.0`（`PureEVConfig.max_win_odds`）を追加し、実弾単勝の非現実的大穴（>50倍）を棄却。<br>**設計**: いずれも**推論時のハードリミット／スムージング**で再学習不要。 |
| **影響範囲** | `VERSION`(1.1.0→1.1.1), `src/ml/manji_calibration.py`, `src/ml/pure_ev_edge.py`, `tests/test_ev_calibration_safety.py`(新規・7件), `docs/7_weakness_ledger.md`(W-066), `docs/1_prediction_logic.md`, `docs/spec/ARCHITECTURE_v1.0.0.md` |
| **検証** | 実較正で odds=49.7 の EV **7.2→2.0** 頭打ち・odds=3.0/8.0 の人気馬は EV 不変を実証。`tests/test_ev_calibration_safety.py` 7件＋影響テスト(pure_ev_edge/grandslam_edgecases/maint_20260531/calibration)全PASS。全スイート回帰。mypy 0・ruff クリーン。DB操作なし。 |
| **ロールバック** | 本コミットを `git revert`。VERSION を 1.1.0 へ戻す。定数 `EV_SANITY_CAP` / `MAX_LIVE_WIN_ODDS` の調整でも挙動変更可。 |
| **関連** | W-066 / W-064（dry-run で本歪みを発見）/ 条項6・7 / `feedback_ev_precision_safety_first`（EV精度最重視） |

### 2026-06-01 — 生成件数監視アラート(W-064)とx_scraperバッチ統合(W-065)

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-01 |
| **バージョン** | `1.0.0` → **`1.1.0`**（MINOR・後方互換な機能追加＝監視アラート＋バッチ統合。※指示文の「1.0.1」は PATCH 位置のため、条項6 のSemVer規約に従い feat=MINOR=1.1.0 を採用） |
| **種別** | 機能追加 / 運用基盤（予防監視） |
| **実施内容** | **W-064 予防監視**: `src/ops/health_reporter.py` に実弾モデル別(本命/卍/Alpha-Payout/Pure_EV_Edge)の直前予想**生成件数(distinct race)**集計を追加（`bet_policy.base_model` で suffix/V2 を剥離、`mode=ro` 同等の読み取りのみ）。開催日に生成0件の実弾モデルがあれば `HealthReport.zero_live_models` に載せ **severity を warn へ昇格**＋Discord #system Embed フィールド＋WARNログ。非開催日は誤検知防止で空。V1/V2併存の二重計上を base別 distinct 集合で回避。<br>**W-065 バッチ統合**: `scripts/today_auto_runner.py` に `_run_x_scraper(date)` を subprocess 実装（`py -m src.scraper.x_scraper --date ISO`・30分timeout・stdout `saved=N` パース）。**金曜夜バッチの JVLink同期直後・暫定予想前**に土日両日分、**土曜夜バッチ**に日曜分を収集起動。フェイルセーフ＝収集0件/失敗時は `x_consensus_score` を無言0埋めせず Discord #system へ明示アラート＋WARNログ（`X_SCRAPER_DISABLED=1` で一時無効化可）。学習済モデルの入力次元を壊す「列ドロップ」案は不採用、明示通知方式を採用。 |
| **影響範囲** | `VERSION`(1.0.0→1.1.0), `src/ops/health_reporter.py`, `scripts/today_auto_runner.py`, `tests/test_health_reporter.py`(+4件), `docs/7_weakness_ledger.md`(W-064/W-065 🔴→🟡), `docs/2_automation_schedule.md`, `docs/spec/ARCHITECTURE_v1.0.0.md`, `docs/SYSTEM_ARCHITECTURE.md` |
| **検証** | `pytest tests/test_health_reporter.py` 7件PASS / 関連(`test_w057_shadow_ab`/`test_pure_ev_wiring`)含め17件PASS / 全スイート回帰確認。mypy `health_reporter.py` 0エラー、ruff クリーン、import 健全性OK。DB操作は読み取りのみ（条項1/4遵守）。 |
| **ロールバック** | 本コミットを `git revert`。VERSION は 1.0.0 へ戻す。 |
| **関連** | W-064 / W-065（実生成・実収集は次開催 土6/06 で実証＝それまで🟡）/ W-057（A/B 母数）/ W-058（日次ヘルス基盤）/ 条項6・7 |

### 2026-06-01 — サイレント障害の発見と調査開始（W-064 / W-065 起票）

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-01 |
| **バージョン** | 据え置き `1.0.0`（ドキュメント・調査のみ。コード修正なし） |
| **種別** | バグ調査 / ドキュメント |
| **実施内容** | システム棚卸し中に発見した2件の「サイレント障害」（台帳上は🟢完了だがライブでは無稼働）を正式起票し、実コードベース＋ライブDB実測で根本原因を診断。<br>**W-064 Pure_EV_Edge 生成0件**: 配線・選定ロジックは健全（dry-runで実オッズ18頭→単勝2点 EV7.2/6.6 を生成・較正器ロードOK）。EV閾値1.15も障害原因でない（卍直前単勝の84.6%が1.15超）。真因は**実行機会ゼロ＝配線が週末レース後の6/01 00:31（コミット`800aa23f`）に投入され、以降レース非開催日が続いただけ**。prerace は subprocess 起動のため常駐プロセスのコード陳腐化も無し。初回実稼働は次開催(土6/06)。<br>**W-065 x_signals 0件**: Phase A/B/C実装済だが `x_scraper.py` が **scheduler/autopilot のどこにも未登録**（grep ヒット0件）。単独CLIツールのまま自動トリガー配線が欠落し、`x_consensus_score` が常時0埋めのデッドフィーチャー化。 |
| **影響範囲** | `docs/7_weakness_ledger.md`（W-064/W-065 起票）, `docs/maintenance/MAINTENANCE_LOG.md`（本エントリ）。**コード変更なし**（調査・記録のみ）。 |
| **検証** | ライブDB読み取りのみ（書き込みゼロ・条項1/条項4遵守）。dry-run は `select_pure_ev_bets` を DB非書込みで実行。EV分布・生成件数はライブ実測値。 |
| **ロールバック** | ドキュメントのみ。本コミットを `git revert` で復旧可。 |
| **関連** | W-064 / W-065（要対応・優先度高）/ W-057（A/Bがn=0固定の遠因）/ W-060（過去の配線バグ修正）/ 条項7（仕様書追従） |

### 2026-06-01 — フェーズA: 自己診断・敗因分析エンジンの導入とオートパイロット組み込み

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-01 |
| **バージョン** | 据え置き `1.0.0`（初版リリースにフェーズAを内包。以後の機能追加は条項6に従い MINOR 繰り上げ） |
| **種別** | 機能追加 |
| **実施内容** | ・`src/analysis/post_race_analyzer.py` を新設。`extract_missed_races()`＝**EV≥1.0 で勝負したが的中しなかった**レースを抽出（予想本命馬の着順/オッズ/人気＋実勝ち馬＋予想根拠notes・`is_superseded`除外）。<br>・`build_analysis_prompt()`/`analyze_losses()`＝オッズ・人気・結果・根拠を整形し **Claude API（`claude-opus-4-8` + adaptive thinking）** へ問い合わせ「敗因の3〜5パターン分類＋改善提言」を言語化（クライアント注入可・対象0件はAPI未呼び出し）。<br>・`post_analysis_to_discord()`＝`src/notification/discord_notifier.DiscordNotifier`（ch=敗因分析）経由で自動投稿。<br>・`run_post_race_analysis()`オーケストレータ＋CLI（`py -m src.analysis.post_race_analyzer --since/--ev/--limit/--dry-run`）。<br>・**週次ジョブ組み込み**: `today_auto_runner.py` の日曜・週次レポート直後に `_kick_post_race_analysis()` を追加。**非同期 daemon スレッド＋例外内包（best-effort）** で起動し、既存の週次サイクルを一切巻き添えにしない。<br>・**非干渉設計**: DB は `get_connection()` の **読み取り専用(mode=ro)** のみ。新規モジュール追加で稼働中 autopilot/watchdog/予想生成に非干渉。 |
| **影響範囲** | `src/analysis/post_race_analyzer.py`（新規）, `src/analysis/__init__.py`（新規）, `tests/test_post_race_analyzer.py`（新規）, `tests/test_post_race_integration.py`（新規）, `scripts/today_auto_runner.py`（週次直後フック追加・`import threading`）, `docs/1_prediction_logic.md`, `docs/spec/ARCHITECTURE_v1.0.0.md`（全体図/モジュールマップ/ジョブ表/更新履歴） |
| **検証** | `pytest` 全 1049 PASS（敗因分析8＝commit e4938bc3 で算入済 ＋ 組み込み6を本コミットで追加）。mypy/ruff クリーン。本番DBに対する **read-only スモーク**で EV≥1.0 不的中 5 件の抽出を確認（実 Claude API・実 Webhook には非接続でテスト）。 |
| **ロールバック** | 分析エンジン本体は commit `e4938bc3`、本組み込み・ドキュメントは本コミット。各 `git revert` で復旧可（新規ファイルは削除でも可）。 |
| **関連** | `docs/spec/ARCHITECTURE_v1.0.0.md`（§2/§7/§8）/ フェーズA / 運用条項3・条項7（仕様書追従） |

### 2026-06-01 — ドキュメント整備・バージョン運用基盤の導入（OSS 水準化）

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-01 |
| **バージョン** | （新規）→ `1.0.0`（`VERSION` ファイル初版作成） |
| **種別** | ドキュメント / 運用基盤 |
| **実施内容** | ・ドキュメント階層を `docs/manual/`（取扱説明書）・`docs/maintenance/`（保守報告書）・`docs/spec/`（仕様書）の 3 階層に最適化。<br>・リポジトリルートに `VERSION`（初期値 `1.0.0`）を新設。<br>・バージョン付き仕様書 `docs/spec/ARCHITECTURE_v1.0.0.md` を `docs/SYSTEM_ARCHITECTURE.md` を正典として作成し、Mermaid 全体図・コンポーネント図を埋め込み。<br>・本保守報告書 `MAINTENANCE_LOG.md` を雛形付きで新設。<br>・`CLAUDE.md` に「バージョン運用フロー（コミット必須3点セット）」と「仕様書追従ポリシー」を追記。<br>・ルート `README.md` を OSS 標準（バッジ・目次・バージョン・本番実態同期・コントリビュート方針）へ刷新。 |
| **影響範囲** | `VERSION`（新規）, `README.md`, `CLAUDE.md`, `docs/manual/*`（新規）, `docs/maintenance/MAINTENANCE_LOG.md`（新規）, `docs/spec/ARCHITECTURE_v1.0.0.md`（新規）, `docs/spec/README.md`（新規） |
| **検証** | ドキュメントのみの変更。Mermaid 記法の構文・相対リンクの整合を確認。本番挙動・DB スキーマ・モデルへの変更なし。 |
| **ロールバック** | 本コミット直前の HEAD へ `git revert`。新規ファイルのため削除でも復旧可。 |
| **関連** | `docs/spec/ARCHITECTURE_v1.0.0.md` / 本番運用条項（CLAUDE.md 条項3・条項5） |
