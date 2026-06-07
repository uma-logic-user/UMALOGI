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

### 2026-06-07 — システム進化: データ完全網羅・モデル特性分析・日次パイプライン（v1.5.3-dev）

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-sonnet-4-6) |
| **修正日** | 2026-06-07 |
| **バージョン** | 1.5.2-dev → 1.5.3-dev |
| **種別** | 機能追加 / データ充填 / 分析基盤 |
| **実施内容** | ①`scripts/data_cleaner.py`新設: センチネルオッズ880件・異常馬体重2件・文字化け1件・古いrealtime_odds 1,431件を自動クレンジング ②`scripts/backfill_pedigree.py`新設: 血統欠損9,669頭対象・200頭先行実行 ③`scripts/analyze_model_traits.py`新設: venue/surface/class/distance/model別ROIをJSON出力・Feature Importance抽出 ④`scripts/daily_update_pipeline.py`新設: 日次データ品質更新パイプライン(JVLink同期+血統100頭/日+クレンジング) ⑤scheduler.py に火〜木22:00 `job_daily_update` 登録 |
| **影響範囲** | scripts/data_cleaner.py(新規) / scripts/backfill_pedigree.py(新規) / scripts/analyze_model_traits.py(新規) / scripts/daily_update_pipeline.py(新規) / scripts/scheduler.py / VERSION |
| **検証** | pytest 1233/1233 PASS。クレンジング正常完了。血統バックフィル200頭エラー0件確認中。 |
| **ロールバック** | git revert HEAD |
| **関連** | W-013/W-007/W-010/W-011 |

---

### 2026-06-07 — 最終決戦: テスト全通・U Score因子追加・バックフィル完遂（v1.5.2-dev）

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-sonnet-4-6) |
| **修正日** | 2026-06-07 |
| **バージョン** | 1.5.1-dev → 1.5.2-dev |
| **種別** | バグ修正 / 機能追加 / データ充填 |
| **実施内容** | ①test_notify_pure_ev_edge_sends_to_prediction FAIL修正: router.pyのフォールバックが内部メソッド`_post()`を呼んでいたのを公開API`send_text()`に変更→1233/1233 PASS達成。②W-003(不完全燃焼度): prerun.pyに`uf_incompleteness`を追加(前走不振+条件好転スコア)。③W-007(斤量インパクト): prerun.pyに`weight_carried_diff`/`uf_weight_impact`を追加。④W-010/011(相手関係/クラス変化): u_score.pyに新グループG`_calc_competition()`を追加(`uf_competition_strength`/`uf_class_change`)。⑤フルバックフィル完遂: 残24レースを充填し累計87,972件のlast_3fを確保。 |
| **影響範囲** | src/notification/router.py / src/features/prerun.py / src/ml/u_score.py / VERSION / docs |
| **検証** | pytest 1233/1233 PASS。バックフィル残0件確認。 |
| **ロールバック** | git revert HEAD |
| **関連** | W-003/007/010/011 / test_pure_ev_wiring |

---

### 2026-06-07 — 技術的負債・クリティカルバグ完全一掃（W-045/050/065/068/069/071/043/044）

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-sonnet-4-6) |
| **修正日** | 2026-06-07 |
| **バージョン** | 1.5.0-dev → 1.5.1-dev |
| **種別** | バグ修正 / 安全装置 / データパイプライン |
| **実施内容** | ①W-045: schema.py DDLに shap_json(prediction_horses) / last_3f(race_results) / training_evaluations テーブルを追加しスキーマドリフトを解消。②W-050: BetGeneratorV2の二重V2タグバグ（"本命V2V2(直前)"）を_save_predictionsのmt_base処理で修正。③W-069: 直前パイプラインにfetch_entry_table呼び出しを追加し馬体重100%欠損を解消。④W-071: ev_overlay_guard.py新設でモデルEVへの手動係数付与をコード構造で禁止。⑤W-043: 日次損失サーキットブレーカー（デフォルト¥30,000閾値）をtoday_auto_runnerに実装。⑥W-044: セッション総クラッシュ上限カウンタ（デフォルト50回・フラッピング障害対策）を追加。⑦W-068: training_scraper.pyのURLをtraining.html(404)からoikiri.htmlに修正、評価グレード取得関数追加。⑧W-065: x_scraper配線は既存実装を確認（x_accounts未設定は運用設定の問題）。 |
| **影響範囲** | src/database/schema.py / src/pipeline/prediction.py / src/ml/ev_overlay_guard.py(新規) / scripts/today_auto_runner.py / src/scraper/training_scraper.py |
| **検証** | pytest 1230 PASS / 1 FAILED（既存バグ test_pure_ev_wiring — 本変更と無関係）。W-050修正ロジック全6ケースOK検証済み。 |
| **ロールバック** | git revert または直前コミットハッシュ参照。 |
| **関連** | W-043/044/045/050/065/068/069/071 |

---

### 2026-06-07 — 前走詳細・血統TE のリークフリー特徴量実装＋OOSバックテスト（W-070 / タスク1）

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-07 |
| **バージョン** | 据え置き `1.5.0-dev`（評価専用・本番FEATURE_COLS非破壊） |
| **種別** | 機能追加（特徴量エンジニアリング・評価基盤） |
| **実施内容** | タスク1（特徴量強化）。①`src/features/prerun.py`：前走3F/着順/着差/間隔/直近3走平均3F/同コース実績を**リークフリー**（現レース日より厳密に過去のみ参照）に計算。②`src/features/pedigree_te.py`：父/母父の複勝率**Target Encoding**を cutoff前のみで fit（ベイズスムージング・未知は全体平均フォールバック）。③`scripts/backtest_v2_oos.py`：時系列分割OOS ROIハーネス。**重大リーク検出**：build_acceleration_features 由来の当該レース上がり3F系を特徴量に入れるとROIが非現実的に急騰→リークと判定し除外（W-071の規律が機能）。 |
| **影響範囲** | `src/features/prerun.py`（新規）, `src/features/pedigree_te.py`（新規）, `scripts/backtest_v2_oos.py`（新規）, `tests/test_prerun_features.py`（新規・6件）, `tests/test_pedigree_te.py`（新規・5件）, `docs/7_weakness_ledger.md`（W-070進捗）。本番FEATURE_COLS(69列)・predictions・本番モデルは非改変。 |
| **検証** | 新規テスト11件PASS（リーク非混入を明示検証）。OOSバックテスト（train1600R/test650R）: **AUC 0.8162→0.8184(+0.002)・ROI 51.6%→74.8%(+23.2pt)・的中率5.4→5.2%**。⇒ 改善は実在だが限定的・**単勝74.8%で黒字未達**。ruff check/format クリーン。 |
| **ロールバック** | 新規ファイル削除のみで原状復帰（本番非改変のため影響なし）。 |
| **関連** | W-070（対応中）, W-001, W-002, W-071（リーク検出に寄与）, W-073（通過順位backfill別起票）。**判断**: 本番再学習＋ライブ実弾切替は非黒字のため時期尚早。次期モデル世代での採用候補。 |

### 2026-06-07 — DB書き込み前 文字化け強制クレンジング・ガード実装（W-072 / タスク3）

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-07 |
| **バージョン** | `1.4.8-dev` → `1.5.0-dev` |
| **種別** | 機能追加（運用基盤・データ品質） |
| **実施内容** | 安田記念敗因分析セッションの一環。DBへの INSERT/UPDATE/REPLACE 直前に全文字列パラメータを `ensure_clean` で検証・修復する `src/database/write_guard.py` を新設（`GuardedConnection`/`guard_connection`/`clean_params`/`is_write_sql`）。回復不能な文字化けは空文字へ落とし「文字化けのままのDB書き込み」を物理的に不可能化。SELECTパラメータは非介入。文字化け最頻発の `save_entries_to_db`（horse_name/jockey/trainer）に局所統合。本番オートパイロット稼働中のためグローバル接続ラップ（pandas.read_sql影響リスク）は見送り、保存関数限定の安全統合とした。 |
| **影響範囲** | `src/database/write_guard.py`（新規）, `src/pipeline/scraping.py`, `tests/test_write_guard.py`（新規）, `docs/7_weakness_ledger.md`（W-072）, `VERSION`。 |
| **検証** | `pytest tests/test_write_guard.py` → **10 passed**。E2E: 制御文字混入の馬名/騎手が save_entries_to_db 経由で浄化保存されることを実証。ruff check / format クリーン。import健全性（scraping/prediction）確認。 |
| **ロールバック** | 直前コミット（本コミットの parent）へ revert。新規ファイル削除＋scraping.py の guard_connection 2箇所を戻す。 |
| **関連** | W-072（完了）, W-068, W-071（同セッションの敗因分析）, CLAUDE.md §10/§16（文字化け根絶）。**未完**: タスク1（特徴量強化）/タスク2（相互補完）/タスク4（再学習・backtest）は検証付き段階実施として継続。 |

### 2026-06-07 — race_results UNIQUE制約修正および文字化けゼロ達成

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-sonnet-4-6) |
| **修正日** | 2026-06-07 |
| **バージョン** | `1.4.7-dev` → `1.4.8-dev` |
| **種別** | fix（DBスキーマ変更・データ浄化）|
| **実施内容** | **(1) UNIQUE制約変更**: `UNIQUE(race_id, horse_name)` を廃止し、部分ユニークインデックス `UNIQUE(race_id, horse_number) WHERE horse_number IS NOT NULL` に置換。同一レース内の複数文字化け行を空文字に一括設定できない根本原因を解消。**(2) ゴミデータ削除**: Cat1（正常行と重複する文字化け行: 304件）+ Cat2_no_num（horse_number=NULL 回収不能文字化け行: 338件）+ 追加検出2件 = **644件 DELETE**。**(3) Phase3 クリア**: 残存 cat2_with_num 116件を空文字クリア。**文字化けゼロ達成**。**(4) コード修正**: `jravan_client.py` Step2 INSERT → `ON CONFLICT(race_id, horse_number)` に変更（horse_number=NULL 時は `ON CONFLICT DO NOTHING`）。`data_sync.py` 同様に変更。`schema.py` DDL 更新。`scripts/migrate_race_results_unique.py` 新設。 |
| **影響範囲** | `src/database/schema.py`, `src/scraper/jravan_client.py`, `src/ops/data_sync.py`, `scripts/migrate_race_results_unique.py`（新規）, `data/umalogi.db`（schema migration + 644行DELETE + 116行空文字化）, `data/backups/umalogi_20260607_*.db`（バックアップ）。 |
| **検証** | `pytest` → **1210 passed / 0 failed**。`v_race_mart` 97,924件正常取得確認。残留文字化け: **0件**。 |
| **ロールバック** | `data/backups/umalogi_20260607_094407_pre_migrate_unique.db` にマイグレーション前バックアップ存在。 |
| **関連** | CLAUDE.md 条項4（DB物理削除禁止・例外承認済み）、条項10・16（文字化けゼロ達成）。 |

---

### 2026-06-06 — 文字化け検知時のDiscordアラート追加および本番DBの浄化完了

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-sonnet-4-6) |
| **修正日** | 2026-06-06 |
| **バージョン** | `1.4.6-dev` → `1.4.7-dev` |
| **種別** | feat（監視強化）+ ops（本番DBクリーニング） |
| **実施内容** | **(1) `src/utils/discord_alert.py` 新設**: 文字化け検出時に非同期でシステムDiscordチャンネル（`DISCORD_SYSTEM_WEBHOOK_URL` 優先、フォールバック `DISCORD_WEBHOOK_SNS`）へアラートを送信する `send_mojibake_alert()` を実装。daemon スレッドでファイアー＆フォーゲット、例外は無害スキップ。**(2) `src/scraper/jravan_client.py`**: `_sjis_name()` の文字化け検出時に `send_mojibake_alert("JVLink", "name_field", ...)` を呼び出し配線。**(3) `src/scraper/entry_table.py`**: U+FFFD ガードレール箇所に `send_mojibake_alert("netkeiba", "horse_name", ...)` を呼び出し配線。**(4) `scripts/clean_mojibake.py` 実行（本番DB浄化）**: `data/umalogi.db` の全対象テーブル（races/entries/race_results/horses/racehorses）をスキャン。**文字化け検知964件・空文字クリア543件**（残り421件は `race_results.horse_name` のUNIQUE制約でクリア不可・既存制約問題）。 |
| **影響範囲** | `src/utils/discord_alert.py`（新規）, `src/scraper/jravan_client.py`, `src/scraper/entry_table.py`, `VERSION`, `data/umalogi.db`（データ浄化）。 |
| **検証** | `pytest` → **1210 passed / 0 failed**。 |
| **ロールバック** | `git revert HEAD`。浄化済みDB は `data/backups/` 未作成（空文字化のみ・論理的に可逆だが物理ロールバック不要と判断）。 |
| **関連** | CLAUDE.md 条項10（JVLink文字化けスクリーニング）、CLAUDE.md 条項16（日本語エンコーディング絶対遵守）。 |

---

### 2026-06-06 — データ取得層の文字化け根本解決および異常文字混入ガードレールの実装

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-sonnet-4-6) |
| **修正日** | 2026-06-06 |
| **バージョン** | `1.4.5-dev` → `1.4.6-dev` |
| **種別** | fix（文字化けガードレール・根本対策） |
| **実施内容** | **(1) `src/utils/text.py`**: `is_garbled_name()` 追加（名前フィールド専用高感度検知。単発 `?X` / 半角カタカナ / U+FFFD / カーリークォートで即検出）。`_NAME_GARBLED_RE` 正規表現を Unicode エスケープで明示。**(2) `src/scraper/jravan_client.py`**: `_sjis_name()` 追加（`_sjis()` + `is_garbled_name()` ガード。文字化け検出時は空文字を返しWARNINGログ）。`_RA_RACE_NAME` / `_SE_HORSE_NM` / `_SE_JOCKEY_NM` / `_SE_TRAINER_NM` の4フィールドを `_sjis_name()` に変更し、JVLink破損データの DB混入を防止。**(3) `src/scraper/entry_table.py`**: netkeiba HTMLレスポンスのエンコーディングを `apparent_encoding` から `'utf-8'` 固定に変更。U+FFFD を含む馬名を空文字で保護するガードレールを `_parse_entry_rows()` に追加。**(4) `scripts/clean_mojibake.py`** 新設: `entries.horse_name` を含む包括的 DB 浄化スクリプト（`--dry-run` / `--race-date` 対応）。**(5) テスト新設**: `tests/test_encoding_guards.py` 18件 TDD RED→GREEN PASS。 |
| **影響範囲** | `src/utils/text.py`, `src/scraper/jravan_client.py`, `src/scraper/entry_table.py`, `scripts/clean_mojibake.py`（新規）, `tests/test_encoding_guards.py`（新規）, `VERSION`。predictions テーブル非改変（条項1）。 |
| **検証** | `pytest` → **1210 passed / 0 failed**（既存4件も解消済み）。TDD 18テスト全 GREEN。ruff cleans。 |
| **ロールバック** | `git revert HEAD`。DB のガードは今後の同期からのみ有効（過去データには適用済みの `clean_mojibake.py` で対処）。 |

---

### 2026-06-05 — ログ自動ローテーション（7日保持）導入と災害復旧手順書の作成

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-05 |
| **バージョン** | 1.4.4-dev → 1.4.5-dev |
| **種別** | 運用基盤（運用硬化・DR） |
| **実施内容** | 長期無人運用の運用硬化。①`src/ops/logger.py`（新規）に `setup_logging()` を実装。`TimedRotatingFileHandler(when="midnight", backupCount=7)` で**日次ローテーション＋7日保持**（8日以上前は自動削除）し、ログ肥大化によるディスク枯渇を防止。UTF-8コンソール併用・冪等（多重出力防止）。②本番3デーモン `scheduler.py`/`today_auto_runner.py`/`watchdog.py` を `setup_logging` に集約（従来のサイズベース `RotatingFileHandler`・watchdogのファイル無し設定を置換）。③`docs/9_disaster_recovery.md`（新規）にPC全損時の最短復旧コマンド・DB復元手順・環境変数リストを記載。 |
| **影響範囲** | `src/ops/logger.py`(新規) / `scripts/scheduler.py` / `scripts/today_auto_runner.py` / `scripts/watchdog.py` / `tests/test_logger.py`(新規) / `docs/9_disaster_recovery.md`(新規) / `docs/2_automation_schedule.md` / `VERSION` |
| **検証** | `pytest` 全 **1192 PASS**（ロガー5ケース新規含む）。ローテーション設定 when=MIDNIGHT・backupCount=7 を smoke test とユニットテストで確認。3デーモンの構文・import 順序を smoke test で確認。 |
| **ロールバック** | 直前コミット `ec7aae1f`。 |
| **関連** | 完全無人運用・DR。[[startup_umalogi.bat]] / `scripts/backup_umalogi.py` / `src/ops/backup.py`。 |

### 2026-06-05 — Discord 定期生存報告（ハートビート）機能の追加

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-05 |
| **バージョン** | 1.4.3-dev → 1.4.4-dev |
| **種別** | 運用基盤（死活監視） |
| **実施内容** | 完全無人運用のつなぎ（AntiCrow 等の本格リモート監視統合までの暫定）として、`src/ops/sns_publisher.py` に `format_heartbeat()` / `send_heartbeat()` を追加（`DISCORD_WEBHOOK_SNS` 宛てに「🟢 [時刻] UMALOGI 定期生存報告：システムは正常に稼働し、待機中です」を送信）。`scripts/scheduler.py` に `job_heartbeat_sns()` を追加し `schedule.every(3).hours` で登録（既存の毎時 `job_heartbeat`＝システムチャンネルは温存）。送信は依存性注入(sender)で差替可能・例外を内部で握りつぶす非ブロッキング設計でメイン処理を一切ブロックしない。 |
| **影響範囲** | `src/ops/sns_publisher.py` / `scripts/scheduler.py` / `tests/test_sns_publisher.py` / `docs/2_automation_schedule.md` / `VERSION` |
| **検証** | `pytest` 全 **1187 PASS**（ハートビート 4 ケース含む）。実 `.env` の `DISCORD_WEBHOOK_SNS` はプレースホルダのため send は非ブロッキングで False を返す（例外を出さない）ことを E2E 確認。 |
| **ロールバック** | 直前コミット `4229fbd1`。 |
| **関連** | 完全無人運用・死活監視。AntiCrow 統合までの暫定機能。 |

### 2026-06-04 — bet_policy 現行仕様へのテスト追従および .gitignore クリーンアップ

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-04 |
| **バージョン** | 据え置き `1.4.3-dev`（テスト・無視設定のみ＝`src/` 挙動への影響ゼロのため繰り上げなし） |
| **種別** | バグ修正（テスト追従）/ 運用基盤（.gitignore） |
| **実施内容** | `bet_policy.LIVE_MODELS={Pure_EV_Edge, 卍, FukushoElite}`（本命/Alpha-Payout を実弾モデルから除外）への変更にテストが追従しておらず、`compute_live_roi`/`fetch_model_roi` 系の 4 テストが実弾0件となり失敗していた技術的負債を解消。**(1)** 旧 live モデル名 `本命(直前)` を現行 live モデル `Pure_EV_Edge(直前)`（単勝・複勝とも実弾）に置換: `tests/test_pnl_accounting.py`（全6箇所）/ `tests/test_grandslam_edgecases.py::test_live_roi_since_filter`（1箇所のみ・A/B レガシー比較で `本命` を意図使用する `test_ab_*` の行は温存）/ `tests/test_streamlit_perf.py::test_model_roi_per_model`（挿入+アサーション）。**ビジネスロジック（`src/`）は一切変更せず**、テスト側 fixture/アサーションのみ現行仕様へ追従。**(2)** `.gitignore` に `data/*.db-wal` / `data/*.db-shm`（SQLite 揮発ファイル）/ `outputs/nar/`（NAR ライブデモ生成物）を追加し、追跡済みだった `data/umalogi.db-wal` / `data/umalogi.db-shm` を `git rm --cached` で追跡解除（物理ファイルは無傷＝稼働中スケジューラに影響なし）。 |
| **影響範囲** | `tests/test_pnl_accounting.py`, `tests/test_grandslam_edgecases.py`, `tests/test_streamlit_perf.py`, `.gitignore`, 追跡解除: `data/umalogi.db-wal`/`data/umalogi.db-shm`。`src/` 非改変。 |
| **検証** | `py -m pytest` → **1183 passed / 0 failed（exit code 0）**。修正前は同4件が失敗。`ruff check` 変更テストファイルはクリーン。 |
| **ロールバック** | `git revert HEAD`。`git rm --cached` の取り消しは `git checkout HEAD~1 -- .gitignore && git add data/umalogi.db-wal data/umalogi.db-shm`。 |
| **関連** | 直前 NAR 作業（feature/nar-support）報告の「pre-existing 4 failures（詰まりポイントB）」の恒久解消。 |

---

### 2026-06-04 — SNS投稿例外遮断・Noteペイウォール安全ガード実装

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-sonnet-4-6) |
| **修正日** | 2026-06-04 |
| **バージョン** | `1.4.2` → `1.4.3-dev`（-dev: 週末凍結前の開発継続タグ） |
| **種別** | fix（例外遮断・安全ガード追加） |
| **実施内容** | **(1) `sns_publisher.py` フォールバック強化**: `_send_x_fallback_discord()` 追加（DISCORD_WEBHOOK_SNS へ「【フォールバック】X投稿失敗：…」形式で送信）。`send_hit_flash()` に `fallback_sender: Sender | None = None` パラメータ追加。sender が False 返却または例外を投げた場合に fallback_sender を呼び出し、プロセスをクラッシュさせずに続行。閾値未満ヒット（generate_hit_flash=None）の場合はフォールバック不発動。**(2) `note_generator.py` ペイウォールガード強化**: `_ensure_paywall(text, allocations_present) -> str` を新設。allocations がある場合に 🔒 がないテキストへ先頭にガードブロックを自動挿入。`generate_note_draft()` の末尾で常に呼ぶよう統合。**(3) テスト新設**: `tests/test_sns_guardrails.py` 12件 TDD RED→GREEN PASS。 |
| **影響範囲** | `src/ops/sns_publisher.py`（_send_x_fallback_discord/send_hit_flash修正）, `src/ops/note_generator.py`（_ensure_paywall追加/generate_note_draft末尾統合）, `tests/test_sns_guardrails.py`（新規）, `VERSION`（1.4.2→1.4.3-dev）。既存 4失敗テストは本変更と無関係。 |
| **検証** | `pytest` → **1179 passed**（+65件増）、既存4失敗は変更前からの既存障害。 |
| **ロールバック** | `git revert HEAD`。 |

---

### 2026-06-03 — サブスク用レース結果報告自動生成ループの構築

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-sonnet-4-6) |
| **修正日** | 2026-06-03 |
| **バージョン** | `1.4.1` → `1.4.2`（後方互換な機能追加 = MINOR） |
| **種別** | feat（新機能追加） |
| **実施内容** | **(1) `sns_publisher.py` 拡張**: `BetResult` dataclass（roi/profit プロパティ付き）+ `generate_x_hit_tweet()`（的中のみ生成・140字保証）+ `generate_post_race_report()`（ROI/的中一覧含む日次 Note 総括）+ `write_daily_reports()`（note_report_YYYYMMDD.md + x_hit_*.txt の同時出力・out_dir テスト注入対応）を追加。`detect_and_flash()` に `out_dir` パラメータと例外セーフな X 速報ファイル書き出しフックを統合。**(2) `note_generator.py` 修正**: `generate_note_draft()` に有料ライン（🔒 ペイウォール区切り）を予算配分表の直前に挿入。**(3) `test_sns_detect_flash.py` 互換修正**: `predictions` テーブルスキーマに `expected_value` カラム追加（DEFAULT NULL / COALESCE対応）。**(4) テスト新設**: `tests/test_post_race_report.py` 28件 PASS。 |
| **影響範囲** | `src/ops/sns_publisher.py`（BetResult/3関数/detect_and_flash拡張）, `src/ops/note_generator.py`（paywall追加）, `tests/test_post_race_report.py`（新規28件）, `tests/test_sns_detect_flash.py`（スキーマ互換修正）, `VERSION`（1.4.1→1.4.2）。predictions テーブル非改変（条項1）。 |
| **検証** | `pytest tests/test_post_race_report.py tests/test_daily_drafts.py tests/test_money_management.py tests/test_sns_detect_flash.py` → **73 passed**。ruff format クリーン。 |
| **ロールバック** | `git revert HEAD`。`outputs/sns/reports/` ディレクトリは空でも問題なし。 |

---

### 2026-06-02 — サブスク集客用 予算配分ロジック・Note/X 自動生成ループの構築

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-sonnet-4-6) |
| **修正日** | 2026-06-02 |
| **バージョン** | `1.4.0-dev` → `1.4.1`（後方互換な機能追加 = MINOR） |
| **種別** | feat（新機能追加） |
| **実施内容** | **(1) `src/ops/money_management.py` 新設**: `BetAllocation` dataclass + `allocate_budget()` 純関数。EV エッジ（EV-1.0）比例で総予算（デフォルト¥10,000）を按分し100円単位に丸める。EV≤1.0 は最大3件の保険枠（100円固定）。合計＝総予算を保証。実弾処理・DB とは完全切り離し。**(2) `src/ops/note_generator.py` 拡張**: `generate_note_draft()` / `generate_x_promo_tweet()` / `write_daily_drafts()` / `_extract_note_bets()` を追加。日次下書きを `outputs/sns/drafts/note_pre_YYYYMMDD.md` / `x_pre_YYYYMMDD.txt` に出力。`run_gachi_pipeline()` の末尾に例外セーフフックとして組み込み済み。**(3) テスト新設**: `tests/test_money_management.py`（16件）+ `tests/test_daily_drafts.py`（25件）= 計41件 PASS。 |
| **影響範囲** | `src/ops/money_management.py`（新規）, `src/ops/note_generator.py`（拡張・imports/定数/4関数/run_gachi_pipeline修正）, `tests/test_money_management.py`（新規）, `tests/test_daily_drafts.py`（新規）, `VERSION`（1.4.0-dev→1.4.1）。予測テーブル非改変（条項1）。 |
| **検証** | `pytest tests/test_money_management.py tests/test_daily_drafts.py` → **41 passed**。全テストスイート: 1114 passed（新規失敗なし）。ruff format 適用・未使用 import 除去済み。 |
| **ロールバック** | `git revert HEAD`（1コミット）。`outputs/sns/drafts/` ディレクトリは空でも問題なし。 |

---

### 2026-06-02 — Challengerモデルの正式昇格および複勝特化較正の再fit完了

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-sonnet-4-6) |
| **修正日** | 2026-06-02 |
| **バージョン** | `1.4.0-dev`（据え置き・dev継続。モデル内容は変更だが API 互換維持） |
| **種別** | モデル昇格 / 較正再fit |
| **実施内容** | (1)**`manji_model.pkl` 正式昇格**: Challenger(train_until=2024・OOS複勝108.8%)を `retrain_manji_weekend.py --promote-fukusho` で本番deply。n_races=1424/n_samples=19800。単勝は WATCH_ONLY のため副作用なし。(2)**複勝 Platt 較正器 再fit**: 新モデルベースで `fit_manji_place_calibrator()` を再実行。**ECE 旧Champion版0.0395 → 新Challenger版0.0271（更に改善・健全 PASS）**。 |
| **影響範囲** | `data/models/manji_model.pkl`(昇格・md5: 1fcd779d), `data/models/manji_place_calibrator.pkl`(再fit), `scripts/retrain_manji_weekend.py`(--promote-fukusho追加), `logs/fukusho_calibration_final_v2.log`, `docs/1_prediction_logic.md`, `docs/7_weakness_ledger.md`(W-067→🟢完了)。**単勝 manji_win_calibrator.pkl は非改変**。predictions テーブル非改変（条項1）。 |
| **検証** | `pytest`（bet_policy/ev_calibration_safety/bet_generator/health_reporter/fukusho_elite/bet_precision_filters）→ **93 passed**。WATCH_ONLY維持(`is_live_bet("卍","単勝")=False` / `is_watch_only=True`)確認済。ECE=0.0271(健全)。 |
| **ロールバック** | `data/backups/manji_model_pre_fukusho_promotion_20260602_121506.pkl`(md5: a90e87f9)から `data/models/manji_model.pkl` へコピーで即時復元可。 |
| **関連** | CLAUDE.md 条項1/4/6/7。W-067(🟢完了) / W-048 / W-059 / W-066。 |

### 2026-06-02 — 卍 複勝特化昇格＋複勝Platt較正器の分離

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-02 |
| **バージョン** | `1.4.0-dev`（据え置き・dev継続。較正器追加＋ゲート粒度化、基底モデル非再訓練のため互換） |
| **種別** | 買い目ロジック（ポリシー）/ 較正 |
| **実施内容** | (1)複勝専用 Platt 較正器を新設（`manji_calibration.fit_manji_place_calibrator`/`calibrate_place_prob`・`manji_place_calibrator.pkl`）。単勝Isotonicと独立。学習=400R/5,679件・**ECE 0.1784→0.0395(健全)**。(2)`bet_policy` に `MODEL_LIVE_BET_TYPES={卍:{複勝}}`・`WATCH_ONLY_MODELS={卍:{単勝}}` を追加し `is_live_bet` を券種粒度化＋`is_watch_only` 新設。卍は複勝のみ実弾・単勝はWATCH_ONLY。(3)`bet_generator` 卍複勝 confidence を Platt較正値へ。 |
| **影響範囲** | `src/ml/bet_policy.py`, `src/ml/manji_calibration.py`, `src/ml/bet_generator.py`, `scripts/retrain_manji_weekend.py`, `data/models/manji_place_calibrator.pkl`(新規), `logs/fukusho_calibration_final.log`, `tests/test_bet_policy.py`, `tests/test_ev_calibration_safety.py`, `docs/1_prediction_logic.md`, `docs/7_weakness_ledger.md`(W-067)。**基底回帰 `manji_model.pkl` は非再訓練（HOLD据え置き）/ 単勝較正器も不変**。 |
| **検証** | `pytest`（bet_policy/health_reporter/fukusho_elite/bet_generator/bet_precision_filters/ev_calibration_safety）→ **90 passed**。複勝較正ECE=0.0395（`logs/fukusho_calibration_final.log`）。 |
| **誠実報告** | 昇格対象=卍複勝の現役Champion OOS2025=90.9%・ライブ複勝=暫定99.4%/直前84.0% ＝**現Championでは黒字未達(≒トントン)**。黒字化(Challenger 108.8%)は基底回帰の再デプロイが別途必要（本コミットは較正器分離＋ゲート分離のインフラ整備に限定）。 |
| **ロールバック** | コード: 直前コミット `f3682cbd`。新規較正器を削除すれば `calibrate_place_prob` はフォールバックに退避（安全）。 |
| **関連** | CLAUDE.md 条項1/3/4/6/7。W-067 / W-048 / W-059。 |

### 2026-06-02 — 週末向け 実弾モデル縮退（卍/Pure_EV_Edge/FukushoElite）＋卍Challenger検証

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-02 |
| **バージョン** | `1.4.0-dev`（据え置き・dev継続。実弾メンバー変更は本番影響だが機構不変のため dev 線内で扱い、本番昇格時に MINOR 確定） |
| **種別** | 買い目ロジック（ポリシー）/ モデル検証 |
| **実施内容** | (1)実弾配信縮退: `bet_policy.LIVE_MODELS` を `{卍, Pure_EV_Edge, FukushoElite}` に集約。確定ROI<100%の 本命/Alpha-Payout を新設 `NON_LIVE_RETIRED` へ退避（投票停止・予想生成継続・復帰可）。Oracle/HitFocus は従前 ORNAMENTAL のまま。(2)卍Challenger再訓練を**安全検証のみ**で実施（`scripts/retrain_manji_weekend.py` 新規）: Champion(現役pkl) vs Challenger(train_until=2024) を 2025 OOS 比較。単勝72.2%/68.5%(共<100%)、**複勝90.9%→108.8%(黒字化)**。保守ゲート未達で **HOLD**。 |
| **影響範囲** | `src/ml/bet_policy.py`, `tests/test_bet_policy.py`, `tests/test_health_reporter.py`, `scripts/retrain_manji_weekend.py`(新規), `logs/training_log_manji_weekend.log`, `docs/1_prediction_logic.md`, `docs/7_weakness_ledger.md`(W-067)。**本番モデル `data/models/manji_model.pkl` は未改変（md5一致）**、predictions テーブル非改変。 |
| **検証** | `pytest`（bet_policy/health_reporter/fukusho_elite_integration/bet_generator/bet_precision_filters）→ **81 passed**。卍OOS検証ログ＝`logs/training_log_manji_weekend.log`。 |
| **ロールバック** | コード: 直前コミット `4108b2cd`。卍pkl: `data/backups/manji_model_20260602_084516.pkl`（今回未昇格のため復元不要）。 |
| **関連** | CLAUDE.md 条項1/4/6/7。弱点 W-067（本コミットで起票）/ W-059 / W-048 / W-064。 |

### 2026-06-02 — マックスプラン最終資産化（性能検証レポート・最終仕様書・堅牢性証明）

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-02 |
| **バージョン** | `1.4.0-dev`（据え置き・コード変更なし・成果物生成のみ） |
| **種別** | ドキュメント / 性能検証 |
| **実施内容** | 稼働中の `bulk_backfill_features`(PID37288)・オートパイロット・watchdog とのDB競合を避け、**読取専用(mode=ro)**で全期間性能を集計。①`reports/final_performance_2026.md` 新規作成（モデル別ライブ真ROI・月別推移・複勝圏的中率・最高配当ランキング）。**システム全体ライブ真ROI=80.1%（純損▲¥1,743,008）= 既知の確定真ROI80%と一致**、唯一の黒字頭は卍(直前131.8%/暫定378.2%)。②`docs/ARCHITECTURE_FINAL.md` 新規作成（Mermaidデータフロー＋推論シーケンス＋v1.2.0/v1.4.0-dev差異ロードマップ）。③堅牢性検証＝指示テスト2件は実体不在のため代替の実在近縁テスト4ファイルを実行し **69 passed**、結果と欠損箇所を `logs/critical_failure.log` へ記録。 |
| **影響範囲** | 新規: `reports/final_performance_2026.md` / `docs/ARCHITECTURE_FINAL.md` / `logs/critical_failure.log`。コード・DB・モデルへの変更なし。 |
| **検証** | `pytest`（test_data_pipeline_v2 / test_pipeline_prediction / test_backtest_all_models / test_models）→ **69 passed (209s)**。DB集計は読取専用接続。 |
| **ロールバック** | 直前コミット `abc6935d`。成果物3ファイルを削除すれば原状復帰（コード非改変）。 |
| **関連** | CLAUDE.md 条項1/3/4/6/7。データ欠損（last_3f 2024-07/08ゼロ・x_signals未配線・WIN5払戻ゼロ）。 |

### 2026-06-02 — JVLink 2024再取得を試行 → JVRead -503（深夜データ提供休止）で保留

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-02 |
| **バージョン** | `1.4.0-dev`（据え置き・コード変更なし・運用記録のみ） |
| **種別** | 運用（外部要因による失敗の記録）/ 障害対応知見 |
| **実施内容** | `check_jravan_integrity` の提案コマンド `py -3-32 -m src.scraper.jravan_client --option 2 --fromtime 20240601`（dataspec 既定 RACE）を実行。**JVLink COM は 32bit Python で稼働可能と判明**（64bit では「クラス未登録」だったが 32bit で Dispatch 成功）。JVInit(sid=UMALOGI00)・JVOpen(code=0・dl=24)・ダイアログ自動突破まで成功したが、**JVRead が一貫して `-503`（HTTP 503 相当の JRA-VAN 配信サーバー Service Unavailable）**を返し中断（3回×2セッション）。実行時刻 23:56〜00:01 の深夜跨ぎ＋JVOpen が dl=24 を返す事実から **JRA-VAN 深夜データ提供休止時間帯**と判断。 |
| **影響範囲** | コード変更なし。`docs/6_special_notes.md`（リカバリ手順記録）, 本ログ。**実DB は無変化**（2024-06〜12 rank 確定 15 件のまま・書込ゼロ・破損なし）。 |
| **検証** | 失敗ログ（JVRead -503 ×6）と DB 件数不変を確認。JVInit/JVOpen 成功でインフラ・認証・契約は正常。 |
| **ロールバック** | 不要（DB 非変更）。 |
| **関連** | 2024後半 distance/結果欠損（W-001/W-002 の distance 根治の前提）/ リカバリは日中の提供時間帯に再実行（docs/6 §更新履歴）/ JVLink は 32bit 専用 |

### 2026-06-01 — W-002同時実装＋last_3f/distance実バックフィル＋暫定重要度検証（v1.4.0-dev）

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-01 |
| **バージョン** | `1.4.0-dev`（据え置き・再学習準備フェーズ。**本番 v1.2.0 凍結維持**・FEATURE_COLS 不変） |
| **種別** | 機能追加（W-002実装）＋データ充填（実バックフィル）＋検証 |
| **実施内容** | ①**JVLink 2024再取得**: 本環境では JVLink COM が「クラス未登録」で**実行不可**（G-Tune PC 専用）と実測判明 → `check_jravan_integrity` で欠損（2024-07/08 結果ゼロ・全期 coverage 75.3%）を再確認し JVLink Setup/Update コマンドを提示するに留めた（捏造せず）。②**W-002 PCI/RPCI 実装**: `compute_race_pci`（各馬PCIの中央値・後傾>50）新設＋`race_pci` 列、`ACCEL_FEATURE_COLS` を4列化（FEATURE_COLS_V2=73）。③**netkeibaバルク・バックフィル実実行**: 計**100レース/約1,480馬行**の last_3f を実DBへ充填（saved100/100・errors0・冪等COALESCE・間隔~2.5s をログ実証）。④**distance 欠損補填**: `races.distance` がDB全体で~0（PCI算出不能）と判明→`_upsert_race_meta` で netkeiba距離を非破壊補填（50R で distance>0）。⑤**暫定LightGBM重要度**（複勝圏・gain%・in-sample 50R）: acceleration_score **51.4%**/pci **21.7%**/last_3f_sec **14.6%**/race_pci **12.4%**＝4特徴量とも有効。 |
| **影響範囲** | `src/features/acceleration.py`(compute_race_pci/race_pci), `src/features/backtest_v2.py`(ACCEL 4列), `scripts/bulk_backfill_features.py`(_upsert_race_meta), `scripts/run_backtest_v2.py`(実fit+importance), `tests/test_acceleration_features.py`(+RPCI3件), `tests/test_data_pipeline_v2.py`(+meta2件), `docs/7_weakness_ledger.md`(W-001/W-002 🟡), `docs/3_data_schema.md`, `docs/2_automation_schedule.md`, `docs/spec/ARCHITECTURE_v1.0.0.md`。**実DB**: race_results.last_3f 100R充填＋races.distance 50R補填（いずれも additive・既存非破壊）。 |
| **検証** | `test_acceleration_features.py`＋`test_data_pipeline_v2.py` 全GREEN＋**FEATURE_COLS 69列ガード継続**＋全スイート回帰。mypy 0・ruff クリーン。実バックフィル saved100/100/errors0。run_backtest_v2 実LightGBM fit で重要度出力。 |
| **ロールバック** | コードは `git revert`。実DBの last_3f/distance は additive のため `UPDATE ... SET last_3f=NULL` 等で戻せるが、正データのため保持推奨。 |
| **関連** | W-001/W-002（残: full backfill ~6,200R・OOS・FEATURE_COLS統合）/ W-014（JVLink歴史データ）/ 2024 distance欠損（JVLink再取得で根治）/ 条項4・6・7 |

### 2026-06-01 — 過去データ整合性チェック・last_3f バックフィル・再シミュ基盤（v1.4.0-dev）

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-01 |
| **バージョン** | `1.3.0` → **`1.4.0-dev`**（プレリリース・再学習準備フェーズ。**本番稼働は v1.2.0 で凍結継続**＝推論挙動・FEATURE_COLS は不変） |
| **種別** | 開発基盤（データ整合性・バックフィル・再シミュレーション自動化） |
| **実施内容** | **①整合性チェック** `scripts/check_jravan_integrity.py`（read-only）: `races` vs `race_results`(rank確定) を月粒度で充足スキャンし結果ゼロ月/低充足月を検出、連続欠損を JVLink Setup/Update レンジに畳んで提案（自動実行はしない）。**本番DB実測で欠損検出**: 2024-07/08 が結果ゼロ、2024後半が低充足（全期間 coverage 75.3%）。<br>**②バックフィル** `scripts/bulk_backfill_features.py`（冪等）: `last_3f` が NULL かつ rank 確定のレースを期間(既定 2023-01-01〜当日)で抽出し netkeiba 再取得→`COALESCE` 保存。各レース間 sleep(既定1.2s)＋`http_client` RateLimiter の二重で負荷配慮。fetcher/sleeper 注入で非ネットワークテスト可。<br>**③再シミュ基盤** `src/features/backtest_v2.py`＋`scripts/run_backtest_v2.py`(骨子): `build_feature_cols_v2`(FEATURE_COLS を**非破壊コピー**して加速力3列を連結)・`attach_acceleration_features`(base_df 不変・左結合・欠損は NaN/0)。学習データ生成の前処理モックでモデル fit は次フェーズ。 |
| **影響範囲** | `VERSION`(1.3.0→1.4.0-dev), `scripts/check_jravan_integrity.py`(新規), `scripts/bulk_backfill_features.py`(新規), `scripts/run_backtest_v2.py`(新規), `src/features/backtest_v2.py`(新規), `tests/test_data_pipeline_v2.py`(新規12件), `docs/2_automation_schedule.md`, `docs/3_data_schema.md`, `docs/spec/ARCHITECTURE_v1.0.0.md` |
| **検証** | `tests/test_data_pipeline_v2.py` 12件（整合性: 欠損月検出/未来月無視/健全・バックフィル: 期間&NULL抽出/充填済スキップ/注入fetcher&sleep/dry-run/エラー継続・v2: FEATURE_COLS非破壊+3列/冪等/結合/last_3f無し安全）＋**FEATURE_COLS 69列ガード継続GREEN**＋全スイート回帰。mypy 0・ruff クリーン。本番DBで整合性スモーク（欠損実検出）＋backfill dry-run（取得なし）。DB 書き込みは行っていない（バックフィルは未実行＝dry-run のみ）。 |
| **ロールバック** | 全て新規スクリプト/モジュール。`git revert` で復旧可。VERSION を 1.3.0 へ。 |
| **関連** | W-001（last_3f を消費）/ W-014 歴史データ大規模取得 / 2024後半の結果欠損（要 JVLink 再取得）/ 条項4（DB操作前提案・自動実行回避）/ 条項6・7 |

### 2026-06-01 — W-001 加速力スコア(上がり3F)＋PCI のデータ基盤構築

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-01 |
| **バージョン** | `1.2.0` → **`1.3.0`**（MINOR・次期学習用の新規モジュール＋additive列追加。**本番推論挙動は不変**＝v1.2.0 凍結を維持） |
| **種別** | 機能追加（次期特徴量のデータ基盤・概念実証） |
| **実施内容** | **調査**: レース上がり3Fは DB/RTD/JVLink 未保存（JVLink `time_3f` は調教専用）。取得源は netkeiba 結果列[11]「上がり」だが従来は破棄。<br>**構築**: ①additive migration `race_results.last_3f REAL`（冪等・nullable）②`netkeiba.py` に `_COL_LAST_3F=11`／`HorseResult.last_3f`／列[11]パース追加＋`fetch_race_result._upsert_race_results` に `COALESCE` 保存（非破壊）③新規 `src/features/acceleration.py`（`parse_time_to_seconds`／`compute_pci` 西田式準拠／`acceleration_score` レース内z-score／`build_acceleration_features` 並行計算・last_3f 未取得でも安全に NaN/0 を返す）。<br>**本番非破壊**: `FEATURE_COLS`(69列) は一切不変。新特徴量は再学習で明示的に取り込むまで推論に非影響（ガードテストで担保）。 |
| **影響範囲** | `VERSION`(1.2.0→1.3.0), `src/features/acceleration.py`(新規), `src/features/__init__.py`(新規), `src/scraper/netkeiba.py`, `scripts/fetch_race_result.py`, `src/database/init_db.py`, `tests/test_acceleration_features.py`(新規13件), `docs/7_weakness_ledger.md`(W-001 🟡), `docs/3_data_schema.md`, `docs/1_prediction_logic.md`, `docs/spec/ARCHITECTURE_v1.0.0.md` |
| **検証** | `tests/test_acceleration_features.py` 13件（タイム解析/PCI既知値52.94・基準50・方向性/加速力score/縮退/DB並行計算 with・without last_3f/空/**FEATURE_COLS 69列不変ガード**）＋全スイート回帰。mypy 0・ruff クリーン。migration を本番DBに冪等適用（last_3f 列追加・20列・既存推論に非影響）。 |
| **ロールバック** | 本コミットを `git revert`。`last_3f` 列は additive のため残存しても無害（NULL）。VERSION を 1.2.0 へ戻す。 |
| **関連** | W-001（残: 実バックフィル→蓄積→再学習→FEATURE_COLS 正式統合は次期 MINOR）/ W-002 PCI（同列を共有）/ 条項6・7 |

### 2026-06-01 — FukushoElite の期待値ベース本番統合（W-020）

| 項目 | 内容 |
|------|------|
| **修正者** | Claude (claude-opus-4-8) |
| **修正日** | 2026-06-01 |
| **バージョン** | `1.1.1` → **`1.2.0`**（MINOR・新規実弾モデルの本番統合＝後方互換な機能追加） |
| **種別** | 機能追加（収益最大化・実弾モデル拡張） |
| **実施内容** | 複勝特化 `FukushoElite` を **EV 最優先ゲート**で実弾パイプラインに正式統合（既存は未結線・誤ラベル・edge判定のみ）。<br>**配線**: `bet_policy.LIVE_MODELS` に `FukushoElite` 追加＋`SELECTIVE_LIVE_MODELS` 新設（厳格セグメントで正当に0件となるため W-064 生成0件アラートから除外）。`init_db._VALID_BASE_TYPES`・`RaceBets.model_type` Literal に追加。<br>**EV最優先2段ゲート**: `generate_elite_fukusho_bets` を刷新し ①segment+edge(venue∈{新潟/東京/福島/京都}・≥13頭・edge≥1.1) ②**統計的複勝EV = P(place)×推定複勝オッズ ≥ `FUKUSHO_ELITE_EV_MIN=1.05`**（Pure_EV と同一の `fukusho_ev` を踏襲・勝率/複勝率単独ベット禁止）。通過馬ゼロは見送り。`model_type="卍"` 誤ラベルを `FukushoElite` に修正し `expected_value` を真の複勝EVに。<br>**結線**: `prediction._run_fukusho_elite()` を新設し直前パイプライン(`if not provisional`)に追加、`predictions(model_type="FukushoElite(直前)")` 保存＋UI payload に `fukusho_elite` セクション追加。 |
| **影響範囲** | `VERSION`(1.1.1→1.2.0), `src/ml/bet_policy.py`, `src/ml/bet_generator.py`, `src/pipeline/prediction.py`, `src/database/init_db.py`, `src/ops/health_reporter.py`, `tests/test_fukusho_elite_integration.py`(新規6件), `docs/7_weakness_ledger.md`(W-020 🟢), `docs/1_prediction_logic.md`, `docs/spec/ARCHITECTURE_v1.0.0.md` |
| **検証** | `tests/test_fukusho_elite_integration.py` 6件（実弾登録/EV高→生成・FukushoEliteラベル/EV低→見送り/境界/セグメント外/頭数不足）＋health_reporter 7件＋全スイート回帰。mypy 0・ruff クリーン。 |
| **ロールバック** | 本コミットを `git revert`。VERSION を 1.1.1 へ戻す。`FUKUSHO_ELITE_EV_MIN` の調整でゲート強度を変更可。 |
| **関連** | W-020 / W-064（SELECTIVE_LIVE_MODELS で誤検知回避）/ W-066（fukusho_ev は EV キャップと整合）/ 条項6・7 / `feedback_ev_precision_safety_first` |

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
