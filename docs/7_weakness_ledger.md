# UMALOGI 弱点・技術的負債 管理台帳

> **CLAUDE.md 条項「弱点管理ルール」に基づき、システムの弱点・技術的負債・未実装機能を**
> **一元管理するドキュメント。新規指示を受けた際は必ずこのファイルを冒頭に確認し、**
> **過去の弱点の改善状況を更新してから実装に進むこと。**

---

## 更新履歴

| 日付 | 更新内容 |
|------|---------|
| 2026-06-01 | 【W-063 🟢完了 / 優先度中】DB肥大化・断片化・統計陳腐化に対する**定期保守の不在**を解消（システム要塞化）。長期稼働でSQLiteは削除ページが回収されずファイルが肥大し、ANALYZE未実行でクエリプランナー統計も陳腐化していた（VACUUMは`cleanup_old_data.py`内に存在したが**定期スケジュール未登録**）。新規 `src/ops/db_optimize.optimize_db()` を実装し WAL checkpoint(TRUNCATE)→VACUUM→ANALYZE を autocommit接続で安全実行、scheduler に `job_nightly_maintenance`（毎日04:00・非干渉帯）を登録。事前に既存 `backup.backup_db()` でホットバックアップ（条項4遵守）。**E2E数値実証**: 本番DBコピーで179.9MB→167.2MB(**12.7MB/7%回収**・10秒)。バックアップ機構は既存(backup.py/weekly_backup.py)を再利用し重複実装を回避。残債: VACUUMは瞬間的に排他ロックを取るが04:00はレース非稼働帯のため影響なし。日次バックアップが23:00と04:00事前の2回になるが5世代ローテで自己抑制。テスト `tests/test_db_optimize.py` 8件PASS・mypyクリーン。影響: src/ops/db_optimize.py(新規), scripts/scheduler.py |
| 2026-06-01 | 【W-061 🟢完了 / 優先度中】**リポジトリ全体 mypy エラー完全ゼロ達成**（`Success: no issues found in 98 source files`）。残71件をファイル予算制チャンク（analysis→scraper→ml/pipeline→評価/通知/ops）で段階解消。実バグ修正を複数含む: `u_score` 未定義型名`date_type`→`date`、`update_payouts` 未import`sqlite3`、`alpha_backtest._run_auto_search` の window_data 4要素誤宣言→3要素是正、`cleanup_old_data.isolation_level`。定石: LGBMハイパラ`dict[str,Any]`、`sys.stdout.reconfigure`の`# type: ignore[union-attr]`統一、BeautifulSoup `.get`の`str()`正規化と`Tag|NavigableString`の`union-attr`抑制、`_BaseModel._filename`基底宣言、`tbl:object→Any`。**併せてテスト環境を完全独立化**（W-062）。隔離worktree(grandslam/test-isolation)で実施、全1010テスト空DB環境PASS・origin/master `2ff66140` にFF push。関連: [[W-061]] [[feedback_parallel_session_conflict]]。影響: analysis/scraper/ml/pipeline/evaluation/notification/ops/utils 計21ファイル |
| 2026-06-01 | 【W-062 🟢完了 / 優先度中】テスト環境の完全独立化。実DB(`data/umalogi.db`直接connect)依存・`.env`(DISCORD_WEBHOOK_URL)依存テストが隔離環境(git worktree・空DB/.env無し)で4件failしていた問題を `tests/conftest.py` の autouse fixture で解消。①実DB不在時のみ`init_db()`＋独立migration(add_training_grade)で完全スキーマ空DBを生成し`PRAGMA journal_mode=DELETE`でWAL肥大破損を防止(本番実DBには非干渉)②未設定の Discord webhook 環境変数に無害ダミー注入。全1010テストが隔離環境でもGREEN。影響: tests/conftest.py(新規) |
| 2026-06-01 | 【W-061 🟡対応中(コア層完了・前進) / 優先度中】型安全をコア層へ拡大。グランドスラム総点検で全体 mypy **142→71エラー（50%減）**。**prediction / bet_generator / umanity_uploader / alpha_payout_model / alpha_place_model をエラー0**化。要点: ①`_run_alpha_payout` の裸 `return`→`return None`（戻り型 `RaceBets\|None` の実バグ修正）②`RaceBets.model_type` Literal を実態（卍/本命/HitFocus/**Alpha-Payout/卍V2/本命V2**）へ拡張③三連系 combinations を可変長 `tuple[int,...]` 契約に統一④V1/V2 モデルを基底型注釈で受け `_run_pure_ev_edge` 戻り型を `PureEVRaceBets` に正名化⑤`TYPE_CHECKING` で循環import回避⑥Playwright 属性と LGBM ハイパラ辞書を `Any`/`dict[str,Any]` 化。型契約回帰テスト `tests/test_typesafety_contracts.py` 23件追加。**残(対応中据え置き)**: analysis/scraper 系 71件は段階適用。隔離worktree(grandslam/typesafety)で実施・並行セッションと非干渉。関連: [[feedback_no_repowide_ruff_format]] [[feedback_parallel_session_conflict]]。影響: src/pipeline/prediction.py, src/ml/bet_generator.py, src/ops/umanity_uploader.py, src/ml/alpha_payout_model.py, src/ml/alpha_place_model.py, src/database/init_db.py(ANALYZE), docs/SYSTEM_ARCHITECTURE.md |
| 2026-06-01 | 【W-061 🟡対応中(中核のみ完了) / 優先度中】型安全の足場固め。`mypy.ini` を新設（`ignore_missing_imports=True`・`follow_imports=silent`・`check_untyped_defs=True`）し、サードパーティ stub 欠如エラーを抑制。会計・実弾判定の中核4モジュール **bet_policy / pnl_accounting / pure_ev_edge / manji_calibration** ＋新設 sns_publisher を **mypy エラー0**（`Success: no issues found in 5 source files`）に到達。修正: `pnl_accounting._summarize` 戻り値を `dict[str, Any]` 明示、`manji_calibration` の較正器キャッシュを `Any` 化。**未達(対応中据え置き)**: 型ヒント100%・全リポジトリ mypy ゼロは未達（中核モジュールに限定）。残りは段階適用とする。関連: [[feedback_no_repowide_ruff_format]]。影響: mypy.ini(新規), src/ml/pnl_accounting.py, src/ml/manji_calibration.py |
| 2026-06-01 | 【W-057 🟢完了 / 優先度中】フィルタ効果のシャドーA/B検証ループを実装。`pnl_accounting.compute_ab_variants()` が確定P&L(コスト=payout−profit・is_superseded除外・単複のみ)で「Pure_EV_Edge適用 vs 従来単複(本命/卍/Alpha)非適用」のROI・純益・勝者を集計し、`health_reporter` の日次Discordヘルスレポートに🅰️🅱️フィールドとして自動追加。これでフィルタの実利益寄与を毎日無人で可視化。テスト `tests/test_w057_shadow_ab.py` 4件。影響: src/ml/pnl_accounting.py, src/ops/health_reporter.py |
| 2026-06-01 | 【W-060 🟢完了 / 優先度高】Pure_EV_Edge が配線バグで生成0だった問題を修正＋メイン配線完了。`_run_pure_ev_edge` の `PureEVConfig(bankroll=)`(TypeError)・`PureEVBet.win_prob`(AttributeError)の2バグで例外握り潰し→**Pure_EV_Edgeは一度も生成・保存されていなかった**。修正し predictions 保存（recommended_bet=¥100×点数）・`bet_policy.LIVE_MODELS`登録・`notify_pure_ev_edge`実装で完全配線。これにより黒字化専用枠が実際に稼働しW-057でA/B追跡可能に。テスト `tests/test_pure_ev_wiring.py` 4件＋全875 PASS。影響: src/pipeline/prediction.py, src/ml/bet_policy.py, src/notification/router.py |
| 2026-06-01 | 【W-048 🟢完了 / 優先度高】卍 confidence=1.0 飽和の根本解消＋安全確認＋実弾復帰。`manji_win_calibrator.pkl` を確定実績で学習（Isotonic・base_rate7%・ev=5→P16.5%と非飽和）、**時系列out-of-sample検証**で飽和34%→0%・較正誤差 **ECE=0.0177**（予測P(win)≒実勝率: [0.10,0.20)→0.151/0.157, [0.40+)→0.462/0.444）を確認し安全性を実測担保。`DISABLE_MANJI_BETS=0` で卍を**単複限定の実弾**に復帰（Phase1 bet_policy ロックにより三連系は不可）。検証スクリプト `scripts/fit_verify_manji_calibration.py`。これで唯一の勝ち頭(卍 単勝 実績ROI674%)が信頼できるEVで稼働。関連: [[W-049]]。影響: data/models/manji_win_calibrator.pkl(生成), src/ml/manji_calibration.py, src/ml/bet_generator.py, .env |
| 2026-06-01 | 【W-059 🟢完了 / 優先度高】実弾の単複限定ロック＋赤字モデル(Oracle/HitFocus)分離。確定実績で三連単−¥1.93M/ワイド−¥204K等が全利益を食い潰す構造が判明。単一真実源 `src/ml/bet_policy.is_live_bet()` を新設し、実弾を 本命/卍/Alpha × 単勝/複勝 のみにロック（三連系・馬連・馬単・ワイド・Oracle・HitFocus を実弾から除外）。Oracle/HitFocus は Discord 上「🎏観賞用・実弾対象外」で note/X 集客のみ。会計は `pnl_accounting.compute_live_roi`(コスト=payout−profit・is_live絞り)＋新規予想の recommended_bet 真コスト化で正常化。確定実績で全体−¥1.74M(ROI80%)→実弾単複+¥778K(ROI222%)を実証。影響: src/ml/bet_policy.py(新規), src/ml/pnl_accounting.py(新規), src/ml/bet_generator.py, src/pipeline/prediction.py, src/notification/discord_notifier.py |
| 2026-05-31 | 【W-058 🟢完了 / 優先度中】日次ヘルスレポートによる可観測性を実装（オーナー承認）。新規 `src/ops/health_reporter.py` が当日の **予想生成カバー率・直前予想率・オッズ時系列健全性（2点以上=drift稼働可/1点/0点）・結果取得欠損数・Discord通知エラー数（ログbest-effort）** を集計し、重大度(ok/warn/crit)に応じ色分けした Embed を Discord #system へ送信。scheduler に `job_health_report`（土日17:50・post_race後・catchup4h）を新設・登録。**本日Dry Run実証**: 「予想24/24・オッズ2点+ 12/1点 12・結果欠損3」→🚨crit と、手動監査で発見した劣化をそのまま自動可視化。これで「12/24が1点」「結果欠損」を毎日自動検知。テスト `tests/test_health_reporter.py` 3件＋関連33件PASS。影響: src/ops/health_reporter.py(新規), scripts/scheduler.py |
| 2026-05-31 | 【月末一斉メンテナンス（オーナー権限・条項2完全バイパス）】6/1完璧稼働へ向け P0〜P1 を一括修正。**W-055 🟢完了**: オッズ時系列の二重管理（realtime_odds と odds_timeseries の分裂・後者は本日0/24で死亡）を **realtime_odds 単一ソース**へ統一。`record_odds_timeseries.py` を「コピー」から発走前ウィンドウのレースへの **実取得(fetch_and_save_odds)** に作り替え、`odds_momentum.py` を realtime_odds 参照へ変更、scheduler を 1分→10分間隔化。これで odds_drift/odds_momentum が最低2点を確実に得る。**W-056 🟢完了**: 発走時刻のハードコード（R1=10:00+30分）を廃し、`races.post_time`(migration#19)を追加、netkeiba `_parse_race_header` で "HH:MM発走" を捕捉→`update_race_details_from_entry` で保存、`today_auto_runner._estimate_start` が post_time 優先（空時のみ推定）。**P1-4 🟢完了**: `predictions.is_superseded`(migration#20)を追加。直前再推論時に `_supersede_prior_predictions()` で旧バリアントを論理無効化、`evaluator` が `is_superseded=0` のみ評価→ROI二重計上を根絶（条項1の例外=内容不改変の論理無効化のみ・オーナー承認）。検証: 全839件PASS。影響: scripts/record_odds_timeseries.py, src/umasugi_engine/factors/odds_momentum.py, scripts/scheduler.py, src/scraper/entry_table.py, src/pipeline/scraping.py, scripts/today_auto_runner.py, src/database/init_db.py, src/database/schema.py, src/evaluation/evaluator.py, src/pipeline/prediction.py |
| 2026-05-31 | 【W-048 🟡→🟢(confidenceバグ修正) / 優先度高】卍 confidence=1.0 固定の**根本修正**（P0-3・オーナー承認）。原因は `min(raw_prob × 係数(5〜30), 1.0)` の係数膨張で confidence が常時1.0飽和→Kelly全額投資→実現ROI26.9%崩壊。新規 `src/ml/manji_calibration.py` で **Isotonic Regression** を確定実績(race_results)で学習（200レース・2861サンプル・base_rate7.0%、ev=2.0→P(win)=15.2% と現実的に較正・飽和なし）。`ManjiStrategy` の単勝prob_topを `calibrate_win_prob`、馬連/ワイド/馬単/三連複の confidence を係数膨張を排した `calibrate_combo_prob` に置換。学習器が無くても保守フォールバックで1.0飽和を防止。**実弾再開(DISABLE_MANJI_BETS=0)はオーナー判断**: confidenceバグは解消したが、確定P&Lでの単複ROI検証後に[[feedback_ev_precision_safety_first]]に沿って単複限定で復帰すべき（バックテストは信用しない）。関連: [[project_pnl_truth_strategy_20260531]]。影響: src/ml/manji_calibration.py(新規), src/ml/bet_generator.py, data/models/manji_win_calibrator.pkl |
| 2026-05-31 | 【W-057 🔴未着手 / 優先度中】フィルタ効果の検証ループ欠如（P1-7）: 危険馬フィルタ・EV補正・卍較正が実際にROIを上げるか測る仕組みが無い。対応方針: フィルタ適用版/非適用版の買い目を両方シャドー記録し確定P&LでA/B数値化（WFバックテストは信用しない）。担当フェーズ: 平日。 |
| 2026-05-31 | 【W-058 🔴未着手 / 優先度中】可観測性の欠如（P2-9）: 「12/24が1点」「odds_timeseries空」「結果3件欠落」を自動検知できず本日まで気づけなかった。対応方針: 日次ヘルスレポート（予想/オッズ点数/結果/通知成否のカバレッジ実測）を毎晩 Discord #system へ自動送信。担当フェーズ: 平日。 |
| 2026-05-31 | 【W-052 🟢完了 / 優先度高】スケジューラ暴走（post_race→retrain 一括評価の全年度SIMULATE暴走）根本修正（オーナー承認・条項2バイパス）。①`batch_evaluate_date` を既定 `retrain=False` 化し毎レースのインライン全年度再シミュレーションを根絶（当日評価+通知のみ）②`weekly_retrain` に土日ガード（`_is_weekend`/`allow_weekend`）追加＝条項2準拠③`job_weekly_retrain` を別スレッド化（`_weekly_retrain_lock` で二重起動防止）し schedule.run_pending をブロックしない。検証: 新規 `tests/test_w052_scheduler_guard.py` 6件＋全824件PASS。影響: scripts/scheduler.py, src/ops/retrain_trigger.py |
| 2026-05-31 | 【ステップ1完了 / JRA-VAN直結 馬体重・天候】速報馬体重(0B11)パーサ `rtd_reader.parse_wh_realtime`＋32bitワーカー `_jvrt_odds_worker`（オッズ0B30/馬体重0B11/天候0B12を1セッション取得）＋`scraping._apply_jvrt_weight_weather`（entries.horse_weight/horse_weight_diff・races.weather/condition を COALESCE/CASE で空NULL非破壊上書き）。**ライブ検証**: 本日 東京2回12日8R で馬体重14頭実取得→entries反映を確認（1番474(-6)…5番478(-14)）。所見: 0B12天候は当該レースで code=-1（RACE dataspec/netkeibaで補完）。影響: src/scraper/rtd_reader.py, scripts/_jvrt_odds_worker.py, src/pipeline/scraping.py, tests/test_jvrt_odds.py |
| 2026-05-31 | 【ステップ2完了 / 推論精度強化（オーナー承認・条項2バイパス）】(2-1)特徴量直前オーバーライド: 直前モードは `fetch_and_save_odds` を常時実行し最新の馬体重/天候/オッズを推論直前に強制反映（`prediction.py:Step1c` の `cached_odds==0` ガードを直前は解除）。(2-2)大口/オッズ歪み検知: 新規 `src/ml/odds_drift.py`（realtime_odds の朝→直前変動率を**レース中央値からの相対乖離**で評価し系統シフトを吸収。plunge=大口流入/abandoned=危険馬）→ `prediction.py:Step4c` で危険馬を軸に含む買い目EVを×0.5減衰しEV<1.0を除外。**ライブ検証**: 8Rで誤検知12頭→意味のある2頭（11番=2.3→1.3倍 大口流入 / 10番=54→156倍 見限り）に改善。W-006 を実質前進。(2-3)異常時再推論: 新規 `src/pipeline/anomaly.py`（取消=最新feed欠落 / 騎手変更=rate-limited netkeiba比較→entries更新）＋ `today_auto_runner` に発走8分前 `recheck` ジョブ追加→異常時のみ自動再推論。検証: 新規 `tests/test_odds_drift.py` 6件・`tests/test_anomaly.py` 5件＋全824件PASS。影響: src/ml/odds_drift.py(新規), src/pipeline/anomaly.py(新規), src/pipeline/prediction.py, scripts/today_auto_runner.py。関連: [[W-006]] |
| 2026-05-31 | 【W-052 🟢完了 / 優先度高】スケジューラ暴走（post_race の全年度再シミュレーション）を根本修正（オーナー承認・条項2バイパス）。真因: `post_race_pipeline`→`incremental_update`→`_build_partial_df` が `_build_train_df`（全年度特徴量再生成）を呼び、当日頭数分(24R)繰り返して約13時間スケジューラをブロック。修正(3点・要件対応): ①レース後評価を **retrain=False** 化し増分学習を停止（評価+Hit Flash通知のみ）・再訓練は月曜 `weekly_retrain` に集約（＝当日経路から全年度再シミュレーションを排除）②`weekly_retrain` に**土日ガード**（条項2）③`job_weekly_retrain` を**バックグラウンドスレッド化**（SIMULATEがメインループをブロックしない・`_weekly_retrain_lock`）。検証: `tests/test_w052_scheduler_guard.py` 6件＋全815件PASS。残: `_build_partial_df` 自体の全件ビルド非効率は将来 incremental を再有効化する場合に最適化要（現状は無効化で回避）。dispatcher dry-runガード([[W-052]]記載のNOTIFY抑制)は今回未対応（retrain停止で誤通知経路は縮小）。影響: scripts/scheduler.py, src/ops/retrain_trigger.py |
| 2026-05-31 | 【W-054 🟢完了（拡張）/ 優先度高】JRA-VAN 速報の**馬体重(0B11/WH)・天候馬場**もリアルタイム化。馬体重WHレイアウトをライブ実証（馬データ開始35・stride45・例482kg-2/492kg+12）。天候馬場は0B42がオッズを返すため不可と判明し0B12のRAレコードを既存 `parse_record` で再利用。速報ワーカーを1セッションでオッズ+馬体重+天候取得に拡張し、Stage0 `_apply_jvrt_weight_weather()` で entries.horse_weight/races.weather を値があるときのみ反映（fail-safe）。馬体重ライブ取得確認済、天候馬場はレース前は未設定（空は非上書き）。テスト10件＋全815件PASS。影響: scripts/_jvrt_odds_worker.py, src/scraper/rtd_reader.py(parse_wh_realtime), src/pipeline/scraping.py |
| 2026-05-31 | 【W-054 🟢完了 / 優先度高】JRA-VAN リアルタイムオッズの COM 一次経路が未実装で恒常的に netkeiba フォールバック（オーナー緊急指示・条項2バイパスで即修復）。**真因調査（ライブ実証）**: `JVInit`=0（認証/契約は正常）だが `src/` 全体に `JVRTOpen` が0件＝速報APIが未実装。リアルタイムオッズは TARGET frontier の `.rtd` キャッシュ依存で、TARGET未起動時(本件)はキャッシュが5/3で停止→毎回 netkeiba に落ちていた（前回コミット deb5bb1e は netkeiba フォールバックを足しただけで jravan_client.py 未修正）。**ライブ検証**: 本日5/31の実 race_id から16桁速報キーを構築し `JVRTOpen("0B30", key)`=code 0、O1速報単勝オッズを実取得（東京2回12日1R: 5番=1.9倍1番人気 等16頭・文字化けなし）。先の-1/-114はキー書式ミスが原因と判明。**修復**: ①`JVLinkClient.rt_open()`(JVRTOpen)追加 ②速報O1パーサ `rtd_reader.parse_o1_realtime()`＋`build_rt_race_key()`（速報版は[37:39]頭数・配列start=43をライブ実証）③32bit ワーカー `scripts/_jvrt_odds_worker.py`（race_id→JSON）④`scraping.fetch_and_save_odds` に **Stage 0: JRA-VAN速報** を追加（完全additive・失敗時は従来 RTD→netkeiba→DB へ自動フォールバック）。検証: 新規 `tests/test_jvrt_odds.py` 7件＋スクレイピング関連50件PASS、64bit→32bitブリッジE2Eで別レースも16頭ライブ取得確認。残: 馬体重(0B11/0B12)・天候馬場(0B42)の速報統合は本コミット未対応（オッズを最優先）。関連: [[W-053]]。影響: src/scraper/jravan_client.py, src/scraper/rtd_reader.py, src/pipeline/scraping.py, scripts/_jvrt_odds_worker.py(新規), scripts/probe_jvlink_realtime.py(新規・診断), tests/test_jvrt_odds.py(新規) |
| 2026-05-31 | 【W-053 🟢完了 / 優先度高】netkeiba スクレイピングブロック(503/429/403)による着順取得失敗（オーナー緊急指示で即修復）。本番ログで netkeiba が 503×201/429/403/404 を返し「3回失敗」多発。根本原因: 単一静的UA+最小ヘッダ・グローバルレート制限欠如(並列スレッドの自己DoS)・Retry-After無視。対応: 共通HTTPクライアント `src/scraper/http_client.py` 新設（UAローテーション/グローバルレート制限 既定1.2s+ジッタ/Retry-After尊重/ステータス別バックオフ: 429・503=長め, 403=UAローテ再試行, 404=即中断）。`netkeiba._fetch_html`・`entry_table._http_get`(tenacity撤去)が共有。JVLink→netkeiba フォールバックはモックテストで発火検証。検証: `tests/test_scraper_resilience.py` 14件＋全798件PASS。残: ①live環境での実効はIP評価/ブロック解除に依存するため次回スケジューラ実走で観測 ②自己DoS抑制には auto_runner の並列度見直し([[W-052]]関連)も推奨。影響: src/scraper/http_client.py(新規), src/scraper/netkeiba.py, src/scraper/entry_table.py, scripts/fetch_race_result.py, tests/test_scraper_resilience.py(新規) |
| 2026-05-31 | 【W-052 🔴未着手 / 優先度高】job_post_race→retrain_trigger 一括評価が全年度SIMULATE再シミュレーションをメインスレッドでインライン無制限実行し scheduler を13時間ブロック: 5/30(土)17:30 の `job_post_race` 起動後、`retrain_trigger 一括評価`(当日24R) が 2024〜2026 全レースの `[SIMULATE]` 特徴量生成ループを `schedule.run_pending` と同一スレッドでインライン実行し、約13時間(CPU約8時間)占有。結果 5/31(土)07:00 暫定予想・08:30 直前予想ジョブが未発火（catchup期限 11:00/11:30 を脅かす本番障害）。さらに評価ループが `dispatcher.dispatch()` を110回(本日15件・例: 馬連 ROI=664% JACKPOT)呼び、`DiscordNotifier._post` に `if not url: return False` 以外の dry-run ガードが無いため本番Discordへ的中通知が誤配信された疑い（本日POST失敗/例外ログ0件）。W-047(job_post_race スレッド化)では本体を別スレッド化したが、内部で呼ぶ `retrain_trigger 一括評価`のシミュレーションがインラインのままで再発。停止には管理者権限が必要（PID 7020 = タスク `\UMALOGI-Scheduler`, Windowsタスクスケジューラ/LocalSystem起動のため非昇格killは「アクセス拒否」）。対応方針: ①retrain_trigger 一括評価のSIMULATEループを別スレッド/別プロセス化 ②評価対象を当日レースのみに限定（全年度再シミュレーション禁止） ③評価/バックテスト経路では `NOTIFY_DISCORD=0` または dry-run で通知dispatch抑制 ④週末(土日)は再訓練・一括評価を実行しないガード(条項2準拠) ⑤schedulerメインループに長時間ジョブのハング検知を追加。復旧手順: 管理者で `schtasks /End /TN "\UMALOGI-Scheduler"` → `schtasks /Run /TN "\UMALOGI-Scheduler"`（起動時catchupで未発火ジョブが復帰）。影響: `scripts/scheduler.py`, `src/ops/retrain_trigger.py`, `src/notification/dispatcher.py`。関連: [[W-047]] |
| 2026-05-31 | 【W-051 🟡対応中 / 優先度中】厳選note/X下書き本文に卍の膨張EVが表示される: 厳選レースの note md / Discord下書きは `_build_race_section` を流用し予測テーブルの全モデル買い目を表示するため、W-048で投資停止中の卍の異常EV（例: 複勝EV=1226%）が下書きに混入する。厳選「判定」からは卍を除外済み（is_gachi_race は Alpha実払戻EVと卍除外クリーン合意のみ使用）だが本文の表示は残る。当面は「Discord下書き→人間が公開前に確認」の運用で緩和。根本対応: W-048解消後に表示復活、または `DISABLE_MANJI_BETS=1` 時は卍セクションを非表示にするガードを `_build_race_section` に追加。影響: `src/ops/note_generator.py` |
| 2026-05-31 | 【厳選レース自動判定・SNS下書き自動生成 実装完了】オーナー承認（条項2バイパス）でX/note集客導線を実装。`note_generator.is_gachi_race()`（Alpha-Payout実払戻EV≥1.25 or 卍除外クリーン合意≥3）・`build_x_post()`・`build_single_race_note_md()`・`export_single_race_note()`・`run_gachi_pipeline()`（top_n=5厳選）・`notify_gachi_for_race()`（pipeline Step7bフック）を追加。`DiscordNotifier.notify_gachi_x_post()` で X コピペテキストを note_draft chへ通知。5/30実データでDry Run検証（5レース厳選・X文面/note md整合確認）。テスト73件PASS。残課題は W-051（卍表示）。影響: src/ops/note_generator.py, src/notification/discord_notifier.py, src/pipeline/prediction.py, .env.example |
| 2026-05-31 | 【W-050 🔴未着手 / 優先度中】BetGeneratorV2 model_type 二重V2タグ不具合: `HonmeiStrategyV2`/`ManjiStrategyV2` が model_type="本命V2"/"卍V2" を返すが、`prediction.py` の `_save_predictions` が is_v2 時に suffix "V2(直前)" を付与するため "本命V2V2(直前)"/"卍V2V2(直前)" となり `insert_prediction` の許可リスト検証で ValueError（例外catch済のため bet レコードが未保存になる。馬分析レコードはハードコード接頭辞 `f"本命{suffix}"` のため "本命V2(直前)" で保存される）。Oracle/HitFocus復活作業中に発見。クリーンHEADから存在する既存不具合で今回の変更とは無関係。対応方針: V2 generate_honmei/manji が "本命"/"卍" を返すよう統一し suffix で V2 を付与する、または _save_predictions 側で二重付与を防ぐ。影響: `src/ml/bet_generator.py`, `src/pipeline/prediction.py` |
| 2026-05-31 | 【W-048 🟡対応中・据え置き判断記録】Oracle/HitFocus復活作業（オーナー承認）に際し卍の `DISABLE_MANJI_BETS=1` 解除を検討したが、根本修正（confidence キャリブレーション）が未実施で実現ROI=26.9%の実損リスクが残るため、オーナー判断により**停止を据え置き**。Oracle/HitFocusのみ復活。月曜にPlatt Scaling等での根本修正を最優先で着手予定（修正プラン4フェーズをセッション末に提示）。 |
| 2026-05-31 | 【W-049 🟢完了 / 優先度中】買い目精度向上フィルタの未実装差分を解消（オーナー特別承認による即時実装）。(a)単勝EVゲート: `TANSHO_EV_MIN=1.2` 未満の単勝を `_apply_odds_band_filter` で除外（卍単勝EV=0.88が実レース202605021201で正しく除外されることを確認）。(b)ワイド多点制限: `_limit_wide_points()` を新設しEV高い順に最大 `WIDE_MAX_POINTS=3` 点へ絞り込み（実レースでワイド5点→3点を確認）。テスト: 新規7件追加（計24件・bet系63件全PASS）。E2E: 実レースで推論→買い目抽出が完走し全買い目が単勝EV≥1.2/ワイド≤3点を満たすことを確認。影響: `src/ml/bet_generator.py`, `tests/test_bet_precision_filters.py`。※IPAT自動投票は引き続き未実装（手動投票運用を維持）。 |
| 2026-05-29 | 【W-048 🟡対応中】卍モデル confidence=1.0 固定キャリブレーション破綻: 一時的対策として `DISABLE_MANJI_BETS=1` 環境変数フラグを `ManjiStrategy.generate()` に実装し `.env` に設定済み（投資完全停止中）。根本修正（Platt Scaling）は未実施。影響: `src/ml/bet_generator.py`, `.env` |
| 2026-05-29 | 【W-048 新規登録】卍モデル confidence=1.0 固定キャリブレーション破綻: EV上位20件の実現ROI=26.9%（EV予測値11〜17に対し実現倍率1〜2倍）。根本原因: `confidence=1.0` 固定出力で `EV = confidence × odds` が odds の大小だけ反映。Platt Scaling / Isotonic Regression による出力確率のキャリブレーション修正が必要。修正完了まで卍モデルへの投資停止を推奨。影響: `src/ml/models.py`, `src/ml/bet_generator.py` |
| 2026-05-29 | 【W-047 新規登録→即完了】job_post_race スレッドブロック: 5/23(土)17:30〜5/24(日)16:53（23時間）`job_post_race` がメインスレッドを占有し日曜バッチ（job_friday_sync）が未実行。バックグラウンドスレッド化で修正済み。影響: `scripts/scheduler.py` |
| 2026-05-29 | 【W-046 新規登録→即完了】全モデル横断バックテスト未統合: 本命・卍・複勝・ALPHA の全4モデルを2年間データで横断比較する `scripts/backtest_all_models.py` を新規作成。影響ファイル: scripts/backtest_all_models.py, tests/test_backtest_all_models.py |
| 2026-05-18 | 初版作成。社長指令「ビジョン再監査」を受け、U score ギャップ・インフラ・データ弱点を全面棚卸し |
| 2026-05-18 | 【W-004 実装完了】大衆心理乖離スコア (crowd_bias_ratio / uf_crowd_bias) を u_score.py・models.py・bet_generator.py に追加。ManjiGenerator・HonmeiGenerator の EV 調整まで統合完了 |
| 2026-05-19 | 【W-026 完了確認】_IsotonicModel プロキシ追加により増分学習 E2E 動作確認済み。フルモード WF バックテスト完走（OOM回避: expanding window + float32 + max_bin=127）。全21組み合わせ ROI 100%超。★QF推奨戦略（本命×ワイド ROI=805%/複勝×馬連 ROI=963%）を bet_generator.py・notify_discord.py に実装。W-022 部分対応: QF推奨 EV≥1.3 フィルタを実質的に適用 |
| 2026-05-19 | 【W-022 完全実装】動的EV閾値: get_dynamic_ev_threshold() で直近28日ROIから1.1/1.2/1.3/1.5を自動選択。Kelly資金管理: calc_qf_kelly_bet()実装。notify_discord.pyにDB接続→閾値・バンクロール自動取得・QF推奨セクションへの推奨ベット額・Kelly%・総資金比を表示統合。影響: src/ml/bet_generator.py / scripts/notify_discord.py |
| 2026-05-19 | 【V1/V2 モデル分離・週次再学習対応完了】models_v2.py 新設・BetGeneratorV2・prerace_pipeline model_version 引数・_archive_and_save() 命名バグ修正・IncrementalTrainer.full_retrain() V2 同時再学習対応。今週末より実弾 A/B テスト開始。影響: src/ml/models_v2.py / src/ml/incremental.py / src/pipeline/prediction.py |
| 2026-05-20 | 【商用化ロードマップ策定・全4週タスク完了】通知ルーター(W-028完了)・実績レポート自動化(generate_performance_report.py)・A/Bテスト自動比較(generate_ab_report.py)・note下書き転送・X信号統合Phase C(FEATURE_COLS)・有料JACKPOT記事フォーマット確立(generate_note_article.py --jackpot-only)・scheduler 月曜08:30/日曜18:00自動ジョブ登録 |
| 2026-05-20 | Discord 通知ルーター新設 (NotificationRouter): EV激熱アラート・note下書き転送・ENABLE_PLAYWRIGHT_POST トグル・IS_PREMIUM_NOTE 有料/無料出し分け・買い方テンプレート自動生成・2カ年バックテストシミュレーター・万馬券特化報告スクリプト実装。影響: src/notification/router.py, src/pipeline/prediction.py, scripts/post_weekly_note_draft.py, scripts/generate_weekly_note.py, scripts/run_2year_backtest.py, scripts/generate_result_note_draft.py |
| 2026-05-20 | EV 特化特徴量エンジン Phase 1 実装（71 テスト全 PASS）: JRATakeoutRates（控除率クラス定数）・Shin 1993 真確率推定・Harville 法・オッズ異常検知・np.cumprod Kelly バンクロールシミュレーター・Sharpe/MDD・グリッドサーチ・READ ONLY DB 監査スクリプト。W-029 (DB インデックス最適化) を Phase 2 として計上、承認待ち。|
| 2026-05-21 | 【W-031 完了】V1 vs V2 A/B テスト週次レポート自動化: `generate_ab_report.py` 完全実装（`build_ab_report()` Markdown生成 + `_send_summary_to_discord()` Embed プッシュ送信）。`scheduler.py` 日曜18:00 自動配信・取りこぼし4時間窓。エラーハンドリング: HTTPError/OSError は WARNING ログ止まり・例外伝播なし。テスト17件 PASS。W-024 を 🟡 対応中 に昇格（週次 ROI レポートが監視要件を部分充足）。実測: V1 ROI=64.1%/純利益 ¥-2,300,518 / V2=0件（V2稼働前）|
| 2026-05-23 | 【W-032 新規登録】スケジューラークロスデイ回収バグ: `_recover_missed_jobs()` が当日曜日のジョブしか確認しないため、前日のジョブ（job_friday_sync の 16h 窓など）が土曜朝起動時に完全スキップされる脆弱性。`day_delta in (0, -1)` ループで前日チェックを追加し修正済み（2026-05-23完了）。影響: scripts/scheduler.py |
| 2026-05-23 | 【note完全自動化ルーティン完成・W-033 新規登録→即完了】`job_note_daily_article()` を scheduler.py に追加（土日10:30）。4ステップ自動実行（記事生成→Discord転送→Embed送信→note.com下書き）。`NOTE_DRAFT_AUTO_POST=0`（デフォルト）でPlaywright未起動でも安全完走。`NOTE_DRAFT_AUTO_POST=1` + `.note_session.json` 存在時のみ Playwright 自動保存。テスト15件PASS / 全560件GREEN。影響: scripts/scheduler.py, tests/test_scheduler_note_article.py(新規), .env(NOTE_DRAFT_AUTO_POST=0追加) |
| 2026-05-27 | 【W-036 完了】キャリブレーション補正: `src/ml/calibration.py` 新設。bin別補正倍率(avg×2.77)を `HonmeiStrategy.generate()` に統合。補正前EV=0.80(採用不可)→補正後EV=1.53(採用可)の隠れ優良レースが発見可能に。ユニットテスト20件 PASS。W-036 🟢完了。影響: `src/ml/calibration.py`, `src/ml/bet_generator.py` |
| 2026-05-28 | 【卍×三連複 ROI=46.7% 損失対応完了】`ManjiStrategy.generate()` の `_TRIO_EV_MIN=1.0` EVゲートを復活（EVゲート撤廃コメントを削除）。TDDで `tests/test_bet_generator_ev_gate.py` 3件追加・全PASS確認済み。影響: `src/ml/bet_generator.py`, `tests/test_bet_generator_ev_gate.py` |
| 2026-05-28 | 【W-038 完了】卍モデル三連複EVゲート復活: `_TRIO_EV_MIN = 1.0` を ManjiGenerator に追加。本番実績 ROI 46.7% の三連複買い目を撤廃。影響: `src/ml/bet_generator.py` |
| 2026-05-28 | 【W-039 完了】バックテスト評価品質改善: `--mode flat` フラグを `scripts/backtest_2024_2025.py` に追加。Kelly複利ROI膨張（最大15000%）なしの実態値を算出可能に。影響: `scripts/backtest_2024_2025.py` |
| 2026-05-28 | 【W-043 新規登録】日次損失サーキットブレーカー欠如: ドローダウン中でも予測・投票が継続する。`scheduler.py` に当日P&Lチェックと停止フラグを実装予定 |
| 2026-05-28 | 【W-044 新規登録】今日の自動運行リトライ無限ループ: `job_today_auto_runner` 内 `_run_loop()` に最大クラッシュ数制限がない。max_crashes=10 を追加予定 |
| 2026-05-28 | 【W-045 新規登録】shap_json スキーマドリフト: migration追加のみでschema.pyのCREATE TABLEに未反映。次期改修時に統合予定 |
| 2026-05-28 | 【W-042 新規登録→即完了】NaN確率/EV → Kelly最大分数誘発バグ: `_kelly_bet(NaN, ...)` および `calc_kelly_stake(_, NaN, ...)` で `float('nan') <= 0.0` が Pythonで False を返しガードをスルーする問題を `math.isnan()` 先頭チェックで修正。`HonmeiModel/PlaceModel/ManjiModel.predict()` に `np.nan_to_num(nan=0.0)` NaN補完を追加。影響: src/ml/bet_generator.py, src/ml/models.py |
| 2026-05-28 | 【W-041 新規登録→即完了】JVDataLoader.load() RuntimeError 未リトライ + DB コネクションリーク: `except (TimeoutError, RuntimeError)` 統合 + `try/finally: conn.close()` 追加。`sync_wood()` に OPT_STORED フォールバック追加。影響: src/scraper/jravan_client.py, src/ops/data_sync.py |
| 2026-05-28 | 【W-040 完了】セグメント分析基盤新設: `scripts/segment_analysis.py` で券種×会場×馬場状態別ROIを自動集計。月曜07:30 Discord自動配信開始。影響: `scripts/segment_analysis.py`, `scripts/scheduler.py` |
| 2026-05-27 | 【W-037 完了】動的セーフティ実装: `calc_kelly_stake()` に `GLOBAL_BALANCE_CAP_PCT=0.05`・`_dynamic_kelly_fraction()`・最低保証額廃止の3機能を追加。Monte Carlo 10,000試行で1/4Kelly破産率 75.97%→**0.00%** 達成（目標<10% クリア）。中央最終残高 ¥831,505。W-037 🟢完了。影響: `src/ml/bet_generator.py`, `scripts/calibration_kelly_audit.py` |
| 2026-05-27 | 【W-036/W-037 新規登録・キャリブレーション&Kelly監査実施】`scripts/calibration_kelly_audit.py` 新設。① 本命(直前)モデルのキャリブレーション: model_score が全 bin で実績的中率を下回る（補正倍率 avg=1.91）→ EV 過小評価状態（守保的設計としては良好だが改善余地あり）。② Kelly Monte Carlo (10,000試行): recommended_bet 基準の真ROI=119.8%、純損益=+¥864,650 で黒字確認。ただし破産率54-93%（全Kelly係数）→ 初期資金¥100K では三連単高額ベットによる連敗で ¥10K ラインを割るリスクが高い。推奨: 運用資金を ¥300K〜¥500K 以上に引き上げ or バランス比率に応じた動的Kelly圧縮を実装。結果は `data/calibration_kelly_audit.json` に保存。 |
| 2026-05-24 | 【W-035 新規登録】training_hillwork 坂路データ 0 件問題: JVLink WOOD に WH レコード不含（仕様）＋ netkeiba 調教ページはレース後404 → 歴史バックフィル不可を確認。`job_training_hillwork_scrape()` を scheduler.py に追加（木曜20:00・金曜18:00）し今週末以降のレースから自動収集開始。 |
| 2026-05-24 | 【umasugi_engine Phase2 完了】調教グレード(8%) + オッズモメンタム(5%) を追加。正規化JOINキー(horse_id[:4]+horse_id[4:9])でtraining_times接続率45.6%達成。`odds_timeseries`テーブル新設・毎分記録ジョブをschedulerに統合。バックテスト ROI73.7%(閾値0.50)。影響: `src/umasugi_engine/scorer.py` `src/umasugi_engine/factors/training_grade.py` `src/umasugi_engine/factors/odds_momentum.py` `scripts/record_odds_timeseries.py` |
| 2026-05-24 | 【umasugi_engine Phase1 実装完了】`src/umasugi_engine/` 新設（ラッパー型）。小回り適性(track_style)・野芝/洋芝(turf_type)・世論分析フィルター(crowd_opinion)を実装。バックテスト: Legacy ROI 68.2% → Umasugi ROI 73.6% (閾値0.50)。ウェイト: turf=0.15(洋芝不得意馬の的中率0%を検出)/track=0.10/crowd=EV直接適用。`/api/compare/[race_id]` エンドポイント追加。設計書: docs/superpowers/specs/2026-05-24-umasugi-engine-design.md |
| 2026-05-24 | 【W-022 追加対応・Kelly完全統合】`calc_kelly_stake()` 公開関数新設・`_KELLY_TYPE_CAPS` 券種別上限辞書追加（複勝3%/馬連1.5%/三連複1%）。ManjiGenerator/HonmeiStrategy/AlphaTrifectaStrategy の `recommended_bet` を ¥100固定→Kelly動的算出に全面移行。WF実証 Alpha-Payout ROI 129.2%（¥100固定64%から+65.2pt改善）。`data_validator.py` 新設でパイプライン先頭での win_odds≥500 センチネル除外を実施。UIに Kelly理論 vs ¥100固定 比較パネル追加。影響: src/ml/bet_generator.py, src/ml/data_validator.py, web/src/components/FinancialDashboard.tsx |
| 2026-05-23 | 【W-034 完全監査・最終版】バックテスト完全リライト: ①データリーク修正 (`build_race_df test_mode=True`)。②Oracle/HitFocus の特徴量混入調査→買い目生成戦略のみ・FEATURE_COLSへの混入ゼロ確認。③TYPICAL_ODDSをモデルトップ馬実態値に更新（ワイド4.0x→2.5x/馬連12.0x→5.0x/三連複35.0x→15.0x）。④コンボ系（ワイド/馬連/三連複）を `COMBO_BET_MODE="disabled"` で無効化（実績ROI 25-50% — 赤字確定）。⑤Kelly に `actual_win_odds` 対応追加（race_results.win_odds 優先）。最終結果: 複勝+単勝のみ・1月ROI 94.9%・2月ROI 61.6%・全体ROI 89.7%・2月に実質破産。改善方針: 複勝特徴量追加→ROI 110%+目標。影響: src/analysis/all_bets_backtest_2026.py, .claudecode/rules/honmei_real_bet_rule.md |
| 2026-05-23 | 【W-017 強化完了】JVLink ダイアログ自動突破ハンドラー新設: `src/ops/jvlink_dialog_handler.py` — 0.3 秒間隔でデスクトップ全ウィンドウをスキャンし、JVLink/設定/セットアップ系ダイアログを BM_CLICK → WM_COMMAND IDOK → VK_RETURN の 3 段階で 0.5 秒以内に自動消去。`scheduler.py` の `run_daemon()` に daemon スレッドとして組み込み。既存の 10 秒タイムアウト → netkeiba fallback と共存する二重安全網を構築。頑固ダイアログ（3 秒超）は WARNING ログ + fallback に委譲。テスト 26 件 PASS（全 512 件 GREEN）。影響: src/ops/jvlink_dialog_handler.py, scripts/scheduler.py |
| 2026-05-21 | 【Week1-4 商用化ロードマップ完全完了 + 本番環境ロック確定】① W-029 完了: DB 複合インデックス 6 件 (migration #15) 適用。idx_pred_model_ev/idx_pred_race_model/idx_tc_horse_date/idx_hc_horse_date/idx_rr_horse_race/idx_pr_pred_hit。② W-030 完了: EV 特化特徴量 7 本を features.py へ統合・try/except ガード付き安全実装。③ 69 FEATURE_COLS 全モデル完全再訓練: HonmeiModel CV AUC=**0.7677** (特徴量重要度 Top3: uf_rank_trend/uf_jockey_win_rate/u_score) / PlaceModel AUC=**0.7293** / ManjiModel 完了。Parquet cache 84,930 行×90 列で再学習 95% 短縮 (38分→2分)。466 テスト ALL GREEN。④ E2E 本番シミュレーション (scripts/e2e_production_sim.py) 全 6 ステップ ALL PASS: prerace_pipeline 2.13秒 / 全 Discord チャンネル routing 確認 / 総スループット **5.12秒**。⑤ 本番ロック確定: DISCORD_WEBHOOK_URL/EV_ALERT/AB_TEST/NOTE_DRAFT/DISCORD_SYSTEM_WEBHOOK_URL 全 URL 設定済み・JVLINK_DISABLED 未設定 (本番 JVLink 有効) / ENABLE_PLAYWRIGHT_POST=0 (X 自動投稿安全オフ) / DRY_RUN 未設定 (本番モード) / scheduler.py 全ジョブ dry_run=False 確認済み。⑥ system アラートテスト 2 件追加 (test_system_alert_routes_to_system_channel / test_legacy_system_webhook_url_accepted): 計 12 テスト PASS。⑦ .env.example 復旧 (全 13 キー完全文書化)。影響: tests/notification/test_router.py, .env.example(復旧) |

---

## ステータス凡例

| マーク | 意味 |
|--------|------|
| 🔴 未着手 | 対応を開始していない |
| 🟡 対応中 | 実装・調査が進行中 |
| 🟢 完了 | 本番反映済み・検証完了 |
| ⚪ 保留 | 意図的に対応を見送り中（理由を記載） |

---

## カテゴリ 1: U score — 完全体ビジョンとのギャップ

> **社長ビジョン**: 「1000以上の要素から厳選した30項目（加速力・PCI・不完全燃焼度・大衆心理ジレンマ排除・AIチーム目視分析）」  
> **現状**: Phase 1 として18因子実装済み（2026-05-17）  
> **目標差分**: **12因子** の追加実装が必要

### 現在実装済み（Phase 1: 18因子）

| # | 因子名 | グループ | 重み | ステータス |
|---|-------|---------|------|-----------|
| 1 | 通算勝率 | A: 能力指数 | 40% | 🟢 完了 |
| 2 | 馬場別勝率 | A: 能力指数 | - | 🟢 完了 |
| 3 | 距離帯別勝率 | A: 能力指数 | - | 🟢 完了 |
| 4 | 直近着順スコア | A: 能力指数 | - | 🟢 完了 |
| 5 | 着順改善トレンド | A: 能力指数 | - | 🟢 完了 |
| 6 | 前走休養日数 | A: 能力指数 | - | 🟢 完了 |
| 7 | 騎手直近勝率 (90日) | B: 人的要素 | 30% | 🟢 完了 |
| 8 | 調教師直近勝率 (90日) | B: 人的要素 | - | 🟢 完了 |
| 9 | 騎手×馬コンビ率 | B: 人的要素 | - | 🟢 完了 |
| 10 | 騎手×会場勝率 | B: 人的要素 | - | 🟢 完了 |
| 11 | 枠番適性 | C: コース適性 | 20% | 🟢 完了 |
| 12 | 会場別勝率 | C: コース適性 | - | 🟢 完了 |
| 13 | 美浦・栗東マッチ | C: コース適性 | - | 🟢 完了 |
| 14 | ウッドスピード指数 | D: 調教指数 | 7% | 🟢 完了 |
| 15 | 坂路スピード指数 | D: 調教指数 | - | 🟢 完了 |
| 16 | 父馬距離適性 | E: 血統適性 | 3% | 🟢 完了 |
| 17 | 母父馬場適性 | E: 血統適性 | - | 🟢 完了 |
| 18 | 父系統適性 | E: 血統適性 | - | 🟢 完了 |

---

### 未実装（Phase 2: 12因子 — 社長ビジョン完全体へ）

#### W-001: 加速力スコア (Acceleration Score)

| 項目 | 内容 |
|------|------|
| **優先度** | 🔴 高 |
| **ステータス** | 🔴 未着手 |
| **社長ビジョン** | 「加速力」— 単なる着順でなく、上がり3Fと前半ラップの差分から"末脚の切れ味"を数値化 |
| **実装概要** | `race_results.finish_time` + JVLink ラップタイム（3F/4F）から算出。上がり3F≤34.0秒で高スコア。`v_race_mart` に `last3f_time` を追加後に算出可能 |
| **データ依存** | ラップタイム: JVLink RACE RA レコードから取得（現在未格納）|
| **追加SQL** | `training_times.time_3f` / 本番は `races` に `lap_3f` カラム追加が必要 |
| **担当フェーズ** | Phase 2-A |

#### W-002: ペース変動指数 (PCI: Pace Change Index)

| 項目 | 内容 |
|------|------|
| **優先度** | 🔴 高 |
| **ステータス** | 🔴 未着手 |
| **社長ビジョン** | 「PCI」— レース前半と後半のペース差分。ハイペース流れ込み型か、スロー上がり勝負型かを数値化し、脚質との相性スコアに変換 |
| **実装概要** | PCI = (後半3F × 2) / 全体タイム。馬の脚質（先行/差し/追込）とのマッチング係数 `pci_style_match` として実装 |
| **データ依存** | レースラップタイム（上記 W-001 と同じ DB カラム追加が前提）|
| **担当フェーズ** | Phase 2-A（W-001 と同時実装可能）|

#### W-003: 不完全燃焼度スコア (Incomplete Combustion Score)

| 項目 | 内容 |
|------|------|
| **優先度** | 🔴 高 |
| **ステータス** | 🔴 未着手 |
| **社長ビジョン** | 「不完全燃焼度」— 前走で力を出し切れなかった馬を発掘。"今回こそ爆発する"という隠れた期待値の源泉 |
| **実装概要** | 以下の条件で `uf_incompleteness` スコア (0〜1) を算出:<br>① 前走着順 < モデル予測順位（実力負け）<br>② 前走が不良馬場で今回は良馬場<br>③ 前走が大外枠で今回は内枠<br>④ 前走がハイペースで今回はスロー予想<br>各条件に重みを付けて合算 |
| **データ依存** | 前走 predictions テーブルのスコア + 馬場状態 + 枠番の変化 |
| **担当フェーズ** | Phase 2-B |

#### W-004: 大衆心理乖離スコア (Crowd Bias Removal Score)

| 項目 | 内容 |
|------|------|
| **優先度** | 🔴 高 |
| **ステータス** | 🟢 完了（2026-05-18） |
| **社長ビジョン** | 「大衆心理のジレンマを排除した真の期待値算出」— 市場の過大/過小評価を定量化し、モデルの EV 計算に組み込む |
| **実装概要** | `crowd_bias_ratio = win_rate_all / market_implied_prob`（学習特徴量）<br>市場乖離 EV 倍率: crowd_bias > 1.3 → 最大 1.5x EV ブースト<br>crowd_bias < 0.7 → 最小 0.5x EV ペナルティ<br>bet_generator.py の ManjiGenerator / HonmeiGenerator 両方に適用済み |
| **実装ファイル** | `src/ml/u_score.py` (_calc_crowd_bias 新設・グループF追加・重み5%)<br>`src/ml/models.py` (FEATURE_COLS: uf_crowd_bias / crowd_bias_ratio 追加)<br>`src/ml/bet_generator.py` (_crowd_bias_ev_multiplier 新設・両Generator適用) |
| **データ依存** | `market_prob` 列（features.py で `1/min(win_odds, 80)` として既存） |
| **効果測定** | ドライラン再学習（2026-05-18）: HonmeiModel AUC **0.7591 → 0.7679**（+0.0088向上） |
| **担当フェーズ** | Phase 2-B ✅ |

#### W-005: X シグナルコンセンサス係数

| 項目 | 内容 |
|------|------|
| **優先度** | 🔴 高 |
| **ステータス** | 🟡 対応中（Phase A スクレイパー実装済み、Phase B 構造化未着手）|
| **社長ビジョン** | 「AIチームの目視分析」— 凄腕予想家の印を AI で構造化し、EV 計算の第4のファクターとして加算 |
| **実装概要** | `x_consensus_score = weighted_avg(x_signals.confidence)` by horse_number<br>重みは過去的中率で動的調整<br>FEATURE_COLS に `x_consensus_score` 追加 |
| **データ依存** | `x_signals` テーブル（DDL 作成済み）、Phase B: Claude Haiku 構造化 |
| **残作業** | Phase B: `src/ml/x_signal_parser.py` 作成<br>Phase C: FEATURE_COLS 統合 + モデル再訓練 |
| **担当フェーズ** | Phase B: 平日実装 |

#### W-006: オッズ動向スマートマネーシグナル

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🟡 対応中（買い目フィルタは実装・2026-05-31／モデル特徴量化は未）|
| **実装概要** | `odds_vs_morning`: 直前オッズ / 朝一オッズ（短縮=大口流入シグナル）<br>`odds_velocity`: 直近1時間のオッズ下落速度<br>**2026-05-31 追加**: `src/ml/odds_drift.py` が realtime_odds の朝→直前変動率を**レース中央値相対**で評価し plunge(大口流入)/abandoned(危険馬) を検知。買い目段で危険馬軸のEV減衰・除外に活用（ステップ2-2）|
| **データ依存** | `realtime_odds` の時系列蓄積（直前 `fetch_and_save_odds` 常時実行で朝暫定+直前の2点以上を確保）|
| **残作業** | ①朝一オッズの安定2点保存（8:30/14:30）②`odds_drift` をモデル FEATURE_COLS へ特徴量化（現状は買い目フィルタのみ）|
| **担当フェーズ** | Phase 2-C |

#### W-007: 斤量インパクト因子

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🔴 未着手 |
| **実装概要** | 前走比斤量変化（`weight_carried_diff`）と、その馬の斤量耐性（過去斤量55kg超のレースでの勝率）を組み合わせたスコア |
| **データ依存** | `race_results.weight_carried`（既存）|
| **担当フェーズ** | Phase 2-A（データ既存のため比較的容易）|

#### W-008: 馬場バイアス × 脚質マッチング

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🔴 未着手 |
| **実装概要** | コース別馬場状態（良/稍重/重/不良）× 馬の脚質（先行/差し/追込）の過去勝率マトリクスを特徴量化。`uf_surface_style_match` スコア |
| **データ依存** | `races.condition`（既存）+ 脚質分類（前走着順推移から推定）|
| **担当フェーズ** | Phase 2-B |

#### W-009: 輸送疲れ係数

| 項目 | 内容 |
|------|------|
| **優先度** | 🟢 低 |
| **ステータス** | 🔴 未着手 |
| **実装概要** | 前走会場 → 今走会場の輸送距離（例: 函館→東京=長距離）に応じて `uf_transport_fatigue` スコアを減点。輸送歴がある馬の克服実績で補正 |
| **データ依存** | `races.venue`（既存）|
| **担当フェーズ** | Phase 2-C |

#### W-010: 相手関係指数 (Competition Strength Index)

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🔴 未着手 |
| **実装概要** | 今回の出走メンバーの `u_score` 平均値と対象馬の `u_score` の差分。強いメンバーでの高着順は評価アップ、弱いメンバーでの高着順は評価抑制 |
| **データ依存** | `u_score` の実装完了（Phase 1 済み）|
| **担当フェーズ** | Phase 2-B |

#### W-011: クラス昇降格インパクト

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🔴 未着手 |
| **実装概要** | 前走クラス（500万下/1000万下/オープン等）と今走クラスの差分。降格馬は高評価、昇格馬は割引。`uf_class_change` (-1〜+1) |
| **データ依存** | `races.grade`（既存）|
| **担当フェーズ** | Phase 2-A |

#### W-012: 非公開情報スコア (Proprietary Signal Score)

| 項目 | 内容 |
|------|------|
| **優先度** | 🟢 低（将来）|
| **ステータス** | 🔴 未着手 |
| **社長ビジョン** | 「1000以上の要素から厳選」の最終段階。X シグナル・厩舎コメント・前日オッズ異変など非定量情報を Claude で構造化 |
| **実装概要** | Phase B の x_signal_parser.py を拡張し、厩舎コメント（JVLink SE レコードの「調教コメント」）も取得・構造化 |
| **データ依存** | x_signal_parser.py Phase B 完成後 |
| **担当フェーズ** | Phase 3 以降 |

---

### U score ギャップサマリー

```
社長ビジョン 30因子
  ├─ 実装済み: 18因子 (Phase 1完了 ✓)
  ├─ Phase 2-A: W-001(加速力) / W-002(PCI) / W-007(斤量) / W-011(クラス変化) — 4因子
  ├─ Phase 2-B: W-003(不完全燃焼) / W-004(大衆心理) / W-008(馬場脚質) / W-010(相手関係) — 4因子
  ├─ Phase 2-C: W-006(オッズ動向) / W-009(輸送疲れ) — 2因子
  └─ Phase B連携: W-005(Xシグナル) / W-012(非公開情報) — 2因子

Phase 2-A 完了後: 22因子
Phase 2-B 完了後: 26因子
Phase 2-C+B完了後: 30因子 ← 社長ビジョン達成
```

---

## カテゴリ 2: データ弱点

#### W-013: win_odds 歴史データ欠損（JVLink SID 制約）

| 項目 | 内容 |
|------|------|
| **ステータス** | ⚪ 保留（外部制約）|
| **影響** | 2024-01 以前の単勝オッズが学習データに存在しない → EV 計算精度が2024年以降のデータに依存 |
| **対応方針** | `netkeiba_research.db` に 2024-2025 を 99.1% スクレイプ済み。本番 DB へのマージが完了次第、歴史オッズで再学習 |
| **解除条件** | CLAUDE.md §14「歴史データ大規模取得」参照 |

#### W-014: jockeys/trainers マスタ未充足

| 項目 | 内容 |
|------|------|
| **ステータス** | ⚪ 保留 |
| **影響** | 騎手・調教師のコード→名前変換に LabelEncode（名前文字列）を使用中。名前変更や新人への対応が不安定 |
| **対応方針** | JVLink DIFN KS/CH マスタを週次取得で充足。スケジューラの job_monday_masters() で実施中 |

#### W-015: ラップタイムデータ未格納

| 項目 | 内容 |
|------|------|
| **ステータス** | 🔴 未着手（W-001/W-002 の前提条件）|
| **影響** | 加速力・PCI 因子が実装できない。上がり3F が最重要指標なのに DB に存在しない |
| **対応方針** | JVLink RACE RA レコードから `RA_LAPS_*` フィールドを取得し `races` テーブルに `last3f_time` / `lap_data` カラムを追加 |
| **作業量** | jravan_client.py の RA パーサー拡張 + schema.py マイグレーション |

#### W-016: 2025年着順データの欠損バイアス

| 項目 | 内容 |
|------|------|
| **ステータス** | 🟡 対応中 |
| **影響** | 2025年 race_results の rank データが有効行 11.5% のみ（残りは rank=0 or NULL）。本命/卍モデルのバックテストが無効 |
| **対応方針** | netkeiba_research.db から 2〜18 着を補完するスクリプトを作成 |

---

## カテゴリ 3: インフラ弱点

#### W-017: JVLink ダイアログ抑制の不確実性

| 項目 | 内容 |
|------|------|
| **ステータス** | 🟢 完了（2026-05-23 最終強化）|
| **修正内容（2026-05-18）** | 3段フォールバック（ParentHWnd → JVSetUIProperties → JVSetUI(0)）+ `_kill_stale_py32` 64bit 誤 kill 根治 + `_JVLINK_STARTUP_TIMEOUT` 60秒化 |
| **強化内容（2026-05-23）** | `src/ops/jvlink_dialog_handler.py` 新設。0.3 秒間隔でデスクトップ全ウィンドウをスキャンし、JVLink/設定/セットアップ/認証/ライセンス系ダイアログを検知次第 **BM_CLICK → WM_COMMAND IDOK → VK_RETURN** の優先順で自動クリック。`scheduler.py run_daemon()` から daemon スレッドとして起動。既存の 10秒タイムアウト → netkeiba fallback と共存する二重安全網 |
| **安全網の層構造** | ① ダイアログ生成自体を COM フラグで抑制（2026-05-18）→ ② 出現したダイアログを 0.3 秒以内に自動クリック（2026-05-23）→ ③ 3秒超残存で WARNING + 10秒タイムアウト Kill → netkeiba fallback |
| **E2E 証明** | elapsed=2.78s で JVLINK_READY 受信確認。テスト 26 件 PASS（全 512 件 GREEN）|

#### W-018: オッズ取得の netkeiba 依存

| 項目 | 内容 |
|------|------|
| **ステータス** | 🟡 対応中 |
| **影響** | JVLink realtime_odds が空の場合、netkeiba からフォールバック取得。netkeiba の利用規約・レート制限に依存 |
| **対応方針** | CLAUDE.md §11「JVLink 一次・netkeiba 二次」の二段構え維持。JVLink SID 制約解消後に移行 |

#### W-019: SQLite の並行書き込み競合

| 項目 | 内容 |
|------|------|
| **ステータス** | ⚪ 保留（現状スケール内）|
| **影響** | scheduler.py の複数ジョブが同時 DB 書き込みを試みると SQLite の write lock 競合が起きる可能性 |
| **対応方針** | WAL モード + `busy_timeout=10000ms` で対応済み（init_db.py）。スケール拡大時は PostgreSQL 移行を検討 |

---

## カテゴリ 4: モデル弱点

#### W-020: FukushoElite 本番未統合

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🟡 対応中（実装済み・本番未結合）|
| **影響** | 複勝 ROI 95.4% → 110%+ 目標のモデルが本番 predictions テーブルに結合されていない |
| **対応方針** | CLAUDE.md §13 参照。X シグナル統合後に再訓練してから本番統合 |

#### W-021: WIN5 モデル精度不足

| 項目 | 内容 |
|------|------|
| **優先度** | 🟢 低 |
| **ステータス** | 🔴 未着手 |
| **影響** | WIN5 ROI 22.8%（目標 110%+）。現在は等確率 + market 50/50 ブレンドで実質ランダム |
| **対応方針** | 本命モデルスコアを WIN5 エンジンに組み込む（CLAUDE.md §15 Plan B参照）|

#### W-022: 動的EV閾値の実装

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🟢 完了（2026-05-19） |
| **実装内容** | `get_dynamic_ev_threshold(conn, lookback_days=28)` を `src/ml/bet_generator.py` に実装<br>直近28日の prediction_results ROI を計算し自動で閾値を選択:<br>ROI≥150% → 1.1(好調期) / ROI 110-150% → 1.2(通常期) / ROI 80-110% → 1.3(低調期) / ROI<80% → 1.5(不調期)<br>Kelly資金管理: `calc_qf_kelly_bet(ev_score, win_odds, bankroll)` で 1/4 Kelly ベット額を算出<br>Discord通知: `_get_threshold_and_bankroll()` でDB接続し動的取得。ヘッダーにモード・ROI・総資金を表示。★QF推奨セクションに推奨ベット額・Kelly%・総資金比を表示 |
| **実機検証** | 直近28日 ROI=62.2% → 不調期 → 閾値1.5 自動適用。Kelly算出正常確認（EV=1.5/odds=5.0 → 1/4 Kelly=3.12% → ¥300/点） |
| **影響ファイル** | `src/ml/bet_generator.py` (2関数追加) / `scripts/notify_discord.py` (全面改修) |

#### W-023: 破産確率 UI の未実装

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🔴 未着手 |
| **影響** | Kelly バンクロールの破産リスクが可視化されていない |
| **対応方針** | Monte Carlo シミュレーション（CLAUDE.md §4-B）|

---

## カテゴリ 5: UI/UX 弱点

#### W-024: 的中率低下のリアルタイムアラート未実装

| 項目 | 内容 |
|------|------|
| **ステータス** | 🟡 対応中（週次 ROI レポートは実装済み、閾値アラートは未実装）|
| **影響** | ROI が急落しても Discord で自動アラートが来ない。目視での発見に依存 |
| **対応方針** | 週次バッチで ROI 計算 → 直近4週 ROI < 90% で Discord アラート |
| **部分対応（2026-05-21）** | `scripts/generate_ab_report.py` が毎週日曜18:00に V1/V2 ROI を Discord ab_test チャンネルへ配信開始。ROI・純利益・勝者バッジを Embed で表示。実測値（直近28日）: V1 ROI=64.1% / 純利益 ¥-2,300,518（単勝 615.6% / 複勝 105.0% が黒字、馬連 47.7% / ワイド 41.2% / 三連単 50.0% が赤字）。**残作業**: ROI < 90% 継続時の専用アラート送信ロジック（job_alert_threshold の新設） |

#### W-025: Web ダッシュボードのオフライン耐性なし

| 項目 | 内容 |
|------|------|
| **ステータス** | ⚪ 保留 |
| **影響** | Next.js サーバーが落ちると UI が完全停止。Discord 通知のみ機能する |
| **対応方針** | 静的 JSON ファイルから直接表示するフォールバックページを追加（将来）|

#### W-028: Discord マルチチャンネル通知の統合管理（→完了）

| 項目 | 内容 |
|------|------|
| **ID** | W-028 |
| **優先度** | 高 |
| **ステータス** | 🟢 完了（2026-05-20） |
| **影響** | DiscordNotifier 直呼び出しが散在し、チャンネル管理・フォールバック制御が困難だった |
| **対応** | `NotificationRouter` 新設。5チャンネル（prediction/system/ev_alert/ab_test/note_draft）を EV 閾値・フォールバック付きで一元管理。全呼び出し元を Router 経由に統一 |
| **影響ファイル** | `src/notification/router.py`（新設）, `src/pipeline/prediction.py`, `scripts/scheduler.py`, `scripts/today_auto_runner.py`, `scripts/post_weekly_note_draft.py` |

---

#### W-029: DB クエリ性能 — 予想・評価クエリの複合インデックス未整備（→完了）

| 項目 | 内容 |
|------|------|
| **ID** | W-029 |
| **優先度** | 中 |
| **ステータス** | 🟢 完了（2026-05-21） |
| **影響** | `predictions`/`race_results`/`training_times` の大量 JOIN クエリが full scan していた（EXPLAIN QUERY PLAN で SEARCH USING COVERING INDEX なし） |
| **対応** | migration #15 で 6 複合インデックス追加。`idx_pred_model_ev` (model_type, ev_score)・`idx_pred_race_model` (race_id, model_type)・`idx_tc_horse_date` (horse_id, date)・`idx_hc_horse_date` (horse_id, date)・`idx_rr_horse_race` (horse_id, race_id)・`idx_pr_pred_hit` (prediction_id, is_hit)。全クエリ SEARCH USING INDEX 確認済み |
| **影響ファイル** | `src/database/schema.py`, `src/database/init_db.py` |

---

#### W-030: EV 特化特徴量の本番統合（→完了）

| 項目 | 内容 |
|------|------|
| **ID** | W-030 |
| **優先度** | 高 |
| **ステータス** | 🟢 完了（2026-05-21） |
| **影響** | オッズから算出できる Shin 真確率・Harville 複勝確率・オッズ異常スコアが FEATURE_COLS に未統合で EV 算出精度に限界があった |
| **対応** | `src/ml/ev_features.py` で 7 特徴量エンジン実装（shin_prob / implied_prob_excess / harville_place_prob / odds_steam_flag / odds_reversal_score / field_strength_ev_adj / ev_rank_in_race）。`FeatureBuilder._add_ev_features()` で `build_race_features()` に統合。try/except ガード付き安全動作。EV 特徴量は FEATURE_COLS 外（買い目サイズ決定専用として機能）。全 69 FEATURE_COLS モデル再訓練完了: HonmeiModel CV AUC=**0.7677** / PlaceModel CV AUC=**0.7293** |
| **影響ファイル** | `src/ml/ev_features.py`（新設）, `src/ml/features.py`, `src/ml/models.py`（FEATURE_COLS 69 列）|

---

#### W-031: V1 vs V2 A/B テスト週次レポート自動化（→完了）

| 項目 | 内容 |
|------|------|
| **ID** | W-031 |
| **優先度** | 高 |
| **ステータス** | 🟢 完了（2026-05-21） |
| **影響** | V1/V2 モデルの成績比較が手動確認に依存しており、どちらのモデルが優れているか週次で定量評価できなかった。A/B テスト稼働直後の旗振りもなく、モデル劣化に気づくタイミングが遅れるリスクがあった |
| **対応** | `scripts/generate_ab_report.py` を完全実装。`_summary_row()` / `_detail_rows()` / `build_ab_report()` / `_send_summary_to_discord()` の4層構造で、対象レース数・ベット数・的中率・ROI・純利益・EV乖離MAE を V1/V2 で比較した Markdown レポートを生成し Discord に自動配信。`scripts/scheduler.py` 日曜18:00 自動実行（`_JOB_SCHEDULES["job_ab_report"] = [(6, 18, 0)]`・取りこぼし4時間窓・`_JOB_MAP_FULL` / `_JOB_MAP` 両対応）|
| **エラーハンドリング仕様** | `_send_summary_to_discord()` は以下の3条件でいずれも例外を外に伝播させない: ① `DISCORD_WEBHOOK_AB_TEST` 未設定 → 静かにスキップ ② `urllib.error.HTTPError`（4xx/5xx）→ `WARNING` ログのみ ③ `OSError`（ネットワーク障害・タイムアウト）→ `WARNING` ログのみ。バッチ全体の継続実行を保証 |
| **実測値（2026-05-21 ドライラン・直近28日）** | V1: 396レース / 8,987ベット / 的中率12.0% / **ROI 64.1%** / 純利益 **¥-2,300,518**。券種別: 単勝ROI 615.6%・複勝ROI 105.0%（黒字）/ 馬単59.6%・三連複87.8%・三連単50.0%・馬連47.7%・ワイド41.2%（赤字）。V2: 0件（V2稼働前期間のため正常ゼロ表示）|
| **Discord Embed 出力** | title: "📊 V1 vs V2 A/B サマリー（直近 N 日）" / fields: V1 ROI・V2 ROI・V1 純利益・V2 純利益・判定（🔵V2優勢/🟠V1優勢/⚖️同等）/ color: Blurple(V2優勢)・Red(V1優勢)・Gray(同等) |
| **テスト** | `tests/scripts/test_ab_report.py` 17件（`TestSendSummaryToDiscord` 8件含む）/ `tests/test_scheduler_state.py` 4件 = 計21件 all PASS。全スイート **486 PASS** |
| **影響ファイル** | `scripts/generate_ab_report.py`（実装）, `scripts/scheduler.py`（ジョブ登録）, `tests/scripts/test_ab_report.py`（17件）, `tests/test_scheduler_state.py`（4件追加）|

---

#### W-032: スケジューラークロスデイ回収バグ（→完了）

| 項目 | 内容 |
|------|------|
| **ステータス** | 🟢 完了（2026-05-23） |
| **優先度** | 高（毎週末の前日バッチ取りこぼしに直結） |
| **影響** | `_recover_missed_jobs()` が `if wd != weekday: continue` で当日の曜日だけを確認していたため、前日（金曜）のジョブ（`job_friday_sync`）が土曜朝の起動時に完全スキップされていた。16時間のリカバリー窓が無意味化し、スケジューラー停止時に金曜夜バッチが必ず取りこぼされる構造的バグ。実際に 2026-05-22 金曜夜バッチが未発火となり手動リカバリーが必要になった |
| **対応方針** | `day_delta in (0, -1)` ループで当日と前日のスケジュールを両方チェック。前日ジョブが catchup 窓内なら当日起動時に即回収 |
| **実装** | `scripts/scheduler.py` — `_recover_missed_jobs()` 関数修正。`for day_delta in (0, -1)` ループ追加・各ループで `target_day.weekday()` を使用・リカバリー後に `break` で重複実行を防止 |

---

#### W-026: 増分学習 `_IsotonicModel.booster_` 属性エラー（→完了）

| 項目 | 内容 |
|------|------|
| **ステータス** | 🟢 完了（2026-05-19） |
| **優先度** | 高（毎レース後に発生・モデル陳腐化リスク） |
| **影響** | W-004 実装時に Isotonic キャリブレーション層を HonmeiModel に導入したが、`incremental.py` の 194/217/218行目が `LGBMClassifier` 生メソッドに直接依存。毎レース後の増分学習が全件スキップされ、モデルが最新データを学習できなかった |
| **対応方針** | `_IsotonicModel`・`_PlattModel` に透過プロキシ3種を追加（`booster_` property / `_Booster` property+setter / `set_params()` メソッド） |
| **実装** | `src/ml/models.py` — 実機E2Eテスト（Booster取得・set_params・_Boosterセット）で全コードパス確認済み |

---

#### W-035: training_hillwork 歴史バックフィル不可（坂路データ空）

| 項目 | 内容 |
|------|------|
| **ID** | W-035 |
| **優先度** | 中 |
| **ステータス** | 🟡 対応中（今後のレースは自動収集、歴史データは取得不可）|
| **影響** | `training_hillwork` テーブルが 0 件のため坂路スピード指数（U score 因子 #15）がデフォルト値しか返せず、umasugi_engine の training_grade スコアに坂路成分が反映されない |
| **根本原因** | ① JVLink WOOD dataspec に WH（坂路）レコードが含まれない（仕様）。② netkeiba 調教ページ（training.html）はレース前にのみ公開されており、レース終了後は 404 を返す。過去30日のレースで全て 404 確認済み（2026-05-24） |
| **対応方針** | 歴史バックフィルは不可。今後のレースは木曜20:00・金曜18:00に `job_training_hillwork_scrape()` でスクレイプし、今週末以降の坂路データから順次蓄積する |
| **実装** | `src/scraper/training_scraper.py`（スクレイパー本体）、`scripts/backfill_training_hillwork.py`（バッチ）、`scripts/scheduler.py` に `job_training_hillwork_scrape()` ジョブ登録済み |

---

## 改善ロードマップ（優先順）

> **2026-05-21 更新: Week1-4 商用化フェーズ完了。本番稼働フェーズ移行。**

```
【完了済み（2026-05-21 本番確定）】
  W-004  大衆心理乖離スコア                    🟢 完了
  W-022  動的EV閾値 + Kelly資金管理            🟢 完了
  W-026  増分学習 _IsotonicModel 修正          🟢 完了
  W-028  Discord マルチチャンネル Router        🟢 完了
  W-029  DB 複合インデックス 6件               🟢 完了
  W-030  EV 特化特徴量統合 + 69列再訓練        🟢 完了
  W-031  V1/V2 A/B テスト週次レポート自動化    🟢 完了
         （Discord Embed サマリー通知・日曜18:00自動配信・例外伝播なし）
  E2E    本番シミュレーション 5.12秒 ALL PASS   🟢 完了

【本番稼働後・次フェーズ（JVLink SID 制約解消次第）】
  W-001  加速力スコア (上がり3F)               🔴 未着手
  W-002  PCI ペース変動指数                    🔴 未着手
  W-020  FukushoElite 本番統合                🔴 未着手
  W-023  破産確率 UI (Monte Carlo)             🟡 対応中（calibration_kelly_audit.py 実装済み・JSON出力あり）
  W-035  training_hillwork 坂路データ蓄積中    🟡 対応中（木金自動収集・今週末以降）
  W-036  モデルキャリブレーション補正実装      🟢 完了（src/ml/calibration.py新設・HonmeiStrategy統合・2026-05-27）
  W-037  運用資金¥100K不足リスク              🟢 完了（動的Kelly実装・1/4Kelly破産率75.97%→0.00%達成・2026-05-27）
  W-046  全モデル横断バックテスト機能      🟢 完了

【中長期（歴史データ大規模取得後）】
  Phase 2-B (W-003/W-008/W-010: 不完全燃焼・馬場脚質・相手関係)
  W-015  ラップタイム DB 格納
  W-016  2025年着順データ補完（netkeiba 一括）
```

---

### W-038: 卍モデル三連複 EV ゲート廃止による損失

| 項目 | 内容 |
|------|------|
| ID | W-038 |
| ステータス | 🟢 完了（2026-05-28） |
| 優先度 | 高 |
| 影響 | 本番実績: 卍×三連複 ROI=46.7%（損失確定）。コード上は `# EVゲート撤廃` として EV < 1.0 の三連複も推奨していた |
| 対応方針 | `ManjiGenerator` に `_TRIO_EV_MIN = 1.0` ゲートを追加して EV < 1.0 の三連複を除外 |
| 担当フェーズ | 実装完了 |

### W-039: バックテスト指標の Kelly 複利膨張

| 項目 | 内容 |
|------|------|
| ID | W-039 |
| ステータス | 🟢 完了（2026-05-28） |
| 優先度 | 中 |
| 影響 | WF バックテストが 2025-08 以降に ROI 1000〜15000% に膨張。経営判断に使えない数字が蓄積している |
| 対応方針 | `--mode flat` フラグ追加で Kelly 複利なしのフラット ROI を算出 |
| 担当フェーズ | 実装完了 |

### W-040: 券種×会場×馬場状態別セグメント分析の欠如

| 項目 | 内容 |
|------|------|
| ID | W-040 |
| ステータス | 🟢 完了（2026-05-28） |
| 優先度 | 中 |
| 影響 | どの会場・馬場・券種で赤字か分からず `_ALLOWED_BET_TYPES` の見直しが勘になっている |
| 対応方針 | `scripts/segment_analysis.py` 新設、月曜07:30 Discord 自動配信 |
| 担当フェーズ | 実装完了 |

---

### W-042: NaN確率/EV → Kelly最大分数誘発バグ（安全性脅威）

| 項目 | 内容 |
|------|------|
| ID | W-042 |
| ステータス | 🟢 完了（2026-05-28） |
| 優先度 | 高 |
| 影響 | LightGBMがNaN確率を返した場合、`float('nan') <= 0.0 = False` でKellyガードをスルーし `calc_kelly_stake` で `int(nan//100)` がValueErrorクラッシュ、`_kelly_bet` では `min(nan, cap)` がPythonでNaNを返し最終的に最低賭け金（¥100）が誤って計上される |
| 対応方針 | `_kelly_bet()` / `calc_kelly_stake()` 先頭に `math.isnan()/isinf()` ガード追加。モデル3種の `predict()` に `np.nan_to_num(nan=0.0)` 補完追加 |
| 担当フェーズ | 実装完了 |

### W-043: 日次損失サーキットブレーカーの欠如

| 項目 | 内容 |
|------|------|
| ID | W-043 |
| ステータス | 🔴 未着手 |
| 優先度 | 高 |
| 影響 | モデルが連続ドローダウン中でも買い目を出し続ける。当日の損失合計が閾値を超えても自動停止しない |
| 対応方針 | `scheduler.py` の予想生成前に当日の `prediction_results` を集計し、損失が初期資金の10%を超えたら Discord 警告 + その日の投票停止フラグを立てる |
| 担当フェーズ | 未実装 |

---

### W-044: today_auto_runner 無限リトライループ

| 項目 | 内容 |
|------|------|
| ID | W-044 |
| ステータス | 🔴 未着手 |
| 優先度 | 中 |
| 影響 | `scripts/scheduler.py:job_today_auto_runner` の内部 `_run_loop()` は例外を捕捉してsleep(30)後に再試行を繰り返すが、最大リトライ数がなく、サブプロセスが常にクラッシュする状況では無限ループになる |
| 対応方針 | `_run_loop()` に `max_crashes=10` カウンタを追加し超過時は Discord 警告 + スレッド終了 |
| 担当フェーズ | 未実装 |

---

### W-045: shap_json カラムのスキーマドリフト

| 項目 | 内容 |
|------|------|
| ID | W-045 |
| ステータス | 🟡 対応中 |
| 優先度 | 低 |
| 影響 | `src/database/init_db.py` の `_migrate_add_shap_json()` でカラムを追加しているが `schema.py` の CREATE TABLE 定義に `shap_json` が含まれていない。フレッシュインストール時はマイグレーション実行までカラムが存在しない可能性がある |
| 対応方針 | `schema.py` の `CREATE TABLE prediction_horses` に `shap_json TEXT` カラムを追記する |
| 担当フェーズ | 次期改修時に対応 |

---

### W-046: 全モデル横断バックテスト機能の欠如

| 項目 | 内容 |
|------|------|
| **優先度** | 🟡 中 |
| **ステータス** | 🟢 完了（2026-05-29） |
| **影響** | 本命・卍・複勝・ALPHA の4モデルを同一データ期間で横断比較できなかった。モデル間のROI・的中率差分が定量評価不可能だった |
| **対応方針** | `scripts/backtest_all_models.py` を新規作成。Train:2024 → Test:2025 の時系列分割でリーク防止。9戦略横断のROI比較テーブル・月別ROI推移・会場別内訳を出力 |
| **対応完了** | 2026-05-29  `scripts/backtest_all_models.py` で統合。commits: b8619a3c(spec)→03f6a83d(plan)→fd68f285(impl) |

---

## チェックリスト（新規開発指示を受けた際に確認）

```
□ このファイルを開いて前回の弱点ステータスを確認したか？
□ 今回の作業で改善された弱点の ステータスを更新したか？
□ 新たに発見した弱点を追加したか？
□ 改善履歴を更新履歴テーブルに記載したか？
```
