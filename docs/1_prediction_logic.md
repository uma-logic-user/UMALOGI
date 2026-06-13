# UMALOGI 予想ロジック設計書

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-06-13 | 【U score 全スキップの根治（W-088・v1.14.4-dev・社長特例で条項2解除）】`src/ml/u_score.py:_calc_competition` が発行する `SELECT race_id, grade FROM races` が `races.grade` 列欠落で例外→`features.py:_add_u_score` のtry/exceptが握り潰し **U score 18因子+u_score が全予想で丸ごとスキップ**していた障害を根治。races に `grade` 列を追加（migration #21）し、`jravan_client._extract_grade` が確定済み競走名から `G1/G2/G3/OP/L/3勝/2勝/1勝/未勝利/新馬` を導出して充填。列追加はDBレベルのため稼働中auto_runnerも次予想から即U score復活（再起動不要）。クラス変化因子(W-011)はgrade空の馬で delta=0 に縮退するがU score本体は正常計算。影響: src/database/schema.py, src/database/init_db.py, src/scraper/jravan_client.py, scripts/backfill_grade.py, tests/test_grade_u_score.py |
| 2026-06-13 | 【サーキットブレーカーの Soft Stop 化（W-087・v1.14.3-dev）】W-043日次損失CB/Pure_EV CB発動時の挙動を「予想生成スキップ」から「アラートのみ・予想生成とDB保存は継続」へ変更（env `CIRCUIT_BREAKER_SOFT_STOP` 既定`1`=soft、`0`でHard Stop）。実弾の単複限定ロック・EV閾値（三連系EV≥1.30/Pure_EV EV≥1.15）は安全/エッジ品質ゲートとして不変。本日CB発動で直前予想が9Rに激減していた問題を解消（→29R）。影響: scripts/today_auto_runner.py, src/pipeline/prediction.py |
| 2026-06-11 | 【過去モデル昇華アンサンブル: 卍EV回帰×三連複（v1.12.0-dev・OOS ROI 110.0%→119.2%・実弾ポリシー不変）】リポジトリ内全過去モデル（ALPHA/ALPHA-Payout/cascade/sandbox/v2系/pre69feat世代）を `scripts/analyze_legacy_models.py` で静的解析（OOS 400R・AUC/現行honmei相関/荒れレースAUC）。**採用=卍(現役EV回帰)のみ**: 全体AUC0.682ながら荒れレースAUC0.754（市場0.612超）・ρ(honmei)=0.33の最大多様性。ALPHA系はρ(market)≈0.96-0.98の市場複製で不採用、cascadeはstage1欠落で再現不能。`src/ml/legacy_ensemble.py` 新設: p_ens=(1−w)·p_honmei+w·(卍EV/odds正規化)（総和保存・w=0で恒等）。重みはtrainフレーム(cutoff前300R)グリッド探索でw=0.4決定→OOS一発検証。**三連複のみ適用**（三連単はw適用で110.0→93.5%劣化のため恒等維持）: OOS合計ROI **110.0%→119.2%**・三連複106.9%→**157.9%**（的中13→26件）・最大1的中除外でも81.8%→**107.8%**（大穴依存低減）。本番配線: `premium_pack.scan_premium_races` が `ManjiScoreSource`（卍pkl直接ロード+FeatureBuilder全頭推論・predictionsの卍保存は買い目3頭のみで使用不能のため）で三連複のみアンサンブル確率から抽出（失敗時は従来確率へフォールバック=恒等）。実弾は単複ロック不変（三連系は購読コンテンツ）。影響ファイル: src/ml/legacy_ensemble.py(新規), src/marketing/premium_pack.py, scripts/backtest_all_tickets.py(--manji-weight/--frame/--ensemble-bet-types), scripts/analyze_legacy_models.py(新規), tests/test_legacy_ensemble.py(新規16件) |
| 2026-06-11 | 【W-078 ポートフォリオ破産シミュレーション＋サブスク向けプレミアム買い目生成（v1.11.0-dev・実弾ポリシー不変）】①`src/ml/bankroll_manager.py` に三連系の高ボラ同時多点向け `simulate_portfolio_ruin`（同一race_id内は排反事象としてカテゴリカル的中・レース間独立・対数複利のベクトル化MC）と `recommend_portfolio_stakes`（P(破産)≤1%を満たす最大Kelly分数を0.05刻み探索→最適ステーク）を追加。実証: 三連系5点ポートフォリオで1/10 Kelly=P(破産)0.0%/分数0.20でP(破産)0.83%<1%。**本番ベットフロー未結線（OOSゲート維持）**。②`src/marketing/premium_pack.py` 新規: 本命model_score→blend_with_market較正→Shin市場確率→`scan_all_tickets`(EV≥1.30)で三連単/三連複のみ抽出→フォーメーション＋allocate_stakes参考ベット額付きMarkdown生成（推定オッズの誠実性注意を必須掲載）。影響ファイル: src/ml/bankroll_manager.py, src/marketing/premium_pack.py(新規), tests/test_bankroll_manager.py(32件), tests/test_premium_pack.py(新規11件) |
| 2026-06-11 | 【全券種EV最適化＋見送り判定モデル新設（v1.9.0-dev・いずれも未結線＝本番挙動不変）】①`src/ml/all_ticket_optimizer.py`: 割引Harville(λ2=0.81/λ3=0.65)による1-3着分布→全券種(馬連/馬単/ワイド/三連複/三連単)EV算出→EV≥1.30の市場歪みのみ抽出→フォーメーション生成。実弾ポリシー(単複ロック)は不変で分析/サブスクコンテンツ用。②`src/ml/no_bet_filter.py`【Fable提案】: レース単位の見送り判定。chaos_score=0.30×オッズエントロピー+0.20×弱い本命+0.20×オーバーラウンド異常+0.20×モデル市場JS乖離+0.10×構造リスク ≥0.42で見送り。確率/EVへの係数操作は構造的に不可(W-071遵守)。結線はPhase2シャドー運用(notes記録のみ)→実測ROI改善確認後にPhase3実弾ゲート昇格(W-079)。詳細: docs/fable_ultimate_upgrade.md。影響: src/ml/all_ticket_optimizer.py(新規), src/ml/no_bet_filter.py(新規), tests/ 33件 |
| 2026-06-11 | 【破産確率最小化バンクロール管理モジュール新設・W-078起票・v1.8.0-dev】`src/ml/bankroll_manager.py` を新規実装。既存 Kelly（pure_ev_edge.kelly_stake 等）に残る4つの数学的穴（①同時ベットの合成Kelly無視 ②静的バンクロール ③破産確率の未定量化 ④ドローダウン時の無減速）を解消する純関数群: `full_kelly`/`effective_bankroll`(確定損益から現在資金・ピークを動的算出)/`drawdown_throttle`(-10%半減・-20%1/4・-30%全停止)/`allocate_stakes`(1点2%キャップ＋日次合計10%超の比例縮約)/`estimate_ruin_probability`(ベクトル化モンテカルロ)/`recommend_kelly_fraction`(P(破産)≤目標の最大分数探索)。**本番ベットフロー(prediction.py)へは未結線**＝既存挙動は完全不変。結線は「現行固定Kelly vs 動的管理」のOOS比較（最大DD/破産確率/最終資産）で優位確認後（W-078・[[feedback_ev_precision_safety_first]]）。EVロジック全体の精査結果は docs/business_architecture_fable.md 領域1参照。テスト tests/test_bankroll_manager.py 25件PASS。影響ファイル: src/ml/bankroll_manager.py(新規), config/automation_daily.yaml(新規・パラメータSSoT) |
| 2026-06-10 | 【市場アンカー型EVブレンド導入・W-066後継・v1.7.2-dev】OOS分析で判明したボトルネック3点を修正。①**EV暴騰の根本解消**: `EV_SANITY_CAP=2.0`（EV を 2.0 に揃えてゲートを素通りさせる逆効果）を廃止し `src/ml/market_blend_calibration.blend_with_market()` へ移行。`P_final = w(odds)·P_model + (1-w)·P_market`（w=min(1,10/odds)、P_market=0.80/odds）で大穴ほど市場確率に収縮。EV 理論上限=0.80×1.5=1.20（旧 2.0 から強化）。100倍超EV中央値 2.46→0.81（理論値に収束）。②**オッズ上限強化**: `TANSHO_ODDS_CEIL=100→30`（全モデル共通）。③**EVゲート緩和**: `TANSHO_EV_MIN=1.2→1.05`（blend後は大穴EV暴騰がないため低閾値でも安全）。バックテスト: 単勝ROI 92.7%(旧)→111.1%(新)/ECE 0.1629→0.0182/後半ROI 81.0%→115.7%。テスト: 12件新規 + 既存93件PASS。影響: `src/ml/market_blend_calibration.py`(新規), `src/ml/manji_calibration.py`(EV_SANITY_CAP削除), `src/ml/bet_generator.py`(TANSHO_ODDS_CEIL/EV_MIN変更), `tests/test_market_blend.py`(新規), `tests/test_ev_calibration_safety.py`(blend移行) |
| 2026-06-02 | 【Challenger 正式昇格デプロイ＋複勝較正器再fit完了・v1.4.0-dev】**Challenger(train_until=2024・OOS複勝108.8%)を `manji_model.pkl` に正式デプロイ**（`retrain_manji_weekend.py --promote-fukusho`）。単勝は `WATCH_ONLY`(投票停止・監視継続)のため副作用を許容。n_races=1424/n_samples=19800。デプロイ後に複勝 Platt 較正器を**新モデルベースで再fit**: ECE 旧Championベース0.0395 → **新Challengerベース0.0271（健全・更に改善）**。較正曲線: ev=1.0→P(複勝圏)0.180 / ev=2.0→0.229 / ev=5.0→0.423（単調増加・実用的レンジ）。バックアップ: `data/backups/manji_model_pre_fukusho_promotion_20260602_121506.pkl`（md5: a90e87f9）。テスト: **93 passed**（WATCH_ONLY維持確認済）。出力: `logs/fukusho_calibration_final_v2.log`。影響: `data/models/manji_model.pkl`(昇格・md5: 1fcd779d), `data/models/manji_place_calibrator.pkl`(再fit), `scripts/retrain_manji_weekend.py`(--promote-fukusho追加)。関連: [[W-067]] [[feedback_ev_precision_safety_first]] |
| 2026-06-02 | 【卍 複勝特化昇格＋較正器分離・v1.4.0-dev】(1)**複勝専用 Platt 較正器の新設**: `manji_calibration.fit_manji_place_calibrator()`（ロジスティック回帰で `ev_score → P(複勝圏=3着内)`）＋`calibrate_place_prob()` を追加。単勝 Isotonic(`manji_win_calibrator.pkl`)とは**独立インスタンス**(`manji_place_calibrator.pkl`)。学習実証=400レース/5,679サンプル・base_rate0.212・**ECE 0.1784→0.0395（健全・ΔECE+0.139）**（`logs/fukusho_calibration_final.log`）。`bet_generator` の卍複勝 confidence を生0.6から **P(複勝圏)較正値**へ置換。(2)**ゲート分離**: `bet_policy` に `MODEL_LIVE_BET_TYPES={卍:{複勝}}` と `WATCH_ONLY_MODELS={卍:{単勝}}` を新設し `is_live_bet` を券種粒度化＋`is_watch_only` 追加。**卍は複勝のみ実弾投票へ昇格・単勝は WATCH_ONLY（予想生成とROI監視は継続・投票せず）**。(3)**ROI実測(誠実報告)**: 昇格対象=卍複勝の現役Champion OOS2025=**90.9%**・ライブ複勝=暫定99.4%/直前84.0% ＝**現Championでは黒字未達(≒トントン)**。黒字(Challenger train_until=2024で108.8%)には**基底回帰の再デプロイが別途必要**（本コミットは較正器分離＋ゲート分離のインフラ整備で、回帰の昇格は未実施＝HOLD据え置き）。閾値: 卍EV選択は `_MANJI_EV_THRESHOLD`(=1.1)系、複勝confidenceはPlatt較正P(複勝圏)。テスト: test_bet_policy 8→10＋test_ev_calibration_safety +3（全90 passed）。影響: src/ml/bet_policy.py, src/ml/manji_calibration.py, src/ml/bet_generator.py, scripts/retrain_manji_weekend.py, data/models/manji_place_calibrator.pkl(新規), logs/fukusho_calibration_final.log, tests/test_bet_policy.py, tests/test_ev_calibration_safety.py。関連: [[W-067]] [[W-048]] [[feedback_ev_precision_safety_first]] |
| 2026-06-02 | 【週末向け 実弾モデル縮退＋卍Challenger再訓練検証・v1.4.0-dev】(1)**実弾配信の縮退**: `bet_policy.LIVE_MODELS` を `{本命, 卍, Alpha-Payout, Pure_EV_Edge, FukushoElite}` → **`{卍, Pure_EV_Edge, FukushoElite}`** に集約。確定実績で100%を下回り続けた **本命(直前ROI88%/暫定60%)・Alpha-Payout(70%)** を実弾から退避（新設 `NON_LIVE_RETIRED` 枠＝予想は生成するが投票対象外・ROI回復時に復帰可）。Oracle/HitFocus は従前どおり `ORNAMENTAL_MODELS`(集客専用)。`is_live_bet` は退避2モデルに False を返す。`health_reporter` の生成0件監視対象も自動的に {卍, Pure_EV_Edge}(広域)＋FukushoElite(選択的)へ縮退。(2)**卍Challenger再訓練(安全検証・本番非昇格)**: `scripts/retrain_manji_weekend.py` を新設し Champion(現役pkl)/Challenger(train_until=2024) を **2025年OOSで比較**。結果＝単勝 Champion72.2%/Challenger68.5%(共に<100%)・**複勝 Champion90.9%→Challenger108.8%(黒字化・+17.9pt)**。保守的ゲート(単勝が現役以上かつ黒字)を満たさず **HOLD**＝`data/models/manji_model.pkl` は**ハッシュ一致で未改変**。較正器(`manji_win_calibrator.pkl`)は再fitせず温存。結果は `logs/training_log_manji_weekend.log`。**重要所見**: 卍単勝はクリーン2025 OOSでは backtest 上 100%割れ（ライブ4-5月の好成績と乖離＝期間/条件差）。複勝側は Challenger が黒字化を示し、複勝特化での昇格余地あり（要 単勝ゲート再設計＋較正再fit）。影響: src/ml/bet_policy.py, tests/test_bet_policy.py, tests/test_health_reporter.py, scripts/retrain_manji_weekend.py(新規), logs/training_log_manji_weekend.log。関連: [[W-059]] [[W-048]] [[feedback_ev_precision_safety_first]] |
| 2026-06-01 | 【W-001 加速力スコア(上がり3F)＋PCI データ基盤・v1.3.0・次期学習用】レース上がり3Fが未保存だった問題に対し、netkeiba 結果列[11]由来の `race_results.last_3f`(additive列) を新設し、`src/features/acceleration.py` で **PCI(西田式準拠: 50×全体平均1F/後半3F平均1F・後傾>50)** と **加速力スコア(レース内z-score・速いほど正)** を並行計算。**本番 FEATURE_COLS(69列)は一切不変**で稼働中v1.2.0モデルに非影響（再学習で明示的に取り込むまで非結線・ガードテストで担保）。影響: src/features/acceleration.py(新規), src/scraper/netkeiba.py, scripts/fetch_race_result.py, src/database/init_db.py |
| 2026-06-01 | 【FukushoElite 期待値ベース本番統合・W-020・v1.2.0】複勝特化 `FukushoElite` を実弾化（`bet_policy.LIVE_MODELS`＋`SELECTIVE_LIVE_MODELS`）。**EV最優先2段ゲート**: ①segment+edge(venue∈{新潟/東京/福島/京都}・≥13頭・edge≥1.1) ②**統計的複勝EV=P(place)×推定複勝オッズ≥`FUKUSHO_ELITE_EV_MIN=1.05`**（`fukusho_ev` 踏襲・勝率/複勝率単独ベット禁止・通過馬ゼロは見送り）。`generate_elite_fukusho_bets` のラベルを `卍`→`FukushoElite` に修正し真の複勝EVを `expected_value` に格納。`prediction._run_fukusho_elite()` を直前パイプラインに結線し `model_type="FukushoElite(直前)"` で保存。狙い=複勝ROI95.4%をEV制御で100%超へ。影響: src/ml/bet_policy.py, src/ml/bet_generator.py, src/pipeline/prediction.py, src/database/init_db.py |
| 2026-06-01 | 【大穴EV暴騰（較正歪み）の安全装置・W-066・v1.1.1】卍 Isotonic 較正器（`calibrate_win_prob`）が `ev_score` のみで `P(win)` を返し **`odds` を考慮しない**ため、大穴に中位馬と同じ確率を付与し EV=P×odds が暴騰（odds=49.7→EV7.2）。推論時の2層安全装置で是正（再学習不要）。①**Layer1**: `src/ml/manji_calibration.py` に `EV_SANITY_CAP=2.0` を追加し `P ≤ EV_SANITY_CAP/odds` で **EV を市場相対に頭打ち**（較正器/フォールバック両経路・卍単勝＋Pure_EV を一括保護。人気馬は非発火で確率不変）。②**Layer2**: `src/ml/pure_ev_edge.py` に `MAX_LIVE_WIN_ODDS=50.0`（`PureEVConfig.max_win_odds`）を追加し実弾単勝の非現実的大穴(>50倍)を棄却。実証: odds=49.7 で EV 7.2→2.0、odds=3.0/8.0 不変。テスト `tests/test_ev_calibration_safety.py` 7件＋影響テスト全PASS。影響: src/ml/manji_calibration.py, src/ml/pure_ev_edge.py |
| 2026-06-01 | 【フェーズA: 自己診断・敗因分析エンジン新設（収益の質向上）】`src/analysis/post_race_analyzer.py` を新規追加。①`extract_missed_races(conn)`=**EV≥1.0で勝負したが的中しなかった**レースを抽出（予想本命馬の実走着順・予想オッズ・人気＋実際の勝ち馬オッズ/人気/馬名＋予想根拠notesを結合。`is_superseded=1`除外・`ev_threshold`/`since`/`limit`引数）。②`build_analysis_prompt()`/`analyze_losses()`=オッズ・人気・結果・根拠を整形し **Claude API（`claude-opus-4-8`＋adaptive thinking）** へ問い合わせ「敗因の3〜5パターン分類＋改善提言」を言語化（クライアント注入可・対象0件はAPI未呼び出し）。③`post_analysis_to_discord()`=`src/notification/discord_notifier.DiscordNotifier`（channel_label="敗因分析"）経由で自動投稿（通知器注入可）。④`run_post_race_analysis()`オーケストレータ＋CLI `py -m src.analysis.post_race_analyzer [--since/--ev/--limit/--dry-run]`。**非干渉設計**: DBは`get_connection()`の**読み取り専用接続(mode=ro)**のみ・新規モジュール追加で稼働中のオートパイロット/watchdog/予想生成に一切非干渉。テスト `tests/test_post_race_analyzer.py` 8件（フェイクClaude/通知注入・実API非接続）・mypy/ruffクリーン・本番DBで5件抽出のread-onlyスモーク確認。影響: src/analysis/post_race_analyzer.py（新規）, src/analysis/__init__.py（新規） |
| 2026-06-01 | 【資金二重性の厳密分離＋A/B昇格しきい値＋卍較正週次自動化（オーナー承認）】(1)**資金二重性整合**: `recommended_bet`=実発注額（Kelly等）、P&L会計/A/Bコストは `bet_policy.flat_cost()`(¥100×点数・`FLAT_UNIT_YEN`)に単一真実源化。`evaluator` の `invested` を `flat_cost(n_tickets)` に明示置換（同値・非recommended_bet）、`prediction._save_predictions` の `recommended_bet` を実発注額(`bet.recommended_bet`)へ戻し、Pure_EV は 1/10 Kelly 実額をrecommended_betに（会計はflat別管理）。これでKelly実額と会計基準の混同を排除。(2)**A/B昇格基準**: `pnl_accounting.AB_MIN_RACES=100`・`AB_ROI_DIFF_THRESHOLD=10.0` を定数定義し、`compute_ab_variants` が Pure_EV の **distinct消化レース数**・残レース・ROI差・`promoted`・`progress_text` を返す。`health_reporter` の🅰️🅱️フィールドに「昇格基準達成までの進捗（あとXR / ROI差Ypt）」を表示。(3)**週次自動再学習**: `job_fit_manji_calibrator`（月03:00）追加（docs/2）。テスト全906 PASS（+test_fit_calibrator_job/promotion/flat_cost）。影響: src/ml/bet_policy.py, src/ml/pnl_accounting.py, src/evaluation/evaluator.py, src/pipeline/prediction.py, src/ops/health_reporter.py, scripts/scheduler.py |
| 2026-06-01 | 【Pure_EV_Edge メインパイプライン完全配線＋W-057シャドーA/B（オーナー承認）】黒字化専用枠 Pure_EV_Edge（単複限定・卍Isotonic較正P×odds・EV≥1.15・1/10Kelly・日次/週次サーキットブレーカー）を本番予想パイプラインへ完全配線。**配線バグ修正**: `_run_pure_ev_edge` の `PureEVConfig(bankroll=)`→`initial_bankroll=`（TypeError）・`PureEVBet.win_prob`→`prob`（AttributeError）の2バグで従来**常に握り潰され生成0**だったのを修正し、実際に生成・**predictions保存**されるようにした。`bet_policy.LIVE_MODELS` に `Pure_EV_Edge` を追加（is_live実弾認識）、DB保存の `recommended_bet` は会計一貫性のため¥100×点数（1/10Kelly額はnotes併記）。欠落していた `NotificationRouter.notify_pure_ev_edge()` を実装（prediction ch＋EV≥1.5でev_alert）。**W-057**: `pnl_accounting.compute_ab_variants()` で「Pure_EV_Edge vs 従来単複(本命/卍/Alpha)」の確定P&L(コスト=payout−profit・is_superseded除外)を比較し、`health_reporter` 日次Discordに🅰️🅱️A/Bフィールド（ROI/純益/勝者）を自動追加。ライブ検証: 従来単複 n=2870/ROI222%/+¥778K に対しPure_EVは蓄積開始(n=0→今後比較)。テスト全875 PASS（+test_w057_shadow_ab/test_pure_ev_wiring/test_bet_policy拡張）。影響: src/pipeline/prediction.py, src/ml/bet_policy.py, src/ml/pnl_accounting.py, src/notification/router.py, src/ops/health_reporter.py |
| 2026-06-01 | 【期待値最大化: 単複限定ロック＋会計真コスト化＋卍較正の統合完了（オーナー承認）】確定実績分析（全体真ROI80%=負け／単複に実エッジ・三連系が全利益を食い潰す）に基づく抜本改修を統合。**(1)単複限定(Phase1)**: 単一真実源 `src/ml/bet_policy.is_live_bet()` 新設（実弾=本命/卍/Alpha × 単勝/複勝のみ）。`bet_generator._ALLOWED_BET_TYPES` を単複ロック、`_apply_roi_filter` を全実弾経路(honmei/manji/hybrid/V2)へ適用・本命三連単の条件付き許可を撤廃。Oracle/HitFocus は Discord で「🎏観賞用・実弾対象外」に降格（note/X集客のみ）。**(2)会計真コスト化(Phase2)**: 新規予想の `recommended_bet` を実購入額(¥100×点数)に統一(`_flat_cost`)、新規 `pnl_accounting.compute_live_roi()`(コスト=payout−profit・is_live絞り)で実弾真ROIを集計。確定実績で **全体−¥1,743,008(ROI80%)→実弾単複のみ+¥778,352(ROI222%)** の反転を実証。**(3)卍較正完了/復帰(Phase3/W-048)**: `manji_win_calibrator.pkl` を確定実績で学習、時系列out-of-sample検証で **飽和34%→0%・ECE=0.0177** を確認し `DISABLE_MANJI_BETS=0` で卍を単複実弾へ復帰。テスト全866 PASS。影響: `src/ml/bet_policy.py`(新規), `src/ml/pnl_accounting.py`(新規), `src/ml/bet_generator.py`, `src/pipeline/prediction.py`, `src/notification/discord_notifier.py`, `scripts/fit_verify_manji_calibration.py`(新規), `.env`/`.env.example` |
| 2026-05-31 | 【卍 confidence キャリブレーション根本修正 P0-3 / W-048（オーナー承認・条項2バイパス）】卍の confidence は `min(raw_prob × 係数(5〜30), 1.0)` で常時1.0飽和→Kelly全額投資→実現ROI26.9%崩壊の主因だった。新規 `src/ml/manji_calibration.py` で **Isotonic Regression** を確定実績(race_results)で学習し（200レース・2861サンプル・base_rate7.0%・ev=2.0→P(win)15.2% と飽和しない現実値）、`ManjiStrategy` の単勝 prob_top を `calibrate_win_prob(ev,odds)`、馬連/ワイド/馬単/三連複 confidence を係数膨張を排した `calibrate_combo_prob(raw)` に置換。学習器が無くても保守フォールバックで1.0飽和を防止。**実弾再開(DISABLE_MANJI_BETS=0)はオーナー判断**: confidenceバグは解消したが、確定P&Lの単複ROI検証後に単複限定で復帰すべき（WFバックテストは信用しない）。テスト全839 PASS。影響: `src/ml/manji_calibration.py`(新規), `src/ml/bet_generator.py` |
| 2026-05-31 | 【推論精度強化 ステップ2-1/2-2（オーナー承認・条項2バイパス）】(2-1)特徴量の直前オーバーライド: `prerace_pipeline()` Step1c の `cached_odds==0` ガードを **直前モードでは解除**し毎回 `fetch_and_save_odds()` を実行、JRA-VAN速報の最新オッズ・馬体重・天候を推論直前に強制反映（馬体重/天候は entries/races を COALESCE/CASE で非破壊上書き）。(2-2)大口・オッズ歪み検知→危険馬フィルタ: 新規 `src/ml/odds_drift.py` が `realtime_odds` 朝→直前変動率を**レース中央値相対**で評価し plunge(大口流入)/abandoned(危険馬)を検知。`prerace_pipeline()` Step4c で危険馬を軸に含む買い目EVを `DANGER_EV_FACTOR=0.5` 減衰しEV<1.0を除外（本命/卍/Oracle/HitFocus/Alpha全モデル・保存前・暫定対象外）。閾値 `ABANDON=0.40`/`PLUNGE=0.25`/`_MIN_FIELD=4`。ライブ実証(東京2回12日8R): 系統+99%シフト下で誤検知12頭→意味のある2頭(11番2.3→1.3倍=大口/10番54→156倍=見限り)。テスト `tests/test_odds_drift.py` 6件＋全824 PASS。影響: `src/ml/odds_drift.py`(新規), `src/pipeline/prediction.py` |
| 2026-05-31 | 【Oracle/HitFocus 予想モデル復活（オーナー特別承認・条項2週末凍結バイパス）】2026-05-23 コミット9c53540bで配線除去されていた Oracle（VirtualOracleStrategy・的中確率最大の三連複/三連単）と HitFocus（HitFocusStrategy・2軸マルチフォーメーション・均等100円/Kelly不使用）を予想パイプライン・Discord通知・note記事採点・UIに再結線。ストラテジークラス本体は削除されておらず純粋に配線のみ復元。`_save_predictions()` に oracle_bets/hit_focus_bets 引数とOracle/HitFocus保存ブロックを復活、`prerace_pipeline` で generate_oracle/generate_hit_focus を呼び出し。note_generatorを4モデル（本命・卍・Oracle・ALPHA）合意採点へ復元。V2 suffix も自動適用（OracleV2/HitFocusV2(直前)）。隔離DB検証で本命/Oracle/HitFocus(直前)＋V2の保存を確認。卍は W-048（confidence=1.0固定・実現ROI26.9%）未解消のため `DISABLE_MANJI_BETS=1` を据え置き（オーナー判断）。テスト全761 PASS（既存失敗5件はFmtCombo/scraping mockで本変更と無関係）。影響: `src/pipeline/prediction.py`, `src/notification/discord_notifier.py`, `src/notification/router.py`, `src/ops/note_generator.py`, `web/src/components/HitHistory.tsx`, `web/src/components/DrillDownAnalytics.tsx` |
| 2026-05-31 | 【W-049 単勝EV抽出＋ワイド多点絞り込み追加（オーナー特別承認・IPAT自動投票は実装せず）】上記フィルタの残課題2点を実装。①単勝EVゲート: `TANSHO_EV_MIN=1.2` 未満の単勝を `_apply_odds_band_filter()` で除外（卍の無条件単勝でもEV<1.2なら除外）。②ワイド多点制限: `_limit_wide_points()` を新設しワイドをEV高い順（combinationsはスコア降順構築）に最大 `WIDE_MAX_POINTS=3` 点へ切り詰め、horse_names(1組2頭)も同期。E2E: 実レース202605021201で完走・卍単勝EV0.88除外・ワイド5点→3点を確認。テスト新規7件（計24件・bet系63件PASS）。影響: `src/ml/bet_generator.py`, `tests/test_bet_precision_filters.py` |
| 2026-05-31 | 【買い目精度向上フィルタ追加（オーナー特別承認スコープA・IPAT自動投票は実装せず手動投票維持）】期待値ロジックにノイズ帯の足切りを3点追加。①#2 単勝オッズ帯フィルタ: `TANSHO_ODDS_FLOOR=1.5`（以下）/`TANSHO_ODDS_CEIL=100.0`（以上）の単勝買い目を除外（過剰人気・大穴ノイズ）。狙うボリュームゾーンは5.0〜30.0倍。②#3 レース選定: `should_skip_race_for_betting()` を新設し新馬戦（「新馬」「メイクデビュー」）・障害戦（「障」「ジャンプ」）を `prerace_pipeline()` Step0bで出馬表/オッズ取得前に見送り（notify_skip→skipped返却）。③#4 ワイド専用EVゲート: ワイド買い目を `WIDE_EV_MIN=1.2`（AIワイド的中率×オッズ）未満で除外。フィルタは `BetGenerator._apply_odds_band_filter()` に集約し V1/V2 全 generate_*（honmei/manji/alpha_trifecta/hybrid）に配線。オッズ未取得時は単勝足切りをスキップ（買い目を消さない）。未実装メモ: 単勝のEV≥1.2抽出とワイド多点絞り込み(1〜3点)はスコープ外（台帳W-049）。テスト: 新規19件＋既存betテスト全PASS（計65件）。影響: `src/ml/bet_generator.py`, `src/pipeline/prediction.py`, `tests/test_bet_precision_filters.py`（新規） |
| 2026-05-24 | 【umasugi_engine Phase3 フル統合完了】3因子追加: ①オッズスパイク検知（_WINDOW=10/15%急落→+0.10/25%→+0.20）を odds_momentum.py に統合。②Discord /paddock コマンドでパドックコメントをDBへ保存・キーワード解析（±5%ブースト）・scorer.py 3%組み込み。③騎手・調教師コース別成績（jockey_stats/trainer_stats/3年バックフィル 1,048+1,817件）をscorerに3%/2%組み込み。ウェイト再編: legacy 0.57→0.50/training_grade 0.08→0.07/+paddock 0.03/+jockey_course 0.03/+trainer_course 0.02。バックテスト ROI: 75.7%(全体)→閾値0.50で**81.1%達成（目標80%超）**。影響: `src/umasugi_engine/scorer.py` `src/umasugi_engine/factors/paddock.py` `src/umasugi_engine/factors/jockey_trainer.py` `src/umasugi_engine/factors/odds_momentum.py` `src/database/schema.py` `src/database/init_db.py` `src/notification/discord_bot.py` `scripts/build_jockey_trainer_stats.py` |
| 2026-05-24 | 【的中報告レポートパイプライン追加】`src/ops/win_report.py` 新設。的中確認後に `data/results/YYYYMMDD/{race_id}_win_report.txt` 生成・Discord 予想ch へ Embed + X投稿テキスト送信・note.com 下書き保存を自動実行。`scripts/fetch_race_result.py` に `_try_publish_win_report()` を追加し `fetch_single_race()`・`fetch_for_date()` から呼び出す。影響ファイル: `src/ops/win_report.py`（新規）, `scripts/fetch_race_result.py` |
| 2026-05-24 | 【umasugi_engine Phase2完了】調教グレード (S〜E, 8%) + オッズモメンタム (5%) を追加。正規化JOINキー(horse_id[:4]+horse_id[4:9])によりtraining_times結合率45.6%達成。ウェイト再編: legacy 0.65→0.57 / turf 0.15 / track 0.10 / grade 0.08 / momentum 0.05 / crowd 0.05。`odds_timeseries` テーブル新設・毎分記録ジョブをschedulerに統合。バックテスト: ROI73.7%(閾値0.50/30日)。影響: `src/umasugi_engine/scorer.py` `src/umasugi_engine/factors/training_grade.py` `src/umasugi_engine/factors/odds_momentum.py` `scripts/record_odds_timeseries.py` `scripts/compute_training_grades.py` |
| 2026-05-24 | 【umasugi_engine Phase1・API・バックテスト完了】`src/umasugi_engine/` に小回り/洋芝/世論分析フィルターを実装。世論分析は legacy の正の相関を逆転させた負の相関（EV × (1−penalty)）で実装。バックテスト結果: Legacy 68.2% → Umasugi 73.6% (閾値0.50/30日)。最適ウェイト確定（turf=0.15/track=0.10）。`/api/compare/[race_id]` エンドポイント新設。影響: `src/umasugi_engine/*` `web/src/app/api/compare/[race_id]/route.ts` `scripts/backtest_umasugi.py` |
| 2026-05-24 | 【AIウマスギフィルター全出力先統合】`prerace_pipeline()` のStep4後に `[AIウマスギ] ROIフィルター適用完了` ログを追加。Discord通知に「🤖 AIウマスギフィルター適用済み」記述 + 本命三連単に「⚡条件付」バッジ。Discord: `notify_prerace_result()` の description/footer 更新。影響: `src/pipeline/prediction.py`, `src/notification/discord_notifier.py` |
| 2026-05-24 | 【ROIフィルター条件付き部分開放】本命モデルの三連単を「完全除外」→「個別EV≥1.5 の場合のみ許可」に変更。`_HONMEI_SANRENTAN_EV_MIN=1.5` 定数を追加。`_apply_roi_filter()` 内で `is_honmei + bet_type=="三連単"` の場合は `_ALLOWED_BET_TYPES` を経由せず EV 値で個別判定。許可時は「条件付き許可」ログを出力。根拠: 5月本番データで本命三連単 ROI272.7%（損失原因は廃止済み Oracle/HitFocus）。影響: `src/ml/bet_generator.py` |
| 2026-05-24 | 【ROIフィルター + 動的Kelly実装】本番実績ROI（2026-04〜05データ）分析結果を反映。`_ALLOWED_BET_TYPES` で本命モデルの馬単/馬連/ワイドを除外、単勝/複勝のみ許可（三連単は個別EV判定）。`get_dynamic_kelly_fraction()` 新設（ROI300%超=1/4Kelly、ROI200-300%=1/5Kelly、ROI100-200%=1/10Kelly、ROI<100%=購入禁止）。`get_model_bet_roi()` でDB照会ベースの ROI 算出関数追加。`BetGenerator._apply_roi_filter()` が `generate_honmei()` `generate_alpha_trifecta()` を自動フィルタリング。影響: `src/ml/bet_generator.py` |
| 2026-05-28 | 【卍モデル三連複EVゲート復活】`ManjiStrategy.generate()` の三連複生成ループに `_TRIO_EV_MIN=1.0` ゲートを追加。本番実績 ROI=46.7% の損失を招いた「確率至上主義・EVゲート撤廃」方針を廃止し、合成EV<1.0の組み合わせを除外する。影響ファイル: `src/ml/bet_generator.py` |
| 2026-05-10 | 初版作成。ALPHA/卍/本命 3モデル並列稼働・三連系生成ロジックを記述 |
| 2026-05-10 | 将来設計案「予測不変性（Prediction Immutability）」を追記 |
| 2026-05-19 | 動的EV閾値（W-022完全対応）実装: `get_dynamic_ev_threshold()` を bet_generator.py に追加。直近28日ROIから自動で1.1/1.2/1.3/1.5を選択（好調/通常/低調/不調）。Kelly資金管理: `calc_qf_kelly_bet()` を追加し、notify_discord.py のQF推奨セクションに推奨ベット額・Kelly%・総資金比を表示。影響ファイル: src/ml/bet_generator.py / scripts/notify_discord.py |
| 2026-05-23 | 【Discord通知完全リアル化】`DISCORD_WEBHOOK_HIT_FLASH` 環境変数を追加し的中速報を専用チャンネルへ分離。直前予想通知に購入単価×点数表示（`¥100×N点=¥XXX`）を追加。`_format_combo_card()` を馬番全表示版に刷新（省略撤廃・軸推奨スマート表記 `【推奨: 三連複流し 軸X - 相手A,B,C】`）。影響: `src/notification/discord_notifier.py` |
| 2026-05-20 | EV>=1.5 の激熱レースを DISCORD_WEBHOOK_EV_ALERT チャンネルへ自動追加送信。NotificationRouter 導入（マルチWebhook 5チャンネル対応）。買い方テンプレート自動送信 (_format_buying_guide)。影響: src/notification/router.py, src/pipeline/prediction.py |
| 2026-05-24 | 【バリデーター自動統合・BANKROLL_OVERRIDE新設】`prerace_pipeline()` の Step 2c として `data_validator.filter_sentinel_horses()` を自動統合（FeatureBuilder直後・モデル推論前）。`get_current_bankroll()` に `BANKROLL_OVERRIDE` 環境変数サポート追加（P&L累積を無視してバンクロールを直接指定、Kelly新規開始時に使用）。`BANKROLL_RESET_DATE` で移行日以降のP&Lのみ集計可能。影響: `src/pipeline/prediction.py` `src/ml/bet_generator.py` `.env.example` |
| 2026-05-24 | 【Kelly資金管理統合 + データバリデーター新設】`_KELLY_TYPE_CAPS` 辞書（券種別バンクロール上限: 複勝3%/馬連1.5%/三連複1%等）を新設。`calc_kelly_stake(bankroll, ev, win_odds, bet_type, n_combos)` 公開関数を追加（formula: f*=(EV-1)/(odds-1), stake=bankroll×min(f*,cap)×0.25）。ManjiGenerator・HonmeiStrategy・AlphaTrifectaStrategy の `recommended_bet` を全面Kelly化（複勝/馬連/ワイド/馬単/三連複）。`src/ml/data_validator.py` 新設: `validate_race_df()/filter_sentinel_horses()/validate_horse_for_axis()` でセンチネルオッズ(≥500)馬を排除。UIに Kelly理論 vs ¥100固定 比較パネル追加（WF 2025年実証ROI対比グラフ）。影響: `src/ml/bet_generator.py` `src/ml/data_validator.py` `web/src/components/FinancialDashboard.tsx` |
| 2026-05-24 | NOTE記事生成に有料仕切りロジック追加: `_build_paywall_separator()` を新設し、1レース目（無料）終了後に `🔒 ここから先は有料エリアです` ブロックを自動挿入。`generate()` の rank==1 後に挿入。影響: `src/ops/note_generator.py` |
| 2026-05-23 | Walk-Forward Backtest 2024-2025 実施: Train=2024-01〜05月 / Test=2025年全12ヶ月。5券種×EV閾値5段階=25パターン成績マトリクス生成（CV AUC=0.7494）。全パターンROI<80%（最高79.3%=単勝EV≥1.5）でランダム水準と判明。推奨ポートフォリオ方針（見送り・改善ロードマップ）を `.claudecode/rules/portfolio_strategy_2024_2025.md` に保存。影響ファイル: src/analysis/walk_forward_backtest_2024_2025.py |

---

## 1. システム概観

UMALOGI は **3モデル完全独立並列稼働** 方式を採用する。
各モデルは異なる目的変数・戦略で動作し、互いの出力を参照しない。

```
レース直前 (発走20分前)
    │
    ├── 🟦 Alpha-Payout モデル  → 複勝 + 三連複/三連単 (EV特化)
    ├── 🟩 卍 モデル           → 三連複/三連単       (回収率特化)
    ├── 🟥 本命 モデル         → 単勝/複勝/馬連      (勝率特化)
    ├── 🟨 Oracle 予想         → 三連系              (参考・非推奨買い目)
    └── 🔶 HitFocus 予想       → 馬連/馬単/三連単    (的中率特化)
```

---

## 2. モデル詳細

### 2-1. 🟥 本命モデル (HonmeiModel)

**ファイル**: `src/ml/models.py` — `HonmeiModel`  
**目的変数**: `is_win` (1着 = 1, 他 = 0)  
**アルゴリズム**: LightGBM (binary classification) + Isotonic Calibration + Platt Scaling  
**キャリブレーション**: OOF (Out-Of-Fold) Isotonic → 予測確率を信頼できる勝率に変換  
**Champion/Challenger**: 末尾20%ホールドアウトで旧モデルとAUCを比較し、上回った場合のみ更新  
**真のAUC**: 約 0.607 (is_win)  

**特徴量** (`FEATURE_COLS`、`src/ml/models.py` L37-93):

| カテゴリ | 特徴量名 | 説明 |
|---------|---------|------|
| 馬能力 | `weight_carried`, `horse_weight`, `horse_weight_diff` | 斤量・馬体重・前走差 |
| 勝率系 | `win_rate_all`, `win_rate_surface`, `win_rate_distance_band` | 通算・馬場別・距離帯別 |
| 直近成績 | `recent_rank_mean` | 直近5走平均着順 |
| カテゴリ | `surface_code`, `sex_code`, `venue_encoded`, `sire_encoded` | 馬場/性別/会場/父馬 |
| レース | `distance`, `gate_number`, `condition_code`, `race_number` | 距離・枠番・馬場状態 |
| 人的要素 | `jockey_code_encoded`, `trainer_code_encoded` | 騎手・調教師エンコード |
| 調教(ウッド) | `tc_4f`, `tc_lap`, `tc_accel_flag`, `tc_4f_diff` | 4Fタイム・ラスト・加速フラグ |
| 調教(坂路) | `hc_4f`, `hc_lap`, `hc_accel_flag`, `hc_4f_diff` | 坂路4Fタイム等 |
| レース内相対 | `*_rank`, `*_zscore` | レース内ランク・偏差値 |
| 当日バイアス | `today_inner_bias`, `today_front_bias`, `today_gate_match` | 当日馬場傾向 |
| オッズ時系列 | `odds_vs_morning`, `odds_velocity` | 朝一比・下落速度 |

**買い目生成** (`HonmeiStrategy`):
- Kelly 基準で推奨投資額を算出（動的バンクロール方式）
- 単勝・複勝・馬連・馬単・三連系を EV > 1.0 でフィルタ

---

### 2-2. 🟩 卍モデル (ManjiModel)

**ファイル**: `src/ml/models.py` — `ManjiModel`  
**目的変数**: `ev_target` = 払戻金 / 馬券代 (回収率直接最適化)  
**アルゴリズム**: LightGBM (regression) + Platt Scaling  
**真のAUC**: 約 0.724 (複勝目的変数)  
**特徴量**: HonmeiModel と同一 FEATURE_COLS を使用  

**買い目生成** (`ManjiStrategy`):
- EV スコア上位馬を軸に三連複・三連単を組む
- Harville 法: `P(A 1着 B 2着 C 3着) = P(A) × P(B)/(1-P(A)) × P(C)/(1-P(A)-P(B))`
- 合成EV = Harville確率 × 推定払戻 / 100

---

### 2-3. 🟦 Alpha-Payout モデル (AlphaPayoutModel)

**ファイル**: `src/ml/alpha_payout_model.py` — `AlphaPayoutModel`  
**目的変数**: `win_payout` (単勝払戻金、実際に得た円額)  
**アルゴリズム**: LightGBM (regression on payout × probability)  
**期待値計算**: `EV = P(win) × estimated_payout / 100` — 市場オッズから払戻推定  

**買い目生成** (`AlphaTrifectaStrategy` — `src/ml/bet_generator.py` L1564):
1. alpha_ev スコア上位5頭を選択
2. **三連複**: 軸馬(1着候補) × 相手上位組み合わせ (最大6点)
3. **三連単**: 軸 → 相手2頭の順列 (最大6点)
4. **複勝**: EV > 1.0 の上位馬 (最大3点)

---

### 2-4. 🟨 Oracle 予想

**実装**: `VirtualOracleStrategy` — 本命モデルのスコアで三連系を生成  
**用途**: 参考表示のみ。Kelly 推奨買い目には含まれない  
**model_type**: "Oracle"

---

### 2-5. 🔶 HitFocus 予想

**実装**: `HitFocusStrategy` — 的中率特化（EV より的中優先）  
**買い目**: 馬単・馬連・三連単  
**特徴**: 上位2頭固定で組み合わせ数を絞る

---

## 3. 予想パイプライン (`src/pipeline/prediction.py`)

```
prerace_pipeline(race_id)
  ├── Step 0: 締め切りチェック (直前モードのみ)
  ├── Step 1: entries キャッシュ確認
  │     └── entries = 0 → netkeiba フォールバック (fetch_entry_table)
  ├── Step 1c: オッズ取得
  │     ├── JVLink realtime_odds
  │     ├── → 空なら netkeiba fetch_odds_from_netkeiba()
  │     └── → fallback: 単勝オッズ推定
  ├── Step 2: 特徴量生成 (FeatureBuilder.build_race_features)
  ├── Step 2b: データ品質チェック (直前のみ)
  │     └── 欠損率 > 閾値 → provisional=True に降格
  ├── Step 3: モデル予測 (HonmeiModel + ManjiModel)
  ├── Step 4: 買い目生成 (BetGenerator.generate / generate_manji)
  │     └── 動的バンクロール × Kelly 計算
  ├── Step 4b: Alpha-Payout 三連系生成 (直前のみ)
  │     └── AlphaTrifectaStrategy.generate()
  ├── Step 5: DB 保存 (predictions テーブル)
  ├── Step 5c: WIN5 生成 (直前のみ・当日最終5レース)
  ├── Step 6: JSON ファイル出力 (data/predictions/{race_id}.json)
  └── Step 7: Discord 通知 (直前のみ)
        └── 3セクション Embed: 🟦 ALPHA / 🟩 卍 / 🟥 本命
```

---

## 4. WIN5 エンジン (`src/ml/win5.py`)

**EV 計算式** (C-01バグ修正済み 2026-05-10):
```
market_prob  = 各レースの (1/win_odds) 正規化積
model_prob   = blend_prob 積 (50% model + 50% market)
estimated_payout = (1 / market_prob) × 0.725 × 100  ← market_prob を使用
EV           = model_prob × estimated_payout / 100
             = (model_prob / market_prob) × 0.725
```

model_prob > market_prob のとき EV > 0.725 でエッジあり。  
旧バグ (model_prob で payout 計算) では EV = 0.725 固定の恒等式になっていた。

---

## 5. 期待値・Kelly 基準

```python
EV = モデル確率 × 推定払戻 / 100
推奨投資 = Kelly * bankroll  (Kelly = (EV - 1) / (オッズ - 1))
```

**判断基準**:
- EV >= 1.0 → 買い目に含める
- EV >= 1.5 → Discord で 🔥 アイコン表示
- EV >= 3.0 → Discord で ⚡ アイコン表示 (JACKPOT カラー)

---

## 6. モデルファイル管理

- 保存先: `data/models/{HonmeiModel,ManjiModel,AlphaPayoutModel}.pkl`
- 世代管理: `data/models/history/` (直近10世代保持)
- Champion/Challenger: AUC が旧モデルを上回った場合のみ置き換え

---

## 7. 将来設計案: 予測不変性（Prediction Immutability）

> **ステータス: 保留 (2026-05-10 時点)。社長承認待ち。コード変更なし。**

### 7-1. 問題の定義

現在の実装では、`predictions` テーブルに保存済みの予想データが後から上書きされる可能性がある。  
具体的には `main_pipeline prerace <race_id>` を複数回実行すると、`combination_json` / `expected_value` / `recommended_bet` が再計算結果で置き換わる。

これにより、Discord に送信した予想と DB の記録が乖離し、事後の回収率・EV 検証が不正確になるリスクがある。

### 7-2. 設計方針（案）

```
原則: prerace 通知後の predictions レコードは UPDATE 不可。再実行時は INSERT IGNORE。
```

実装イメージ:
```python
# predictions テーブルに "locked" フラグを追加
ALTER TABLE predictions ADD COLUMN locked INTEGER NOT NULL DEFAULT 0;

# Discord 通知後に locked = 1 にセット
UPDATE predictions SET locked = 1 WHERE race_id = ? AND model_type = ?;

# 再実行時: locked = 1 のレコードはスキップ
INSERT OR IGNORE INTO predictions (...) VALUES (...);  # UNIQUE制約で自然に弾く
```

または、既存レコードが存在する場合は INSERT を行わず警告を出す保守的な方針でもよい。

### 7-3. 実装コスト試算

| 作業 | コスト |
|------|--------|
| DDL: `locked` カラム追加 + マイグレーション | 小 |
| `bet_generator.py`: INSERT 前に locked チェック | 小 |
| `notify_prerace_result()`: 送信後に locked=1 UPDATE | 小 |
| バックテスト系スクリプトへの影響確認 | 中 |

### 7-4. 注意点

- `self_healing_monitor.py` が predictions = 0 を検出して再生成を試みる場合、locked チェックと競合する可能性がある。修復対象は locked=0 のレコードのみに制限する必要がある。
- バックテスト・simulate_year.py 系は DB を直接書き込む場合があり、locked 制約の例外処理が必要になる。
