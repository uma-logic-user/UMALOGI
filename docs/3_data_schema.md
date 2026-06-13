# UMALOGI データ設計書（JRA-VAN / netkeiba ハイブリッド）

## 更新履歴（Changelog）

| 2026-06-08 | 【W-076 騎手/調教師コードベース結合（v1.6.2-dev）】`race_results` と `entries` に `jockey_code`/`trainer_code TEXT` を additive 追加。氏名はSE8バイト=4文字切り詰め＋文字化けでマスタ結合できないため、SEのコード(jockey=slice(296,301)5桁/trainer=slice(85,90)下5桁・先頭は東西区分)を直接保存しコードベース結合へ移行。`v_race_mart` のjockey/trainer結合も将来コード化候補。backfill: `scripts/backfill_se_codes_w076.py`(冪等)。実証: backfill行のマスタ結合 騎手98.9%/調教師99.4%。影響: src/scraper/jravan_client.py, src/database/init_db.py, src/database/schema.py, src/ml/features.py |

| 2026-06-07 | 【W-074 競走馬マスタ(UM)パーサ全面是正＋racehorses.birth_date 追加（v1.6.0-dev）】`racehorses` が `_UM_*` スライス誤配置で全列ゴミ化し horse_id が race_results と結合0件だった破損を、実 UM バイト(1609B)で realign 修正（horse_id[11:21]/生年月日[38:46]/馬名[46:82]/性別[200:201]/毛色[202:204]/3代血統[204:434]）。composite key 用に `racehorses.birth_date TEXT`（"YYYY/MM/DD"）を additive migration（`extend_db_schema` 内 ALTER）。修正パーサでUM再取り込みし racehorses を正データへ再構築。馬ID紐付けマスタープロトコル（`src/database/check_integrity.py`／`upsert_horses_data.py`／`scripts/monthly_horse_cleanse.py`）を新設。⚠️ KS/CH マスタ・NAR SE 保存失敗は W-075 として別途（未対応）。影響: src/scraper/jravan_client.py, src/database/check_integrity.py(新規), src/database/upsert_horses_data.py(新規), scripts/monthly_horse_cleanse.py(新規), racehorses(birth_date列) |

| 日付 | 変更内容 |
|------|---------|
| 2026-06-13 | 【SE保存upsertのON CONFLICT句修正（W-086・v1.14.2-dev）】`_save_se` の race_results 新規INSERTのconflict targetを `ON CONFLICT(race_id, horse_number) WHERE horse_number IS NOT NULL` に修正。部分UNIQUEインデックス `idx_rr_unique_horsenum` とWHERE句が一致しないとSQLiteが保存を全拒否する（2026-06-07 c36ab38f当初からの構造バグでJVLink経由の新規出走馬保存が全滅していた）。影響: src/scraper/jravan_client.py |
| 2026-06-01 | 【last_3f/distance 実バックフィル＋distance欠損補填（v1.4.0-dev）】`bulk_backfill_features` を実DBへ実行し計100レース/約1,480馬行の `race_results.last_3f` を充填（冪等COALESCE）。**重大発見**: `races.distance` がDB全体でほぼ0（2024-2026は全0・PCI算出不能）。`bulk_backfill_features._upsert_race_meta` を追加し netkeiba 取得時に `races.distance`(0/NULL時のみ)/`surface` を非破壊補填（50レースで distance>0 化）。distance系の根治は2024後半のJVLink再取得（G-Tune PCで実施）が必要。影響: scripts/bulk_backfill_features.py |
| 2026-06-01 | 【データ整合性・バックフィル基盤（v1.4.0-dev）】`scripts/check_jravan_integrity.py` が `races`(スケジュール) と `race_results`(rank確定) の月粒度充足をスキャンし欠損を検出（**実測: 2024-07/08 結果ゼロ・全期間 coverage 75.3%**）。`scripts/bulk_backfill_features.py` が `race_results.last_3f IS NULL AND rank IS NOT NULL` の確定レースを期間抽出し netkeiba 再取得→`COALESCE(excluded.last_3f, last_3f)` で冪等保存（既存値非破壊）。DBスキーマ変更なし（last_3f 列は v1.3.0 で追加済）。影響: scripts/check_jravan_integrity.py(新規), scripts/bulk_backfill_features.py(新規) |
| 2026-06-01 | 【W-001 加速力スコア基盤: race_results に上がり3F列を追加（v1.3.0・additive）】`race_results.last_3f REAL`（nullable）を `_migrate_race_results_new_columns` に冪等追加。netkeiba 結果ページ列[11]「上がり」由来で、`netkeiba.HorseResult.last_3f` → `fetch_race_result._upsert_race_results` で `COALESCE` 保存（既存値非破壊・未取得は NULL）。次期学習用の加速力スコア/PCI計算（`src/features/acceleration.py`）が参照する。**本番 FEATURE_COLS(69列)は不変**で稼働中v1.2.0モデルに非影響。既存レコードは NULL のため次回結果取得から自動充填（実バックフィルは次段）。影響: src/database/init_db.py, src/scraper/netkeiba.py, scripts/fetch_race_result.py, src/features/acceleration.py(新規) |
| 2026-05-31 | 【月末メンテ: スキーマ2列追加＋オッズ時系列単一ソース化（オーナー承認・条項2バイパス）】migration #19 `races.post_time TEXT`（実発走時刻 HH:MM・netkeiba `_parse_race_header` 捕捉→`update_race_details_from_entry` 保存・空時は推定にフォールバック）。migration #20 `predictions.is_superseded INTEGER DEFAULT 0`（直前再推論の論理無効化・`evaluator` が `=0` のみ評価しROI二重計上を防止・条項1例外）。**オッズ時系列単一ソース化(W-055)**: `realtime_odds` を唯一の真実のソースに統一し、旧 `odds_timeseries`（realtime_odds のコピー・本日0/24で死亡）依存を解消。`record_odds_timeseries.py` は発走前レースへ `fetch_and_save_odds` で実取得、`odds_momentum.py` は realtime_odds 参照へ。影響: src/database/init_db.py, src/database/schema.py, src/scraper/entry_table.py, src/pipeline/scraping.py, scripts/record_odds_timeseries.py, src/umasugi_engine/factors/odds_momentum.py, src/evaluation/evaluator.py |
| 2026-05-31 | 【JRA-VAN 速報の馬体重・天候馬場をリアルタイム化（規則11拡張・W-054続き）】速報ワーカー `_jvrt_odds_worker.py` を拡張し、1 JVLink セッションで オッズ(0B30/O1)＋**馬体重(0B11/WH)**＋**天候馬場(0B12/RA)** を取得。馬体重WHレイアウトをライブ実証（バイト基準: 馬データ開始35・stride45・馬番[0:2]/馬名[2:38]/体重[38:41]/符号[41:42]/差[42:45]、例482kg-2・492kg+12）。天候馬場は0B42がオッズを返すため不可と判明→**0B12のRAレコードを既存 `parse_record` で再利用**。`scraping.fetch_and_save_odds` の Stage0 で `_apply_jvrt_weight_weather()` により entries.horse_weight/horse_weight_diff・races.weather/condition を **値があるときのみ** 反映（NULL/空では既存値を上書きしない・fail-safe）。馬体重はライブ取得確認済、天候馬場はレース前は未設定（空）で値が入り次第反映。影響: scripts/_jvrt_odds_worker.py, src/scraper/rtd_reader.py(parse_wh_realtime追加), src/pipeline/scraping.py |
| 2026-05-31 | 【JRA-VAN 速報オッズの COM 一次経路を新設（規則11オッズ取得ルール変更・JVLink真の一次化）】リアルタイム単勝オッズを TARGET frontier の `.rtd` キャッシュ依存ではなく `JVRTOpen("0B30")` で JRA-VAN から直接取得する経路を実装。`fetch_and_save_odds` のフォールバックを「**Stage0 JRA-VAN速報(JVRTOpen) → Stage1 RTDキャッシュ → Stage2 netkeiba → Stage3 DB既存**」の4段に変更（全段fail-safe）。32bit COM のため 64bit 本番からは `scripts/_jvrt_odds_worker.py` を `py -3.14-32` の subprocess で呼び JSON 受け取り。速報O1レコードは .rtd 版とヘッダ長が異なり [37:39]=出走頭数・単勝配列start=43・entry8(馬番2+オッズ×10 4+人気2)。本日5/31ライブ実証済（16頭・5番1.9倍1番人気）。realtime_odds テーブルのスキーマ変更なし。影響: src/scraper/jravan_client.py(rt_open追加), src/scraper/rtd_reader.py(build_rt_race_key/parse_o1_realtime追加), src/pipeline/scraping.py, scripts/_jvrt_odds_worker.py(新規) |
| 2026-05-31 | 【netkeiba スクレイピング堅牢化（取得ルール変更・規則11冗長化強化）】本番ログで netkeiba が 503×201/429/403/404 を多発し着順取得が3回リトライ後に失敗していたため、共通HTTPクライアント `src/scraper/http_client.py` を新設。①UAローテーション+ブラウザ完全ヘッダ（Accept/Referer/Sec-Fetch等）②プロセス全体のグローバルレート制限（`NETKEIBA_LIMITER`・既定1.2秒間隔+ジッタ、`NETKEIBA_MIN_INTERVAL`/`NETKEIBA_JITTER`で調整可）で並列スレッドの自己DoSを防止③Retry-After尊重（429/503）④ステータス別バックオフ（429/503は最低5秒×指数、403はUAローテで再試行、404は即中断）。`netkeiba._fetch_html` と `entry_table._http_get`（tenacity撤去）が本ロジックを共有。JVLink一次→netkeiba二次のフォールバックは従来どおり `fetch_single_race`/`fetch_for_date` で機能。影響: src/scraper/http_client.py(新規), src/scraper/netkeiba.py, src/scraper/entry_table.py, scripts/fetch_race_result.py(冒頭の誤コメント修正) |
| 2026-05-27 | 【prediction_horses.shap_json 追加 (migration #18)】`prediction_horses` に `shap_json TEXT` 列（NULL許容）を追加。SHAP 上位10特徴量の寄与度を `{"feature_name": value, ...}` 形式で保存。推論時に `shap.TreeExplainer(booster)` で計算、`build_shap_map()` で `horse_number → shap_json` に変換。新規予測から順次記録（既存レコードはバックフィルせず NULL のまま UI で「SHAP未計算」表示）。影響: `src/database/init_db.py`, `src/ml/shap_explainer.py`（新規）, `src/pipeline/prediction.py` |
| 2026-05-25 | 【マルチ券種オッズ対応 (migration #17)】multi_odds テーブル追加（枠連/馬連/ワイド/馬単/三連複/三連単）。UNIQUE(race_id, bet_type, combination, recorded_at) でスナップショット履歴を保持。MultiOddsEntry dataclass + insert_multi_odds() ヘルパーを init_db.py に追加。src/scraper/multi_odds_scraper.py 新規実装（UMALOGI_MOCK_MULTI_ODDS=1 でモック動作）。Next.js API /api/races/[race_id] に multi_odds フィールド追加。TypeScript 型 MultiOddsEntry/MultiOddsSnapshot を race.ts に追加。影響: src/database/schema.py, src/database/init_db.py, src/scraper/multi_odds_scraper.py, web/src/types/race.ts, web/src/app/api/races/[race_id]/route.ts |
| 2026-05-24 | 【Phase3 DBスキーマ拡張 (migration #16)】paddock_notes / jockey_stats / trainer_stats テーブルを追加。paddock_notes: race_id+horse_number+comment+boost_factor+source。jockey_stats/trainer_stats: name+venue+surface の PRIMARY KEY で win_rate/last_30d_win_rate を管理。スクリプト scripts/build_jockey_trainer_stats.py で過去3年バックフィル実行済み（jockey 1,048件/trainer 1,817件）。影響: src/database/schema.py, src/database/init_db.py |
| 2026-05-24 | 【training_hillwork 取得方針確定】JVLink WOOD dataspec に WH（坂路）レコードが含まれないことを診断スクリプト（scripts/diagnose_wood_wh.py）で確認済み。代替として netkeiba 調教ページスクレイパー（src/scraper/training_scraper.py）を実装。ただし調教ページはレース前のみ公開のため歴史バックフィルは不可。scheduler.py に木曜20:00・金曜18:00のジョブ（job_training_hillwork_scrape）を追加し、今週末以降のレースから収集開始。影響: src/scraper/training_scraper.py, scripts/backfill_training_hillwork.py, scripts/scheduler.py |
| 2026-05-24 | 【entries.horse_weight バックフィル】scripts/backfill_horse_weight.py で race_results → entries への馬体重コピーを実行、8,638件更新。4月以降カバレッジ 99〜100% 達成。src/ml/features.py に race_results フォールバック COALESCE を追加。src/scraper/entry_table.py に _find_weight_cell()（3戦略セレクタ）を追加。影響: scripts/backfill_horse_weight.py, src/ml/features.py, src/scraper/entry_table.py |
| 2026-05-20 | 【EV 複合インデックス Phase 2 適用】schema.py DDL_STATEMENTS に 6 本追加（idx_pred_model_ev/idx_pred_race_model/idx_tc_horse_date/idx_hc_horse_date/idx_rr_horse_race/idx_pr_pred_hit）、init_db.py マイグレーション #15 _migrate_add_ev_indexes() 追加・実行済み。EXPLAIN QUERY PLAN でフルスキャン 0 件確認。影響: src/database/schema.py, src/database/init_db.py |
| 2026-05-20 | 【insert_prediction バリデーション拡張】`_VALID_BASE_TYPES` に `卍V2`/`本命V2`/`OracleV2`/`HitFocusV2` を追加。`reconcile.py` モデル成績再集計を `("卍","卍V2","本命","本命V2")` に拡張。影響: `src/database/init_db.py`, `src/ml/reconcile.py` |
| 2026-05-18 | 【x_accounts / x_signals テーブル追加】X 世論分析 Phase A として 2 テーブルを schema.py に追加・DB 作成確認済み。x_accounts: 監視アカウントマスタ（screen_name/weight/hit_rate_30d 等）。x_signals: 予想家ポストから抽出した馬番シグナル（tweet_id UNIQUE/race_id FK/signal_type/confidence/parsed フラグ）。インデックス 4 件追加。影響: src/database/schema.py |
| 2026-05-13 | win5_results テーブル追加: race_date(UNIQUE)/race_ids(JSON)/winning_numbers(JSON)/payout。マイグレーション: init_db._migrate_create_win5_results()。影響: src/database/schema.py, src/database/init_db.py |
| 2026-05-10 | 初版作成。ハイブリッド補完ルール・全テーブルスキーマを記述 |

---

## 1. データ取得戦略（ハイブリッド補完ルール）

JRA-VAN（JVLink）を一次ソース、netkeiba を二次（フォールバック）として運用する。

```
【原則】 JVLink が公式の真実。netkeiba は JVLink 失敗時の補完手段。

オッズ:
  JVLink realtime_odds (RTD) → realtime_odds テーブル空 → netkeiba fetch_odds()

エントリー/出走馬:
  JVLink SE レコード → entries テーブル空 → netkeiba fetch_entry_table()

確定結果/払戻:
  JVLink RACE (SE/HR レコード) → 未取得 → netkeiba update_payouts.py

レース基本情報:
  JVLink RA レコード → race_name/distance欠損 → netkeiba repair_race_data.py

調教タイム:
  JVLink WOOD (TC/HC) → 欠損許容 (fillna(-1) でモデルは継続動作)
```

---

## 2. JVLink データ仕様

### 2-1. データスペック

| DATASPEC | 内容 | 取得タイミング |
|---------|------|--------------|
| RACE | 出馬表(RA/SE)・成績(HR)・払戻 | 金曜夜・土日17:30後 |
| WOOD | 調教タイム(TC)・坂路(HC) | 土日07:30 |
| BLOD | 血統(BT)・繁殖馬 | 初期取込のみ |

### 2-2. オプション

| オプション | 意味 |
|-----------|------|
| OPT_NORMAL (1) | 差分取得（ポインタ以降） |
| OPT_STORED (2) | ローカルキャッシュから読込 |
| OPT_SETUP (3) | サーバーから全件強制取得 |

### 2-3. CP932 文字化け対策 (CLAUDE.md §10)

JVLink COM は CP932 バイト列を Latin-1 として返すことがある。

```python
# _to_bytes() で変換（src/scraper/jravan_client.py）
if ord(ch) <= 0xFF:
    byte = ch.encode('latin-1')   # CP932 リードバイトをそのまま保持
else:
    byte = ch.encode('cp932')     # Pattern 2 (正規 Unicode 日本語)

# 保存前スクリーニング（src/utils/text.py）
_GARBLED = re.compile(r'\?[^\s\?]{1,4}\?')  # ?X? パターン検出
sanitize_str(s)  # 制御文字 [\x00-\x08...] を除去
```

---

## 3. DB スキーマ（`data/umalogi.db`）

接続: `src/database/init_db.py` の `init_db()` 経由  
設定: `PRAGMA foreign_keys = ON` / `PRAGMA journal_mode = WAL`

### 3-1. 主要テーブル

#### `races` — レース基本情報

| 列名 | 型 | 説明 |
|-----|----|------|
| race_id | TEXT PK | 12桁 (YYYY場RR開催日2桁R番号2桁) |
| race_name | TEXT | レース名 (garbled 検査対象) |
| date | TEXT | 開催日 YYYY-MM-DD |
| venue | TEXT | 開催場 |
| race_number | INTEGER | R番号 |
| distance | INTEGER | 距離 (m) |
| surface | TEXT | 芝/ダート/障害 |
| weather | TEXT | 天候 |
| condition | TEXT | 馬場状態 |
| track_direction | TEXT | コース方向 |

#### `race_results` — 出走・着順結果

| 列名 | 型 | 説明 |
|-----|----|------|
| race_id | TEXT FK | races.race_id |
| horse_id | TEXT | 馬 ID |
| horse_name | TEXT | 馬名 |
| horse_number | INTEGER | 馬番 (NULL = CP932文字化け) |
| rank | INTEGER | 着順 (NULL/0 = 競走中止) |
| finish_time | TEXT | タイム |
| win_odds | REAL | 単勝オッズ |
| popularity | INTEGER | 人気順 |
| horse_weight | INTEGER | 馬体重 |
| horse_weight_diff | INTEGER | 前走比 |

#### `race_payouts` — 確定払戻

| 列名 | 型 | 説明 |
|-----|----|------|
| race_id | TEXT FK | |
| bet_type | TEXT | 単勝/複勝/馬連/ワイド/馬単/三連複/三連単/返還 |
| combination | TEXT | 馬番組み合わせ (例: "3-7-12") |
| payout | INTEGER | 払戻金額 (100円単位) |

**注意**: `bet_type='返還'` は対象馬券を 100円返還として処理。  
**注意**: 同着 (dead heat) は `rank` が同値の複数行。払戻は分割。

#### `predictions` — 予想バッチ

| 列名 | 型 | 説明 |
|-----|----|------|
| race_id | TEXT FK | |
| model_type | TEXT | Alpha-Payout(直前)/卍(直前)/本命(直前)/Oracle/HitFocus |
| bet_type | TEXT | 単勝/複勝/馬連/三連複/三連単 |
| confidence | REAL | モデル確信度 |
| expected_value | REAL | 期待値 (EV) |
| recommended_bet | REAL | 推奨投資額 (円) |
| combination_json | TEXT | JSON 配列の馬番組み合わせ |
| notes | TEXT | 補足情報 (EV・Harville確率等) |

#### `realtime_odds` — リアルタイムオッズ

| 列名 | 型 | 説明 |
|-----|----|------|
| race_id | TEXT FK | |
| horse_number | INTEGER | 馬番 |
| horse_name | TEXT | 馬名 |
| win_odds | REAL | 単勝オッズ |
| place_odds_min | REAL | 複勝オッズ下限 |
| place_odds_max | REAL | 複勝オッズ上限 |
| snapshot_time | TEXT | 取得時刻 |

#### `entries` — エントリー/出走馬

| 列名 | 型 | 説明 |
|-----|----|------|
| race_id | TEXT FK | |
| horse_number | INTEGER | 馬番 |
| gate_number | INTEGER | 枠番 |
| horse_id | TEXT | 馬 ID |
| horse_name | TEXT | 馬名 |
| jockey_id | TEXT | 騎手 ID |

### 3-2. マスタテーブル

| テーブル | 説明 | JVLink DIFN |
|---------|------|-----------|
| `horses` | 馬マスタ (血統 sire/dam/dam_sire) | UM |
| `racehorses` | 競走馬マスタ | UM |
| `jockeys` | 騎手マスタ | KS |
| `trainers` | 調教師マスタ | CH |
| `breeding_horses` | 繁殖馬マスタ | BLOD:BT |
| `training_times` | 調教タイム | WOOD:TC |
| `training_hillwork` | 坂路調教 | WOOD:HC |

### 3-3. ビュー

| ビュー | 説明 |
|-------|------|
| `v_race_mart` | AI学習用フラットビュー (63列・全テーブル結合済) |
| `v_analytics` | 予想精度分析ビュー |
| `v_prediction_summary` | 予想サマリー (model_type × bet_type 別集計) |
| `v_model_annual_summary` | 年度別モデルパフォーマンスサマリー |

---

## 4. データフロー図

```
JVLink COM (32bit専用)
  │  ← py -3.14-32 _jvlink_force_worker.py
  ├── RACE → races / race_results / race_payouts / entries
  ├── WOOD → training_times / training_hillwork
  └── BLOD → breeding_horses / horses

netkeiba (フォールバック・スクレイピング)
  │  ← src/scraper/netkeiba.py
  │  ← src/scraper/entry_table.py
  ├── レース基本情報 → races (race_name / distance / surface 補完)
  ├── 出走馬 → entries
  ├── オッズ → realtime_odds
  └── 払戻 → race_payouts
```

---

## 5. 文字化け対策 チェックリスト

保存前に必ず通過:
- [ ] `sanitize_str()` で制御文字除去
- [ ] `_GARBLED.search(s)` で `?X?` パターン検出・ワーニング
- [ ] バッチ完了後に `races.race_name LIKE '%?%'` で残留確認
- [ ] `repair_race_data.py --date` で事後修復
