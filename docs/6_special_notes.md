# UMALOGI 特記事項・障害対応履歴

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
| 2026-05-17 | 【PID死活監視を psutil 完全改修】auto_runner.pid の重複起動防止ロジックが wmic ベースで脆弱だったため3点根治。①_is_umalogi_process(): psutil で PID 生存＋Python プロセス名＋スクリプト名の3重検証に変更。②ゾンビ PID（死亡プロセス or PID 再利用別プロセス）を自動検知・PIDファイル自動削除・自己修復起動。③atexit + SIGTERM シグナルハンドラーで異常終了時もPIDファイル確実削除。テスト: フェイクPID99999→ゾンビ検出・削除・正常起動確認。正規PID登録後の重複起動→[ABORT]ブロック確認（3テスト全証明済み）。影響: scripts/today_auto_runner.py |
| 2026-05-17 | 【本日データ緊急復旧】auto_runner.pid 残存ゾンビPID(33700)により金曜夜間バッチが沈黙。force_provisional_today.py で全36レース暫定予想を手動生成(393件)→Discord 5分割送信→Next.js クリーンビルド再起動→today_auto_runner.py 起動で監視ループ復旧。根本原因: wmic 旧ロジックが空文字返却時に生存判定してしまう脆弱性（本変更で完全解消）。 |
| 2026-05-17 | 【的中実績UI消失（第2次）→ 根本原因特定・復旧完了】「的中実績がごっそり消えた」との報告。調査: predictions=8,225件・is_hit=1=782件→DB完全無損傷。原因はNext.jsビルドが不完全状態（.next に BUILD_IDなし）でサーバー起動不能。next build → next start で復旧。/api/hits が782件を正常返却確認。月別: 2026-04: 382件、2026-05: 400件。CLAUDE.md 条項4の事故事例を更新（DB直接確認手順・サーバー障害チェックリスト追記）。教訓: 「UIに出ない≠データ消失」→必ずDBを直接COUNT確認してから判断。影響: CLAUDE.md, web/.next(ビルド) |
| 2026-05-15 | 【バックテスト実施・2025年着順データ欠損発見】厳密Walk-Forwardバックテスト実施。2025年 race_results の rank データが著しく欠損（有効行11.5%・その61%がrank=1）→ 本命/卍/PlaceModel のテストデータが勝者のみに偏り結果無効。ALPHA(複勝)のみ有効（ROI=92.6%）。修正要: 2025年全レースの2〜18着着順をnetkeiba等から補完後に再バックテスト。影響: scripts/run_strict_backtest.py(新規), data/strict_backtest_result.json(新規) |
| 2026-05-16 | 【的中実績UI消失 → 表示バグ修正】DBデータは無事（predictions 7,582件・is_hit=1: 782件）。原因: /api/predictions のデフォルトlimit=1000に対し5/16分だけで914件あり、過去の的中データが枠から溢れてUIに表示されなかった。修正: /api/hits エンドポイント新設（is_hit=1のみ全件返却）→ AppShellで別途フェッチしHitHistoryに渡す方式に変更。CLAUDE.md 条項4（DB物理削除禁止・作業前バックアップ義務）追記。影響: web/src/app/api/hits/route.ts(新規), web/src/components/AppShell.tsx, CLAUDE.md |
| 2026-05-16 | 【/api/races・/api/predictions dateフィルタ修正】`?date=` パラメータが SQLクエリで完全無視されていた（WHERE句なし）→ dateFilter 変数を追加しWHERE date=? 条件を組み込み修正。next build → next start 再起動で適用。5/17のhorse_number=NULL汚染行466件もDB削除（日曜朝JVLinkで再生成予定）。影響: web/src/app/api/races/route.ts, web/src/app/api/predictions/route.ts |
| 2026-05-16 | 【sex_age/weight_carried 完全修復・CLAUDE.md §16 Web UI禁止追加】5/2・5/3・5/9・5/10 の race_results で異常sex_age（JVLinkコード '10'/'11'/'21'等）が57件残存→ 該当日 entries を netkeiba から再取得し一括UPDATE。5/17未満の異常sex_age=0件で完全修復。CLAUDE.md §16 に「Web UI 文字化け表示の絶対禁止」ルール（TypeScript判定パターン3種）追記。影響: data/umalogi.db, CLAUDE.md |
| 2026-05-16 | 【race_results 全件文字化け修復】race_resultsのhorse_name/jockey/trainerが全件JVLink CP932ガーベージ→WebUIに反映されていた。entries（netkeiba取得、クリーン）→race_resultsへの一括コピーで修復。対象: 5/16(493行)・5/17(18行)・entriesがある313日分(85738行+113232行jockey/trainer)。UNIQUE制約違反は2ステップrename+merge処理で解消。is_garbled()の検出漏れ(_JVLINK_QUESTION_RE {2,}拡張/_HALFWIDTH_MIXED_RE追加)も同時修正。影響: src/utils/text.py, 直接DB更新 |
| 2026-05-16 | 【ML汚染調査結果】モデル(honmei/manji/place)の特徴量はhorse_id/jockey_code/trainer_codeベースで馬名・騎手名は非使用。win_rate_allもhorse_idで集計。文字化けhorse_nameはML特徴量に影響なし→モデル再学習不要と判定。 |
| 2026-05-16 | 【races.race_name 文字化け修復】5/16・5/17 の races.race_name が計20件文字化け（半角カタカナ+?混在パターン）→ netkeiba fetch_race_results() で全件修復。is_garbled() の検出漏れも修正: _JVLINK_QUESTION_RE を {3,}→{2,}+半角カタカナ(U+FF61-FF9F)対応に拡張、_HALFWIDTH_MIXED_RE 追加。影響: src/utils/text.py |
| 2026-05-15 | 【JVLink文字化け緊急リカバリ】5/16-18エントリー全件文字化け → netkeiba再取得・race_name修復・暫定予想再生成。5/16: entries 493件/predictions 394件、5/17: ヴィクトリアマイル(202605020811)のみ entries 18件/predictions 11件（他35レースは5/16金曜公開予定）。影響: scripts/refetch_entries_from_netkeiba.py 作成 |
| 2026-05-15 | 【エンコーディング根治】文字化け検知・回復・防止を完全実装。①netkeiba.py の EUC-JP ハードコードを廃止→Content-Type優先+mac/Greek誤検知フォールバック(_detect_encoding)。②src/utils/text.py に is_garbled()/try_recover_encoding()/ensure_clean() 追加。③init_db.py の horses INSERT に ensure_clean() バリデーション追加。④scripts/cleanup_encoding.py 作成・実行: DB全件スキャンで7,562件の文字化けを修正（racehorses.horse_name 5,481件/trainer_name 2,066件/races.race_name 15件）。⑤CLAUDE.md §16 追記。影響: src/scraper/netkeiba.py, src/utils/text.py, src/database/init_db.py, scripts/cleanup_encoding.py, CLAUDE.md |
| 2026-05-15 | Sprint A 詳細設計書 作成: docs/sprint_A_design.md。A1(Xシグナル統合)/A2(FukushoElite本番統合)の完全アーキテクチャ・DB設計・実装手順を記述。次回セッションから即実装可能な状態。 |
| 2026-05-12 | Day2 SRE 運用プロトコル策定完了。CLAUDE.md に絶対行動規範3条項追記（予測不変性/平日改修週末凍結/docs同期強制）。HKCU Run自動起動登録済み。影響: CLAUDE.md, scripts/install_autostart.ps1 |
| 2026-05-10 | 初版作成。既知バグ・手動リカバリ手順・クリティカル障害履歴を記述 |

---

## 1. 既知の問題・制限事項

### 1-1. 発走時刻の推定誤差

`today_auto_runner.py` は発走時刻を **R1=10:00 / 30分間隔** で推定している。  
実際の発走時刻はレース条件・前走繰上げ等で前後することがある。  
→ prerace スキップ閾値を「発走後30分」に緩和済み (2026-05-10 修正)。

---

### 1-2. JVLink 32bit 制約

JVLink COM は 32bit プロセスから呼び出す必要がある。  
`py -3.14-32 scripts/_jvlink_force_worker.py` で専用プロセスを起動。  
64bit Python からは `subprocess` 経由で呼び出す。

```python
# 呼び出しパターン (scripts/scheduler.py)
subprocess.run(
    ["py", "-3.14-32", "_jvlink_force_worker.py", "--dataspec", "RACE", "--option", "3"],
    timeout=1800
)
```

---

### 1-3. SQLite WAL モード + 同時書き込み

複数プロセス（scheduler + auto_runner + self_healing_monitor）が同時に DB へ書き込む。  
WAL モード (`PRAGMA journal_mode=WAL`) で並行書き込みを許容している。  
ただし `EXCLUSIVE` ロック待ちでタイムアウトする場合がある (30秒デフォルト)。

---

### 1-4. 予想 EV が常に低い場合

原因候補:
1. モデルが古い (最終再学習日を確認 → `data/models/*.pkl` のタイムスタンプ)
2. 特徴量の欠損率が高い (調教データ未取得)
3. realtime_odds が空 (オッズフォールバックが等確率になっている)

---

## 2. 手動リカバリ手順

### 2-1. 特定日データの完全再構築

```bash
# Step 1: 対象日のデータを削除 (ユーザー承認必須)
py -c "
import sqlite3
conn = sqlite3.connect('data/umalogi.db')
date = '2026-05-10'
race_ids = [r[0] for r in conn.execute(f\"SELECT race_id FROM races WHERE date='{date}'\")]
ph = ','.join('?'*len(race_ids))
for t in ['predictions','realtime_odds','entries','race_results']:
    conn.execute(f'DELETE FROM {t} WHERE race_id IN ({ph})', race_ids)
conn.commit()
"

# Step 2: race_name 修復 (netkeiba から再取得)
py scripts/repair_race_data.py --date YYYY-MM-DD --skip-results

# Step 3: 全レース予想を再生成
for race_id in <race_ids>:
    py -m src.main_pipeline prerace <race_id>
```

---

### 2-2. 払戻データの補完

```bash
# JVLink 経由で再取得
py scripts/repair_race_data.py --date YYYY-MM-DD --payouts

# netkeiba 経由で直接補完
py src/scraper/update_payouts.py --date YYYY-MM-DD
```

---

### 2-3. モデルのロールバック

```bash
# 旧バージョンを確認
ls data/models/history/

# ロールバック (例: HonmeiModel)
copy data\models\history\HonmeiModel_20260505_120000.pkl data\models\HonmeiModel.pkl
```

---

### 2-4. scheduler プロセスが死んでいる場合

```bash
# watchdog が自動再起動するが、手動起動も可能
py scripts/scheduler.py

# watchdog 自体が死んでいる場合
py scripts/watchdog.py
```

---

### 2-5. Discord 通知が届かない場合

1. `.env` の `DISCORD_WEBHOOK_URL` / `DISCORD_SYSTEM_WEBHOOK_URL` を確認
2. `py scripts/test_discord_channels.py` でテスト送信
3. `discord_notifier.py` の `parents[2]` パス確認 (→ プロジェクトルートの `.env` を指す)

---

## 3. クリティカル障害対応履歴

### 2026-05-15: 5/16-18 エントリー文字化け緊急リカバリ

**背景**: JVLink RACE データ取得時に `_str()` の `errors='replace'` により CP932 マルチバイト先行バイト（U+0081-U+009F）が `?`(0x3F) に置換され、5/16-18 全エントリーの horse_name が `?A?h?}?C...` パターンに文字化け。  
**影響**: 5/16 entries 493件・predictions 396件が文字化けデータで生成済み。5/17 entries 18件が文字化け。  
**対応**:
1. 5/16-17 の entries (511件) を全削除。5/16 の ガーベージ predictions (396件) も全削除（未来レース・Discord通知前のため条項1除外）
2. `scripts/refetch_entries_from_netkeiba.py` を作成し netkeiba から 72レース分を再取得 → 成功37件/スキップ35件（5/17 未公開）
3. `repair_race_data.py` で 5/16-17 全 race_name を修復（各36レース・成功率100%）
4. `force_provisional_today.py 20260516` で 5/16 全36レースの暫定予想を再生成 (394件)
5. `force_provisional_today.py 20260517` で 5/17 ヴィクトリアマイル(202605020811)のみ生成 (11件)

**恒久対策**: `src/utils/text.py:ensure_clean()` による保存前文字化け検知・回復を実装済み。

---

### 2026-05-10: DB深部クレンジング実施

**背景**: JVLink CP932文字化けにより、5/10 の全36レースの race_results に horse_number=NULL が混入。  
**対応**:
1. 5/10 の predictions(780件)・realtime_odds(503件)・entries(503件)・race_results(1010件) を全削除
2. race_name 26件を netkeiba から再取得・修復
3. 全36レース × 3モデルの予想を再生成 (776件)

---

### 2026-05-10: C-01 WIN5 EV恒等式バグ修正

**バグ**: `estimated_payout` の計算に `model_prob` を使用 → EV = 0.725 固定の恒等式  
**修正**: `market_prob`（win_odds の逆数正規化）を使用するよう変更  
**影響ファイル**: `src/ml/win5.py` — `_enumerate_combinations()` L308  
**証明**: 修正後テストで EV 範囲 7.46〜709.31 (73,926 ユニーク値) を確認

---

### 2026-05-04: RTDパターン / rank汚染バグ修正

**バグ**: `race_results.rank` に 20, 30, ...90 などの不正値が混入  
**原因**: HR (払戻) レコードが race_results に誤挿入されていた  
**修正**: RTD パターンマッチングを厳格化、HR レコードを race_results に書かないよう修正

---

### 2026-05-04: _save_se() cat='7' NULL上書きバグ修正

**バグ**: cat='7' の SE レコード処理時に horse_name が NULL で上書きされていた  
**修正**: UPSERT 時に horse_name IS NOT NULL の条件を追加  
**影響**: horse_name UNIQUE 違反も同時に解消

---

### 2026-05-04: netkeiba 払戻パーサー "250円" 形式バグ修正

**バグ**: "250円" の形式の払戻金額がパース失敗 → 0円として保存  
**修正**: `re.sub(r'[^\d]', '', s)` で数字のみ抽出するよう変更

---

### 2026-05-04: Isotonic OOF バグ修正

**バグ**: fold 0 が val として使われず、x=0 のダミー点が混入 → 全スコアが均一化  
**修正**: fold 0 を val として使う実装に修正 (再訓練が必要)

---

### 2026-05-03 Discord通知パス修正

**バグ**: `discord_notifier.py` が `parents[3]` (→ `C:\dev\.env`) を参照していた  
**修正**: `parents[2]` (→ `C:\dev\horse-racing-ai\.env`) に変更

---

## 4. 設定ファイル一覧

| ファイル | 説明 |
|---------|------|
| `.env` | シークレット (Git 管理外) |
| `CLAUDE.md` | AI 開発ガイドライン |
| `data/scheduler_state.json` | scheduler のジョブ実行履歴 |
| `data/auto_runner.log` | today_auto_runner の稼働ログ |
| `data/scheduler.log` | scheduler の稼働ログ |
| `data/backup/` | 日次 DB バックアップ |
| `data/models/history/` | モデル世代履歴 (直近10世代) |

---

## 5. デバッグ用コマンド集

```bash
# 本日のデータ状況確認
py -c "
import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
conn = sqlite3.connect('data/umalogi.db')
date = '2026-05-10'
print('races:', conn.execute(f\"SELECT COUNT(*) FROM races WHERE date='{date}'\").fetchone()[0])
print('entries:', conn.execute(f\"SELECT COUNT(*) FROM entries WHERE race_id IN (SELECT race_id FROM races WHERE date='{date}')\").fetchone()[0])
print('predictions:', conn.execute(f\"SELECT COUNT(*) FROM predictions WHERE race_id IN (SELECT race_id FROM races WHERE date='{date}')\").fetchone()[0])
print('realtime_odds:', conn.execute(f\"SELECT COUNT(*) FROM realtime_odds WHERE race_id IN (SELECT race_id FROM races WHERE date='{date}')\").fetchone()[0])
"

# 文字化けチェック
py -c "
import sqlite3, re
conn = sqlite3.connect('data/umalogi.db')
GARBLED = re.compile(r'\?[^\s\?]{0,4}\?')
rows = conn.execute(\"SELECT race_id, race_name FROM races WHERE date='2026-05-10'\").fetchall()
garbled = [(r,n) for r,n in rows if n and GARBLED.search(n)]
print(f'文字化け: {len(garbled)}件')
"

# Discord テスト送信
py scripts/test_discord_channels.py

# self_healing_monitor 1回実行
py scripts/self_healing_monitor.py --once --date 20260510

# prerace 手動実行
py -m src.main_pipeline prerace 202605020611
```
