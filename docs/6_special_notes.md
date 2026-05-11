# UMALOGI 特記事項・障害対応履歴

## 更新履歴（Changelog）

| 日付 | 変更内容 |
|------|---------|
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
