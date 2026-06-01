# 📙 UMALOGI 運用者マニュアル（OPERATIONS MANUAL）

> 対象: システムを**起動・停止・保守する運用者**。
> 予想の見方は [USER_MANUAL.md](USER_MANUAL.md) を参照。
> 本マニュアルは [`../SYSTEM_ARCHITECTURE.md`](../SYSTEM_ARCHITECTURE.md) / `CLAUDE.md`「本番稼働アーキテクチャ」と同期している。

---

## 1. 本番常駐プロセス（無人運用の中核）

本番では以下の 3 プロセスが常駐する。**これが現在の真の稼働実態である。**

| プロセス | 起動コマンド | 役割 |
|---|---|---|
| **オートパイロット** | `py scripts/today_auto_runner.py --continuous` | 週次自律運転の中核。金曜夜の同期＋暫定予想 → 土日の直前予想/結果速報監視 → 日曜の週次レポート → 翌週金曜まで自動スリープを人手ゼロで回す。 |
| **ウォッチドッグ** | `py scripts/watchdog.py --interval 5` | 自己修復番犬。当日レースのオッズ欠損を監視し、検知時に JVLink 再起動＋データ再同期を段階実行。 |
| **ダッシュボード** | `py -m streamlit run web_streamlit/app.py --server.port 8501` | 成果可視化 Streamlit UI。正本は `web_streamlit/app.py` 唯一。 |

### ⚠️ scheduler.py との排他（最重要）

- `scripts/scheduler.py` は **schedule ライブラリ方式の排他代替**で、現在の本番では**稼働していない**。
- オートパイロットと scheduler.py を**同時に常駐させてはならない**（二重予想・二重 Discord 通知・`predictions` 汚染を招く）。
- scheduler.py 方式に切り替える場合のみ `scripts/bat/start_scheduler_mode.bat`（排他ガード付き）を使う。

---

## 2. ワンクリック起動・停止（Windows）

| 操作 | バッチ | 挙動 |
|------|--------|------|
| **起動** | `scripts/bat/start_umalogi.bat` | 上記 3 プロセスを別ウィンドウで非同期起動（二重起動ガード付き）。 |
| **停止** | `scripts/bat/stop_umalogi.bat` | Name が python 系 **かつ** 当該スクリプト実行中の PID のみを安全停止（全 Python 一括 kill はしない）。 |

詳細・PC 起動時の自動実行登録は [`../../scripts/bat/README_BAT.md`](../../scripts/bat/README_BAT.md) を参照。

> **Python 実行環境**: venv/Poetry 不使用。`py` ランチャー（既定 64bit Python 3.14）を使う。
> JVLink COM 操作（32bit 制約）のみ内部で 32bit Python の subprocess に委譲されるため、バッチ側で 32bit を意識する必要はない。

---

## 3. 週次サイクル（オートパイロット内部）

| タイミング | 実行内容 |
|---|---|
| 月曜 03:00 | 卍 Isotonic 較正器の週次再学習 |
| 月曜 07:00 | 全件再学習（土日ガード・別スレッド） |
| 金曜 夜 | 出馬表取得 + JRA-VAN RACE 同期 + 暫定予想 |
| 土日 08:30〜 | 当日全レース直前予想ループ |
| 土日 17:30 | 結果取得 + 評価 + 通知 |
| 土日 17:50 | 日次ヘルス + W-057 A/B 進捗 → Discord |

---

## 4. 障害時チェックリスト

### 4-1. 「予想・的中実績が UI に出ない」

> ⚠️ **「UI に出ない＝データ消失」は誤り。** 必ず DB を直接確認してから判断すること（CLAUDE.md 条項4 事故事例）。

1. **DB の COUNT を直接確認**（最優先）:
   ```bash
   py -c "import sqlite3; con=sqlite3.connect('data/umalogi.db'); print(con.execute('SELECT COUNT(*) FROM predictions').fetchone())"
   ```
2. ポート 3000（Next.js）/ 8501（Streamlit）の疎通確認。
3. Next.js 再起動: `cd web && npm start`。
4. ビルドが古い場合のみ再ビルド: `cd web && npm run build && npm start`。

### 4-2. 「オッズが取得できない／全 NaN」

1. ウォッチドッグのログを確認（JVLink 再起動を自動試行しているはず）。
2. `realtime_odds` の当該 `race_id` 件数を確認。0 件なら netkeiba フォールバックが効くはず。
3. JVLink ダイアログが残存していないか確認（`jvlink_dialog_handler` が自動突破するが、3 秒超残存で WARNING ログ）。

### 4-3. 「文字化けが表示された」

1. `scripts/cleanup_encoding.py` を実行して DB 側を修復。
2. 対象フィールド: `horse_name`, `jockey`, `trainer`, `race_name`, `sex_age`。

---

## 5. 作業前の鉄則（DB 保護）

> CLAUDE.md 条項1・条項4 の要約。**違反は「障害」として扱われる。**

- 過去 `predictions` の **UPDATE / DELETE は禁止**。再推論は新規 INSERT ＋ `is_superseded` 論理無効化。
- DB の **物理削除（DELETE / DROP TABLE）は禁止**。論理削除（フラグ）または UPDATE のみ。
- スキーマ変更・大規模データ操作・モデル再学習の前に必ずバックアップ:
  ```bash
  cp data/umalogi.db data/backups/umalogi_$(date +%Y%m%d_%H%M%S).db
  ```

---

## 6. 修正作業時の必須ルール（運用者・Claude 共通）

コードを修正してコミットする際は、以下の 3 点セットを必ず実施する（[`../../CLAUDE.md`](../../CLAUDE.md) バージョン運用フロー）。

1. **`VERSION` の更新**（SemVer に従い MAJOR/MINOR/PATCH を繰り上げ）。
2. **`docs/maintenance/MAINTENANCE_LOG.md` への記述**（修正者・修正日・バージョン・実施内容・影響範囲）。
3. **`docs/spec/` の該当バージョン仕様書の更新**（アーキテクチャに影響する変更時）。

加えて、関連する `docs/N_*.md`（予測ロジック・スケジュール・スキーマ等）の更新履歴へも追記する（仕様書追従ポリシー）。
