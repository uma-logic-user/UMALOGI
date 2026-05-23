# UMALOGI 自動化スケジュール完全仕様

<!-- ============================================================
【Claude Code への絶対指示】
今後、自動化スケジュール（cron トリガー、scheduler.py の `schedule.every()` 呼び出し、
`_JOB_SCHEDULES` 定義、または自動実行に関わる任意のコード）に変更・修正を行う場合は、
**開発を執行する前に必ずこの `docs/automation_schedule.md` の該当箇所を同時に書き換え**、
コードと仕様書の整合性を 100% 保った状態で git commit すること。
このドキュメントがコードと乖離している状態は「障害」として扱う。
============================================================ -->

**最終更新**: 2026-05-23  
**管理スクリプト**: `scripts/scheduler.py`  
**起動方法**: `UMALOGI起動.bat` または `python scripts/scheduler.py`

---

## 週次スケジュール一覧

| 曜日 | 時刻 | ジョブ名 | 主要処理 | タイムアウト窓 |
|------|------|----------|----------|---------------|
| 毎時 | :00 | `job_heartbeat` | 死活監視 → Discord #system | — |
| 毎日 | 23:00 | `job_daily_backup` | DB バックアップ（5世代ローテーション） | — |
| **金曜** | 20:00 | `job_friday_sync` | JVLink同期 → 翌日(土曜)全レース暫定予想生成 → Discord通知 | 16h |
| **土曜** | 20:00 | `job_friday_sync` | JVLink同期 → 翌日(日曜)全レース暫定予想生成 → Discord通知 | 16h |
| 土日 | 07:00 | `job_weekend_batch_pre` | note下書き / Umanity暫定投稿 / X告知 | 4h |
| 土日 | 07:30 | `job_morning_wood` | 調教タイム取得（JVLink 32bit） | 4h |
| 土日 | 08:30 | `job_today_auto_runner` | 当日全レース直前予想ループ起動 → Discord通知まで自動 | 3h |
| 土日 | 09:00 | `job_win5_prediction` | WIN5 バッチ予測（締切前） | 2h |
| 土日 | 10:30 | `job_note_daily_article` | note AI厳選記事生成 → Discord転送 | 3h |
| 土日 | 13:00 | `job_umanity_upload` | ウマニティ予想投稿（直前予想確定後） | — |
| 土日 | 13:00 | `job_intraday_sync` | レース中間結果の確定払戻を随時同期 | — |
| 土日 | 15:30 | `job_intraday_sync` | レース後半の払戻同期 | — |
| 土日 | 17:15 | `job_win5_result_fetch` | WIN5 確定結果取得（全レース終了後） | 4h |
| 土日 | 17:30 | `job_post_race` | 払戻確定後レース後処理（的中評価 / Hit Flash） | 4h |
| **日曜** | 18:00 | `job_ab_report` | V1/V2 A/B 週次成績比較レポート | 4h |
| 土日 | 18:30 | `job_weekend_batch_post` | P&L集計 / 的中カード生成 / X結果報告 | 4h |
| **月曜** | 05:00 | `job_weekly_backup` | DB+ログ 週次 ZIP バックアップ | — |
| 月曜 | 06:00 | `job_monday_masters` | マスタ更新（騎手/調教師/馬マスタ全件） | 12h |
| 月曜 | 07:00 | `job_weekly_retrain` | 全モデル週次再学習（Champion/Challenger 評価含む） | 12h |
| 月曜 | 08:00 | `job_git_push` | 差分を Git コミット → リモートへプッシュ | 12h |
| 月曜 | 08:30 | 直前実行 lambda | 直近28日実績サマリーを Discord へ自動送信 | 2min |

---

## 土日の詳細タイムライン

```
金 20:00  job_friday_sync ─── JVLink同期 → 翌日(土曜)暫定予想生成 → Discord通知
               └── src/pipeline/prediction.py provisional --date <土曜日>

土 07:00  job_weekend_batch_pre ── note下書き / X告知
土 07:30  job_morning_wood ─────── 坂路・ウッド調教タイム取得
土 08:30  job_today_auto_runner ── 直前予想ループ（発走20分前ごとに順次実行）
土 09:00  job_win5_prediction ──── WIN5 締切前に予測発行
土 10:30  job_note_daily_article ─ AI厳選コンテンツ生成
土 13:00  job_umanity_upload ───── ウマニティ投稿
土 13:00  job_intraday_sync ─────── 昼開催 払戻確定同期
土 15:30  job_intraday_sync ─────── 後半開催 払戻確定同期
土 17:15  job_win5_result_fetch ── WIN5 確定払戻取得
土 17:30  job_post_race ──────────── 的中評価 / Hit Flash 送信
土 18:30  job_weekend_batch_post ─ P&L集計 / 的中カード / X報告

土 20:00  job_friday_sync ─── JVLink同期 → 翌日(日曜)暫定予想生成 → Discord通知
               └── src/pipeline/prediction.py provisional --date <日曜日>

日 07:00〜18:30  土曜と同じ流れ（job_ab_report が 18:00 に追加される）
```

---

## ジョブ定義ファイル参照

| ジョブ名 | 関数 | 定義行（scheduler.py） |
|----------|------|----------------------|
| `job_heartbeat` | `job_heartbeat()` | L637付近 |
| `job_daily_backup` | `job_daily_backup()` | L637付近 |
| `job_friday_sync` | `job_friday_sync()` | L638 |
| `job_weekend_batch_pre` | `job_weekend_batch_pre()` | L637付近 |
| `job_morning_wood` | `job_morning_wood()` | L637付近 |
| `job_today_auto_runner` | `job_today_auto_runner()` | L637付近 |
| `job_win5_prediction` | `job_win5_prediction()` | L637付近 |
| `job_note_daily_article` | `job_note_daily_article()` | L637付近 |
| `job_umanity_upload` | `job_umanity_upload()` | L637付近 |
| `job_intraday_sync` | `job_intraday_sync()` | L637付近 |
| `job_win5_result_fetch` | `job_win5_result_fetch()` | L637付近 |
| `job_post_race` | `job_post_race()` | L637付近 |
| `job_ab_report` | `job_ab_report()` | L637付近 |
| `job_weekend_batch_post` | `job_weekend_batch_post()` | L637付近 |
| `job_weekly_backup` | `job_weekly_backup()` | L637付近 |
| `job_monday_masters` | `job_monday_masters()` | L637付近 |
| `job_weekly_retrain` | `job_weekly_retrain()` | L637付近 |
| `job_git_push` | `job_git_push()` | L637付近 |

---

## スケジュール登録コード（`register_schedules()`）

コードの正式定義は `scripts/scheduler.py` の `register_schedules()` 関数（L1540〜）と
`_JOB_SCHEDULES` dict（L115〜）を参照すること。

### キャッチアップ機能

スケジューラが停止中だった場合の取りこぼし対策として、
`_should_run_catchup(job_name, scheduled_dt)` が起動時に自動で過去分を再実行する。

```python
_CATCHUP_WINDOW_H: dict[str, int] = {
    "job_friday_sync":       16,   # Fri/Sat 20:00 → 翌12:00 まで
    "job_morning_wood":       4,   # 07:30 → 11:30
    "job_weekend_batch_pre":  4,   # 07:00 → 11:00
    "job_today_auto_runner":  3,   # 08:30 → 11:30
    "job_win5_prediction":    2,   # 09:00 → 11:00
    "job_note_daily_article": 3,   # 10:30 → 13:30
    "job_win5_result_fetch":  4,   # 17:15 → 21:15
    "job_post_race":          4,   # 17:30 → 21:30
    "job_ab_report":          4,   # 18:00 → 22:00
    "job_weekend_batch_post": 4,   # 18:30 → 22:30
    "job_monday_masters":    12,   # 06:00 → 18:00
    "job_weekly_retrain":    12,   # 07:00 → 19:00
    "job_git_push":          12,   # 08:00 → 20:00
}
```

---

## 緊急手動実行コマンド

スケジューラが間に合わなかった場合の手動実行例:

```powershell
# 暫定予想の緊急生成（例: 翌日 2026-05-25 分）
py -m src.main_pipeline provisional --date 20260525

# JVLinkデータ同期（32bit Python必須）
py -3.11-32 scripts/fetch_jvlink_stored.py --mode stored

# 直前予想ループ（当日全レース）
py scripts/auto_runner.py --date 20260525

# WIN5予測
py -m src.main_pipeline win5 --date 20260525

# 払戻確定同期
py scripts/update_payouts.py --date 20260525
```

---

## 変更履歴

| 日付 | 変更内容 |
|------|----------|
| 2026-05-23 | 初版作成。job_friday_synを土曜20:00にも追加（日曜暫定予想の取りこぼし修正）。scheduler.py register_schedules() + _JOB_SCHEDULES を同時更新。 |
| 2026-05-23 | `DISCORD_WEBHOOK_HIT_FLASH` 環境変数を追加（的中速報専用チャンネル分離）。discord_notifier.py の `notify_hit_summary()` 送信先を変更。|
