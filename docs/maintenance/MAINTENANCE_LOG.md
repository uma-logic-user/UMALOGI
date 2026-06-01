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
