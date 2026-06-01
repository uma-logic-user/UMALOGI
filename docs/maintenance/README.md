# 🛠️ docs/maintenance/ — 保守報告書

このディレクトリは UMALOGI の保守・修正作業の正式記録を管理する。

| ファイル | 役割 |
|---------|------|
| [MAINTENANCE_LOG.md](MAINTENANCE_LOG.md) | すべての修正のタイムライン記録（修正者・修正日・バージョン・実施内容・影響範囲・検証・ロールバック）。コミットのたびに追記必須。 |

## 既存の保守系ドキュメントとの関係

UMALOGI には作業領域別の Changelog 運用が既に存在する（[`CLAUDE.md`](../../CLAUDE.md) ドキュメント保守ルール）。
本ディレクトリはそれらを**横断する単一タイムライン**として機能する。

| 種別 | 記録先 |
|------|--------|
| 全修正の横断タイムライン | **本ディレクトリ `MAINTENANCE_LOG.md`** |
| 予測ロジック変更の詳細 | `docs/1_prediction_logic.md` 更新履歴 |
| 障害対応・手動リカバリ手順 | `docs/6_special_notes.md` |
| 弱点・技術的負債の進捗 | `docs/7_weakness_ledger.md`（W-NNN） |

> コミット時の記入ルールは [`CLAUDE.md`](../../CLAUDE.md) の「バージョン運用フロー」を参照。
