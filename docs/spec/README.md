# 📐 docs/spec/ — システム仕様書（バージョン固定）

このディレクトリは UMALOGI の**バージョン固定アーキテクチャ仕様書**を管理する。
各仕様書はリリース時点の全体設計を凍結したスナップショットであり、
ファイル名に `_vMAJOR.MINOR.PATCH` を付与してバージョンを明示する。

## 仕様書一覧

| 仕様Ver | ファイル | ステータス | 概要 |
|---------|---------|-----------|------|
| 1.0.0 | [ARCHITECTURE_v1.0.0.md](ARCHITECTURE_v1.0.0.md) | ✅ 現行 | Pure_EV_Edge / W-057 A/B / 卍 Isotonic 較正 / 単複ロック / 会計二重性分離 / autopilot＋watchdog 本番構成 |

## 関連ドキュメント

- 最新の自動同期版（バージョン非固定）: [`../SYSTEM_ARCHITECTURE.md`](../SYSTEM_ARCHITECTURE.md)
- 予測ロジック詳細: [`../1_prediction_logic.md`](../1_prediction_logic.md)
- 自動化スケジュール: [`../2_automation_schedule.md`](../2_automation_schedule.md)
- DB スキーマ: [`../3_data_schema.md`](../3_data_schema.md)

## バージョニング規約

[Semantic Versioning 2.0.0](https://semver.org/lang/ja/) に準拠。

| 桁 | 繰り上げ条件 | アクション |
|----|--------------|-----------|
| **MAJOR** | 後方互換を破る変更 | 新ファイル `ARCHITECTURE_v2.0.0.md` を新設、旧版は保持 |
| **MINOR** | 後方互換な機能追加 | 新ファイル `ARCHITECTURE_v1.1.0.md` を新設、または現行を改訂 |
| **PATCH** | バグ修正・挙動互換の変更 | 現行仕様書の更新履歴へ追記 |

> 仕様書の更新は**コードの変更とセット**で行うこと（[`CLAUDE.md`](../../CLAUDE.md) 仕様書追従ポリシー）。
