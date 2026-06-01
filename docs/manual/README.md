# 📖 docs/manual/ — 取扱説明書

このディレクトリは UMALOGI の操作・運用マニュアルを管理する。
読者の役割に応じて 2 系統に分かれる。

| ファイル | 対象読者 | 内容 |
|---------|---------|------|
| [USER_MANUAL.md](USER_MANUAL.md) | 利用者（予想を閲覧・参考にする人） | Next.js / Streamlit ダッシュボードの見方、予想スコア・EV・ケリー推奨額の読み方、的中実績の確認方法 |
| [OPERATIONS_MANUAL.md](OPERATIONS_MANUAL.md) | 運用者（システムを動かす人） | 本番常駐プロセスの起動・停止、無人運用バッチ、障害時のチェックリスト・手動リカバリ手順 |

## 関連ドキュメント

- 初期セットアップ・依存関係: ルート [`README.md`](../../README.md)
- システム全体設計: [`../spec/ARCHITECTURE_v1.0.0.md`](../spec/ARCHITECTURE_v1.0.0.md)
- 障害対応の詳細事例: [`../6_special_notes.md`](../6_special_notes.md)
