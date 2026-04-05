# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 言語・スタイル規約

- **応答言語**: 日本語
- **Python型ヒント**: 必須。すべての関数・メソッドに引数と戻り値の型アノテーションを付与すること（mypy strict 相当）

## プロジェクト概要

**UMALOGI** — 競馬AI分析システム。血統解析・レース結果分析・WIN5予想をPythonで実装し、Next.jsでWeb公開することをゴールとする。

## ディレクトリ構成

```
src/         # Pythonソースコード（血統解析・予想モデル・スクレイパー）
data/raw/    # 取得生データ（スクレイピング結果・JRA-VAN出力）
data/processed/  # 加工済みデータ（特徴量・正規化済みDB）
tests/       # pytestテスト群
docs/        # 設計ドキュメント
web/         # Next.jsフロントエンド（Phase 4以降）
```

## アーキテクチャ方針

- **データフロー**: `src/scraper/` でデータ取得 → `data/raw/` 保存 → `src/pipeline/` で加工 → `data/processed/` → 予想モデル入力
- **血統解析** (`src/pedigree/`): 父系・母系3代をベクトル化し、コース・距離適性スコアを算出
- **WIN5予想** (`src/win5/`): 単勝オッズとモデル確率を比較し、期待値ベースで組み合わせを絞り込む
- **Web層** (`web/`): Next.js API Routes 経由でPythonバックエンドと連携
