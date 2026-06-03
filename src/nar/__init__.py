"""src.nar — 地方競馬（NAR: National Association of Racing）対応モジュール群。

UMALOGI の中央競馬（JRA）本番パイプラインから **完全に隔離** された、
地方競馬への横展開のための基盤パッケージ。

設計原則:
  - 本パッケージは src/ops・src/ml 等の中央競馬用コードを一切変更しない。
  - 既存の Note/X 生成・予算配分ロジック（src.ops.money_management /
    src.ops.note_generator）は **再利用** する（NoteBet 互換アダプタ経由）。
  - 実弾投票・bet_policy・DB へ副作用を持たない（表示/取得の基盤のみ）。
"""

from __future__ import annotations

__all__ = [
    "data_fetcher",
    "note_adapter",
]
