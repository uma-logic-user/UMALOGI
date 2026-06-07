"""
EV オーバーレイ禁止ガード（W-071）

安田記念(2026-06-07)の実損 ▲8,840円・ROI6% の教訓:
  未検証の手動係数（調教ランク×馬体重×直近フォーム）を学習済みモデルEVに
  掛け算した結果、4番シックスペンス(真のEV1.49)を9位に沈めて見逃した。

【ルール】
  - 本番 prerace_pipeline 内で学習済みモデルの EV 系列に
    手動係数を掛けてはならない（CLAUDE.md 条項 W-071）。
  - オーバーレイを使いたいなら:
      1. src/features/ に特徴量として実装
      2. 再学習して OOS ROI 改善を実証
      3. FEATURE_COLS に正式追加
    この手順を踏まずに係数をかけることは禁止。

【使い方】
  from src.ml.ev_overlay_guard import assert_no_manual_overlay, apply_validated_overlay

  # 本番パイプラインでは何もしなくてよい（ガードはコードレビュー時の抑止力）
  assert_no_manual_overlay(ev_series, context="prerace_pipeline")

  # テスト・研究用に係数を適用したい場合は必ず allow_research=True を渡す
  modified = apply_validated_overlay(ev_series, multiplier=1.15, allow_research=True)
"""

from __future__ import annotations

import os

import pandas as pd

# 環境変数 ALLOW_EV_OVERLAY=1 の場合のみ研究用オーバーレイを許可する（本番は設定禁止）
_OVERLAY_ENV_ALLOWED: bool = os.getenv("ALLOW_EV_OVERLAY", "0") == "1"


def assert_no_manual_overlay(
    ev_series: pd.Series,
    context: str = "unknown",
) -> None:
    """本番パスで呼ぶことで「手動オーバーレイを適用していない」ことを宣言する。

    現在はログ出力のみ。将来的に AUDIT_MODE=1 時に厳密検証を追加できる。
    """
    import logging

    logger = logging.getLogger(__name__)
    logger.debug(
        "[W-071] EV オーバーレイ禁止ガード通過 (context=%s, n=%d, max=%.3f)",
        context,
        len(ev_series),
        float(ev_series.max()) if not ev_series.empty else 0.0,
    )


def apply_validated_overlay(
    ev_series: pd.Series,
    multiplier: float,
    allow_research: bool = False,
) -> pd.Series:
    """EV 系列に係数を掛ける。本番では禁止・研究専用。

    Args:
        ev_series: 学習済みモデルの EV スコア系列。
        multiplier: 乗算係数（例: 1.15）。
        allow_research: True のときのみ実行可能。False ならエラー。

    Returns:
        係数適用後の EV 系列（研究環境のみ）。

    Raises:
        RuntimeError: allow_research=False かつ ALLOW_EV_OVERLAY 環境変数が未設定の場合。
    """
    if not allow_research and not _OVERLAY_ENV_ALLOWED:
        raise RuntimeError(
            "[W-071] 未検証の手動 EV オーバーレイは本番パイプラインで禁止されています。\n"
            "  - 特徴量として実装 → 再学習 → OOS ROI 実証 の手順を踏んでください。\n"
            "  - 研究目的なら allow_research=True を渡すか ALLOW_EV_OVERLAY=1 を設定してください。"
        )
    import logging

    logger = logging.getLogger(__name__)
    logger.warning(
        "[W-071] 研究用 EV オーバーレイ適用: multiplier=%.3f (n=%d) — 本番では禁止",
        multiplier,
        len(ev_series),
    )
    return ev_series * multiplier
