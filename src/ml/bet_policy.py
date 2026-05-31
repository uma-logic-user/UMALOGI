"""
src/ml/bet_policy.py — 実弾(ライブ)ベットの単一真実源

UMALOGI の確定実績(2026-05-31 分析)に基づく恒久ポリシー:
  - 単勝・複勝 のみが実弾(実際に投票する買い目)。三連系・馬連・馬単・ワイドは
    控除率+点数増で構造的に負けるため実弾から完全除外する。
  - Oracle / HitFocus は赤字(直前ROI 21〜66%)のため実弾から分離し、
    note/X 集客用の「観賞用買い目」としてのみ出力する。
  - 実弾モデルは 本命 / 卍 / Alpha-Payout の3つ。

このモジュールが「何を実弾としてカウントするか」の唯一の定義であり、
買い目フィルタ・ROI会計・Discord通知ラベルは全てここを参照する。
"""

from __future__ import annotations

import re

# ── ポリシー定義 ──────────────────────────────────────────────────────────────
# 実弾モデル（実際に投票する）
LIVE_MODELS: frozenset[str] = frozenset({"本命", "卍", "Alpha-Payout"})

# 実弾券種（単勝・複勝のみ）
LIVE_BET_TYPES: frozenset[str] = frozenset({"単勝", "複勝"})

# 観賞用モデル（note/X 集客専用・実弾対象外）
ORNAMENTAL_MODELS: frozenset[str] = frozenset({"Oracle", "HitFocus"})

# model_type の末尾サフィックス（"(直前)" / "(暫定)"）と V2 を剥がす正規表現
_SUFFIX_RE = re.compile(r"\((直前|暫定)\)\s*$")


def base_model(model_type: str) -> str:
    """タグ付き model_type からベースモデル名を抽出する。

    例: "本命(直前)" → "本命" / "OracleV2(暫定)" → "Oracle" /
        "Alpha-Payout(直前)" → "Alpha-Payout"

    Args:
        model_type: predictions.model_type の値（サフィックス付き）。

    Returns:
        ベースモデル名（不明時は入力をそのまま返す）。
    """
    if not model_type:
        return ""
    base = _SUFFIX_RE.sub("", model_type).strip()
    if base.endswith("V2"):
        base = base[:-2]
    return base


def is_ornamental(model_type: str) -> bool:
    """観賞用（Oracle / HitFocus）モデルなら True。"""
    return base_model(model_type) in ORNAMENTAL_MODELS


def is_live_bet(model_type: str, bet_type: str) -> bool:
    """この (モデル, 券種) が実弾（実際に投票する買い目）か判定する。

    実弾の条件: 観賞用モデルでない かつ 実弾モデル かつ 実弾券種(単勝/複勝)。

    Args:
        model_type: predictions.model_type（サフィックス付き可）。
        bet_type: 券種（"単勝"/"複勝"/"三連単" 等）。

    Returns:
        True = 実弾としてカウント / False = 観賞用（ROI会計・実弾通知から除外）。
    """
    base = base_model(model_type)
    if base in ORNAMENTAL_MODELS:
        return False
    return base in LIVE_MODELS and bet_type in LIVE_BET_TYPES


def live_bet_types_sql_list() -> str:
    """SQL の IN 句用に実弾券種をクオート済みカンマ区切りで返す。"""
    return ", ".join(f"'{t}'" for t in sorted(LIVE_BET_TYPES))
