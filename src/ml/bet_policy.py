"""
src/ml/bet_policy.py — 実弾(ライブ)ベットの単一真実源

UMALOGI の確定実績(2026-05-31 分析)に基づく恒久ポリシー:
  - 単勝・複勝 のみが実弾(実際に投票する買い目)。三連系・馬連・馬単・ワイドは
    控除率+点数増で構造的に負けるため実弾から完全除外する。
  - Oracle / HitFocus は赤字(直前ROI 21〜66%)のため実弾から分離し、
    note/X 集客用の「観賞用買い目」としてのみ出力する。
  - 実弾モデルは 本命 / 卍 / Alpha-Payout / Pure_EV_Edge / FukushoElite。
    FukushoElite(W-020) は複勝特化で、segment+edge フィルタに加え
    統計的複勝EV>=しきい値を満たすレースのみ生成する EV 最優先ゲートを持つ。

このモジュールが「何を実弾としてカウントするか」の唯一の定義であり、
買い目フィルタ・ROI会計・Discord通知ラベルは全てここを参照する。
"""

from __future__ import annotations

import re

# ── ポリシー定義 ──────────────────────────────────────────────────────────────
# 実弾モデル（実際に投票する）。Pure_EV_Edge は黒字化専用の単複バリアント、
# FukushoElite(W-020) は複勝特化(EV最優先ゲート)。
LIVE_MODELS: frozenset[str] = frozenset(
    {"本命", "卍", "Alpha-Payout", "Pure_EV_Edge", "FukushoElite"}
)

# 選択的実弾モデル: 厳格なセグメント条件で多くの開催日に正当に0件となるため、
# 「生成0件=サイレント障害」アラート(W-064)の対象から除外する。
# 広域モデル(本命/卍/Alpha-Payout/Pure_EV_Edge)は毎開催日に発火が期待され監視対象。
SELECTIVE_LIVE_MODELS: frozenset[str] = frozenset({"FukushoElite"})

# 実弾券種（単勝・複勝のみ）
LIVE_BET_TYPES: frozenset[str] = frozenset({"単勝", "複勝"})

# 観賞用モデル（note/X 集客専用・実弾対象外）
ORNAMENTAL_MODELS: frozenset[str] = frozenset({"Oracle", "HitFocus"})

# ── 資金会計の単一真実源（実発注額 と 評価用コストの厳密分離）──────────────────
# predictions.recommended_bet は「実発注額（Kelly 等の実際に賭ける額）」。
# P&L 会計・A/B 評価のコストは **常に flat_cost()（¥100×点数）** を用いる。
# これにより賭け額の大小に依存しない stake-independent な ROI 比較が保証され、
# Kelly 実額と会計基準が混同されない（evaluator / pnl_accounting が本関数を共有）。
FLAT_UNIT_YEN: int = 100  # 会計上の1点あたり単位（JRA 最小単位）


def flat_cost(n_points: int) -> int:
    """評価・A/B 会計用のフラットコスト（¥100 × 点数）を返す。実発注額とは別概念。"""
    return FLAT_UNIT_YEN * max(int(n_points), 0)


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
