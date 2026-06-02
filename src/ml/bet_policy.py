"""
src/ml/bet_policy.py — 実弾(ライブ)ベットの単一真実源

UMALOGI の確定実績(2026-05-31 分析 / 2026-06-02 週末縮退)に基づく恒久ポリシー:
  - 単勝・複勝 のみが実弾(実際に投票する買い目)。三連系・馬連・馬単・ワイドは
    控除率+点数増で構造的に負けるため実弾から完全除外する。
  - Oracle / HitFocus は赤字(直前ROI 21〜66%)のため実弾から分離し、
    note/X 集客用の「観賞用買い目」としてのみ出力する。
  - 実弾モデルは 卍 / Pure_EV_Edge / FukushoElite の3つに集約する(2026-06-02 縮退)。
    本命(直前ROI 88%/暫定60%) と Alpha-Payout(直前ROI 70%) は確定実績で
    100%を下回り続けたため実弾から除外し、観賞用相当の非実弾モデルへ退避する。
    残す3モデルは黒字化の核: 卍(較正済みEV・唯一の黒字頭, 直前131.8%/暫定378.2%)、
    Pure_EV_Edge(単複EV>=1.15バリアント, 2年BTでROI137〜211%)、
    FukushoElite(W-020 複勝特化・統計的複勝EV>=しきい値の EV 最優先ゲート)。
  - 卍 複勝特化昇格(2026-06-02): OOS検証で卍は複勝が黒字傾向(Challenger 108.8%)・
    単勝は backtest で100%割れ(ライブと乖離)のため、卍は**複勝のみ実弾**へ昇格し
    (MODEL_LIVE_BET_TYPES)、**単勝は WATCH_ONLY**(投票せず予想生成とROI監視のみ)とする。
    複勝EVは専用 Platt 較正器(manji_place_calibrator)で P(複勝圏) を較正する。

このモジュールが「何を実弾としてカウントするか」の唯一の定義であり、
買い目フィルタ・ROI会計・Discord通知ラベルは全てここを参照する。
"""

from __future__ import annotations

import re

# ── ポリシー定義 ──────────────────────────────────────────────────────────────
# 実弾モデル（実際に投票する）。2026-06-02 週末縮退で 卍/Pure_EV_Edge/FukushoElite の
# 3モデルに集約。Pure_EV_Edge は黒字化専用の単複バリアント、
# FukushoElite(W-020) は複勝特化(EV最優先ゲート)。
# 本命・Alpha-Payout は確定実績ROI<100%のため非実弾へ退避(NON_LIVE_RETIRED 参照)。
LIVE_MODELS: frozenset[str] = frozenset({"卍", "Pure_EV_Edge", "FukushoElite"})

# 2026-06-02 縮退で実弾から退避したモデル（予想自体は生成・表示するが投票対象外）。
# Oracle/HitFocus(ORNAMENTAL_MODELS) と異なり集客専用ではなく、ROI回復時に
# LIVE_MODELS へ復帰しうる「保留」枠。is_live_bet は False を返す。
NON_LIVE_RETIRED: frozenset[str] = frozenset({"本命", "Alpha-Payout"})

# 選択的実弾モデル: 厳格なセグメント条件で多くの開催日に正当に0件となるため、
# 「生成0件=サイレント障害」アラート(W-064)の対象から除外する。
# 広域監視対象は 卍/Pure_EV_Edge（毎開催日の発火を期待）。FukushoElite は選択的。
SELECTIVE_LIVE_MODELS: frozenset[str] = frozenset({"FukushoElite"})

# 実弾券種（単勝・複勝のみ）。モデル別の上書きが無い場合のデフォルト。
LIVE_BET_TYPES: frozenset[str] = frozenset({"単勝", "複勝"})

# ── モデル別 実弾券種オーバーライド（2026-06-02 卍 複勝特化昇格）──────────────────
# 既定は LIVE_BET_TYPES（単複両方）。ここに登録したモデルは、その券種のみを実弾とする。
# 卍は OOS 検証で複勝が黒字傾向(Challenger 108.8%)・単勝は backtest で100%割れのため、
# 複勝のみを実弾投票対象へ昇格し、単勝は WATCH_ONLY_MODELS へ退避する。
MODEL_LIVE_BET_TYPES: dict[str, frozenset[str]] = {
    "卍": frozenset({"複勝"}),
}

# 監視専用 (WATCH_ONLY): 予想データ生成と ROI 監視は継続するが「投票しない」(model, 券種)。
# 実弾(is_live_bet=False)でありながら、観賞用(集客)とも異なる「保留・監視」枠。
# 単勝が backtest と乖離(ライブ好調/OOS不調)するため、投票を止めて実績を観察し続ける。
WATCH_ONLY_MODELS: dict[str, frozenset[str]] = {
    "卍": frozenset({"単勝"}),
}

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


def is_watch_only(model_type: str, bet_type: str) -> bool:
    """この (モデル, 券種) が監視専用（投票しないが予想生成・ROI監視は継続）か判定する。

    例: 卍×単勝 は WATCH_ONLY（is_live_bet=False だが予想は保存・実績を追跡）。

    Args:
        model_type: predictions.model_type（サフィックス付き可）。
        bet_type: 券種。

    Returns:
        True = 監視専用（実弾投票しない）。
    """
    base = base_model(model_type)
    return bet_type in WATCH_ONLY_MODELS.get(base, frozenset())


def is_live_bet(model_type: str, bet_type: str) -> bool:
    """この (モデル, 券種) が実弾（実際に投票する買い目）か判定する。

    実弾の条件: 観賞用モデルでない かつ 監視専用でない かつ 実弾モデル かつ
    そのモデルの実弾券種（MODEL_LIVE_BET_TYPES の上書き or 既定 LIVE_BET_TYPES）。

    Args:
        model_type: predictions.model_type（サフィックス付き可）。
        bet_type: 券種（"単勝"/"複勝"/"三連単" 等）。

    Returns:
        True = 実弾としてカウント / False = 観賞用・監視専用・対象外（実弾通知/会計から除外）。
    """
    base = base_model(model_type)
    if base in ORNAMENTAL_MODELS:
        return False
    if base not in LIVE_MODELS:
        return False
    if is_watch_only(model_type, bet_type):
        return False
    allowed = MODEL_LIVE_BET_TYPES.get(base, LIVE_BET_TYPES)
    return bet_type in allowed


def live_bet_types_sql_list() -> str:
    """SQL の IN 句用に実弾券種をクオート済みカンマ区切りで返す。"""
    return ", ".join(f"'{t}'" for t in sorted(LIVE_BET_TYPES))
