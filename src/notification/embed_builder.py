"""src/notification/embed_builder.py — Discord Embed プレミアム・インベスター化ビルダー

通知をプレーンテキストから「投資レポート級」の Embed に引き上げるための
共通部品を提供する:

  - infer_grade()      : レース名から格付け（G1/G2/G3/L/OP）を推定
  - grade_color()      : JRA 公式配色に準拠した格付けカラー（G1=青 / G2=赤 / G3=緑）
  - confidence_color() : 自信度 0.0〜1.0 をスチール→ゴールドのグラデーションに写像
  - dynamic_color()    : 万馬券級 EV > 格付け > 自信度 の優先順で Embed 色を決定
  - stake_bar()        : 推奨投資比率の ████░░░░░░ 40% インジケーター
  - build_axis_partner_fields(): 軸馬・相手馬・EV/想定オッズの 3 カラムグリッド

すべて純粋関数（I/O なし）。discord_notifier から利用される。
"""

from __future__ import annotations

import re

# ── 格付けカラー（JRA 公式のレース格付け配色に準拠）────────────────────────
COLOR_G1: int = 0x2E5FCC  # G1 = 青
COLOR_G2: int = 0xD92B47  # G2 = 赤
COLOR_G3: int = 0x1FA86B  # G3 = 緑
COLOR_LISTED: int = 0x9B59B6  # リステッド = 紫
COLOR_OPEN: int = 0xE67E22  # オープン特別 = 橙
COLOR_JACKPOT: int = 0xFF4500  # 万馬券級 EV = 赤橙
COLOR_DEFAULT: int = 0x00C8FF  # 既定 = シアン

_GRADE_COLORS: dict[str, int] = {
    "G1": COLOR_G1,
    "G2": COLOR_G2,
    "G3": COLOR_G3,
    "L": COLOR_LISTED,
    "OP": COLOR_OPEN,
}

# 自信度グラデーションの両端（スチールブルー → ゴールド）
_CONF_LOW_RGB: tuple[int, int, int] = (0x5A, 0x7A, 0x99)
_CONF_HIGH_RGB: tuple[int, int, int] = (0xFF, 0xD7, 0x00)

# ローマ数字・全角を含む格付け表記ゆれの正規化パターン
_GRADE_PAT = re.compile(
    r"[(（]\s*(?:"
    r"(?P<g1>G\s*[1１]|G\s*[IＩ](?![IＩVＶ])|GⅠ)|"
    r"(?P<g2>G\s*[2２]|G\s*[IＩ]{2}(?![IＩ])|GⅡ)|"
    r"(?P<g3>G\s*[3３]|G\s*[IＩ]{3}|GⅢ)|"
    r"(?P<listed>L|Ｌ)|"
    r"(?P<op>OP|ＯＰ|オープン)"
    r")\s*[)）]",
    re.IGNORECASE,
)


def infer_grade(race_name: str | None) -> str | None:
    """レース名から格付けを推定する。

    Args:
        race_name: レース名（例: '安田記念（GⅠ）'）。

    Returns:
        'G1' / 'G2' / 'G3' / 'L' / 'OP'。判定不能時は None（平場・条件戦・裏開催）。
    """
    if not race_name:
        return None
    m = _GRADE_PAT.search(race_name)
    if m is None:
        return None
    for key, label in (
        ("g1", "G1"),
        ("g2", "G2"),
        ("g3", "G3"),
        ("listed", "L"),
        ("op", "OP"),
    ):
        if m.group(key):
            return label
    return None  # pragma: no cover - パターン網羅済み


def grade_color(grade: str | None) -> int | None:
    """格付けに対応する Embed カラーを返す（未知の格付けは None）。"""
    if not grade:
        return None
    return _GRADE_COLORS.get(grade.upper())


def confidence_color(confidence: float) -> int:
    """自信度 0.0〜1.0 をスチールブルー→ゴールドのグラデーション色に写像する。

    Args:
        confidence: モデル自信度（範囲外はクランプ）。

    Returns:
        16進数 RGB 整数（Discord Embed の color フィールド用）。
    """
    t = min(max(confidence, 0.0), 1.0)
    r, g, b = (
        round(lo + (hi - lo) * t)
        for lo, hi in zip(_CONF_LOW_RGB, _CONF_HIGH_RGB, strict=True)
    )
    return (r << 16) | (g << 8) | b


def dynamic_color(
    grade: str | None = None,
    confidence: float | None = None,
    max_ev: float | None = None,
) -> int:
    """Embed カラーを動的に決定する。

    優先順位:
      1. max_ev >= 3.0（万馬券級の歪み）→ 赤橙（最優先で目立たせる）
      2. 格付け（G1=青 / G2=赤 / G3=緑 / L=紫 / OP=橙）
      3. 自信度グラデーション（スチール→ゴールド）
      4. 既定シアン
    """
    if max_ev is not None and max_ev >= 3.0:
        return COLOR_JACKPOT
    by_grade = grade_color(grade)
    if by_grade is not None:
        return by_grade
    if confidence is not None:
        return confidence_color(confidence)
    return COLOR_DEFAULT


def stake_bar(fraction: float, width: int = 10) -> str:
    """推奨投資比率をインジケーター風テキストで表現する。

    例: stake_bar(0.4) → '████░░░░░░ 40%'

    Args:
        fraction: バンクロールに対する投資比率（0.0〜1.0。範囲外はクランプ）。
        width: バーの全ブロック数。

    Returns:
        ████░░░░░░ NN% 形式の文字列。1 ブロック未満の端数は小数 1 桁で表示。
    """
    t = min(max(fraction, 0.0), 1.0)
    filled = round(t * width)
    bar = "█" * filled + "░" * (width - filled)
    pct = t * 100
    pct_str = f"{pct:.1f}%" if 0 < pct < 10 and pct != int(pct) else f"{pct:.0f}%"
    return f"{bar} {pct_str}"


def _horses_label(horses: list[tuple[int, str]]) -> str:
    """(馬番, 馬名) リストを '5 アーバンシック' 改行区切りの表示文字列にする。"""
    if not horses:
        return "—"
    lines = []
    for num, name in horses:
        lines.append(f"`{num:>2}` **{name}**" if name else f"`{num:>2}`")
    return "\n".join(lines)


def build_axis_partner_fields(
    axis: list[tuple[int, str]],
    partners: list[tuple[int, str]],
    ev: float,
    odds: float | None,
) -> list[dict[str, object]]:
    """軸馬・相手馬・EV/想定オッズを 3 カラムのグリッド fields にする。

    Discord は inline=True のフィールドを横並びグリッドに配置するため、
    「🎯 軸馬 | 🤝 相手馬 | 💎 期待値」が 1 行に整然と並ぶ。

    Args:
        axis: 軸馬の (馬番, 馬名) リスト。
        partners: 相手馬の (馬番, 馬名) リスト。
        ev: 期待値。
        odds: 想定オッズ（None なら未取得表示）。

    Returns:
        Discord embed の fields リスト（3 要素・すべて inline）。
    """
    odds_str = f"{odds:,.1f}" if odds is not None else "—（推定不能）"
    return [
        {"name": "🎯 軸馬", "value": _horses_label(axis), "inline": True},
        {"name": "🤝 相手馬", "value": _horses_label(partners), "inline": True},
        {
            "name": "💎 期待値 / 想定オッズ",
            "value": f"**EV {ev:.2f}**\n{odds_str} 倍",
            "inline": True,
        },
    ]
