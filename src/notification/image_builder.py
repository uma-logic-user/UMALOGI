"""
証拠画像ビルダー

的中した際の「証拠画像」を Pillow で生成する。
画像サイズ: 800×500 px / ダークサイバーパンク風デザイン

依存: pillow
"""

from __future__ import annotations

import logging
import math
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    from PIL import Image, ImageDraw, ImageFont  # type: ignore[import-untyped]
    _PIL_AVAILABLE = True
except ImportError:
    _PIL_AVAILABLE = False
    logger.warning("Pillow がインストールされていません: pip install pillow")

# ── 設計定数 ─────────────────────────────────────────────────────
_W, _H   = 800, 500
_BG      = (2, 6, 14)        # #02060e
_SURFACE = (8, 21, 38)       # #081526
_CYAN    = (0, 200, 255)
_GOLD    = (255, 215, 0)
_GREEN   = (0, 255, 136)
_RED     = (255, 51, 102)
_WHITE   = (224, 244, 255)
_MUTED   = (74, 122, 150)


def _load_font(size: int) -> "ImageFont.FreeTypeFont | ImageFont.ImageFont":
    """日本語フォント（NotoSansCJK / Meiryo）を探して返す。なければデフォルト。"""
    candidates = [
        "C:/Windows/Fonts/meiryo.ttc",
        "C:/Windows/Fonts/YuGothR.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKjp-Regular.otf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except (OSError, IOError):
            continue
    return ImageFont.load_default()


def build_hit_image(
    race_name: str,
    date: str,
    bet_type: str,
    combination: list[str],
    payout: float,
    roi: float,
    invested: float,
    out_path: Path,
) -> Path | None:
    """
    的中証拠画像を生成して out_path に保存する。

    Returns:
        保存したパス、または Pillow 未インストール時は None
    """
    if not _PIL_AVAILABLE:
        return None

    img  = Image.new("RGB", (_W, _H), _BG)
    draw = ImageDraw.Draw(img)

    # ── グリッドライン ──────────────────────────────────────────
    for x in range(0, _W, 48):
        draw.line([(x, 0), (x, _H)], fill=(0, 200, 255, 6), width=1)
    for y in range(0, _H, 48):
        draw.line([(0, y), (_W, y)], fill=(0, 200, 255, 6), width=1)

    # ── ヘッダーバー ────────────────────────────────────────────
    draw.rectangle([(0, 0), (_W, 72)], fill=_SURFACE)
    draw.line([(0, 72), (_W, 72)], fill=_CYAN, width=1)

    fn_title  = _load_font(28)
    fn_label  = _load_font(16)
    fn_value  = _load_font(36)
    fn_small  = _load_font(13)

    # ロゴ
    draw.text((24, 20), "UMA-LOGI AI", font=fn_title, fill=_CYAN)
    draw.text((24, 52), "HORSE RACING PREDICTION SYSTEM", font=fn_small, fill=_MUTED)

    # 日付・レース名
    draw.text((_W - 300, 16), f"{date}", font=fn_label, fill=_MUTED)
    draw.text((_W - 300, 40), race_name[:20], font=fn_label, fill=_WHITE)

    # ── 的中バッジ ──────────────────────────────────────────────
    badge_color = _GOLD if roi >= 300 else _GREEN
    draw.rounded_rectangle([(24, 90), (220, 140)], radius=8, fill=badge_color)
    hit_text = "万馬券 HIT!" if payout >= 10_000 else "的中 HIT!"
    draw.text((30, 98), hit_text, font=fn_label, fill=_BG)

    # ── 馬券種 ──────────────────────────────────────────────────
    draw.text((24, 155), "馬券種", font=fn_small, fill=_MUTED)
    draw.text((24, 175), bet_type, font=fn_value, fill=_CYAN)

    # ── 組み合わせ ───────────────────────────────────────────────
    combo_str = " / ".join(combination[:4])
    draw.text((200, 155), "予想", font=fn_small, fill=_MUTED)
    draw.text((200, 175), combo_str[:30], font=fn_label, fill=_WHITE)

    # ── 払戻金 ──────────────────────────────────────────────────
    y_row2 = 270
    draw.text((24, y_row2), "払戻金", font=fn_small, fill=_MUTED)
    payout_str = f"¥{payout:,.0f}"
    draw.text((24, y_row2 + 20), payout_str, font=fn_value, fill=_GOLD)

    # ── 回収率 ──────────────────────────────────────────────────
    draw.text((320, y_row2), "回収率", font=fn_small, fill=_MUTED)
    roi_color = _GOLD if roi >= 300 else _GREEN if roi >= 100 else _RED
    draw.text((320, y_row2 + 20), f"{roi:.1f}%", font=fn_value, fill=roi_color)

    # ── 購入金額 ─────────────────────────────────────────────────
    draw.text((580, y_row2), "購入金額", font=fn_small, fill=_MUTED)
    draw.text((580, y_row2 + 20), f"¥{invested:,.0f}", font=fn_label, fill=_MUTED)

    # ── セパレーター ─────────────────────────────────────────────
    draw.line([(24, 370), (_W - 24, 370)], fill=_CYAN, width=1)

    # ── フッター ─────────────────────────────────────────────────
    draw.text((24, 385), "#競馬AI  #UmaLogiAI  #JRA", font=fn_small, fill=_MUTED)

    # ── ネオン枠 ─────────────────────────────────────────────────
    draw.rectangle([(2, 2), (_W - 3, _H - 3)], outline=_CYAN, width=1)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(str(out_path), "PNG")
    logger.info("証拠画像生成: %s", out_path)
    return out_path
