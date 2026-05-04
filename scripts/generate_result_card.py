"""
scripts/generate_result_card.py — 的中実績アピールカード画像の自動生成

prediction_results テーブルから的中データを取得し、Pillow で
X（Twitter）投稿用の 1080×1080px 的中証明カード画像を生成する。

Usage:
    py -3 scripts/generate_result_card.py --race-id 202604010203
    py -3 scripts/generate_result_card.py --date 20260503
    py -3 scripts/generate_result_card.py --date 20260503 --min-payout 10000
    py -3 scripts/generate_result_card.py --race-id 202604010203 --out outputs/cards/test.png
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import date as dt_date
from pathlib import Path
from typing import Any

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

# ── フォント設定 ──────────────────────────────────────────────────────
_FONT_DIR   = Path("C:/Windows/Fonts")
_FONT_BOLD  = str(_FONT_DIR / "NotoSansJP-VF.ttf")    # 見出し用（Variable Font）
_FONT_REG   = str(_FONT_DIR / "NotoSansJP-VF.ttf")    # 本文用
_FONT_YUGU  = str(_FONT_DIR / "YuGothB.ttc")           # フォールバック

# ── カードサイズ・カラーパレット ─────────────────────────────────────
_W, _H = 1080, 1080

# ダークグラデーション + アクセントカラー
_C = {
    "bg_top":     (10, 14, 28),       # ネイビーブラック
    "bg_bot":     (18, 24, 48),       # 深い紺
    "accent":     (255, 180, 0),      # ゴールド
    "accent2":    (255, 120, 30),     # オレンジ
    "white":      (255, 255, 255),
    "silver":     (200, 210, 230),
    "green":      (40, 210, 100),     # 的中グリーン
    "red":        (255, 60, 60),
    "border":     (255, 180, 0, 180), # ゴールド半透明
    "card_bg":    (22, 30, 58, 220),  # カード内背景
    "overlay":    (0, 0, 0, 80),
}

_DEFAULT_OUT_DIR = _ROOT / "outputs" / "cards"


# ── フォントローダー ──────────────────────────────────────────────────

def _load_font(size: int, bold: bool = False) -> Any:
    """Pillow ImageFont をロードする（フォールバック付き）。"""
    from PIL import ImageFont
    candidates = [_FONT_BOLD if bold else _FONT_REG, _FONT_YUGU]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


# ── グラデーション背景生成 ─────────────────────────────────────────

def _make_bg(w: int, h: int) -> Any:
    """上から下へのグラデーション + ノイズテクスチャ背景を生成する。"""
    from PIL import Image
    import random

    img = Image.new("RGB", (w, h))
    pix = img.load()

    top = _C["bg_top"]
    bot = _C["bg_bot"]

    for y in range(h):
        ratio = y / h
        r = int(top[0] + (bot[0] - top[0]) * ratio)
        g = int(top[1] + (bot[1] - top[1]) * ratio)
        b = int(top[2] + (bot[2] - top[2]) * ratio)
        for x in range(w):
            # ごく微細なノイズで質感を出す
            n = random.randint(-3, 3)
            pix[x, y] = (
                max(0, min(255, r + n)),
                max(0, min(255, g + n)),
                max(0, min(255, b + n)),
            )
    return img


def _draw_rounded_rect(
    draw: Any,
    x0: int, y0: int, x1: int, y1: int,
    radius: int = 20,
    fill: tuple = (0, 0, 0, 160),
    outline: tuple | None = None,
    outline_width: int = 2,
) -> None:
    """角丸矩形を描画する（RGBA 対応）。"""
    from PIL import ImageDraw
    draw.rounded_rectangle(
        [(x0, y0), (x1, y1)],
        radius=radius,
        fill=fill,
        outline=outline,
        width=outline_width,
    )


def _draw_horizontal_gradient_line(
    img: Any,
    y: int,
    x0: int, x1: int,
    color_l: tuple, color_r: tuple,
    width: int = 3,
) -> None:
    """水平グラデーションラインを描画する。"""
    from PIL import ImageDraw
    draw = ImageDraw.Draw(img)
    length = x1 - x0
    for i in range(length):
        ratio = i / max(length, 1)
        r = int(color_l[0] + (color_r[0] - color_l[0]) * ratio)
        g = int(color_l[1] + (color_r[1] - color_l[1]) * ratio)
        b = int(color_l[2] + (color_r[2] - color_l[2]) * ratio)
        for dy in range(width):
            draw.point((x0 + i, y + dy), fill=(r, g, b))


# ── テキスト描画ユーティリティ ────────────────────────────────────────

def _center_text(
    draw: Any,
    text: str,
    y: int,
    font: Any,
    fill: tuple,
    shadow: bool = True,
    shadow_color: tuple = (0, 0, 0),
    shadow_offset: int = 2,
) -> None:
    """水平中央揃えテキストを描画する（シャドウ付き）。"""
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    x = (_W - tw) // 2
    if shadow:
        draw.text((x + shadow_offset, y + shadow_offset), text,
                  font=font, fill=shadow_color)
    draw.text((x, y), text, font=font, fill=fill)


def _left_text(
    draw: Any,
    text: str,
    x: int, y: int,
    font: Any,
    fill: tuple,
    shadow: bool = False,
) -> None:
    if shadow:
        draw.text((x + 2, y + 2), text, font=font, fill=(0, 0, 0))
    draw.text((x, y), text, font=font, fill=fill)


def _right_text(
    draw: Any,
    text: str,
    x_right: int, y: int,
    font: Any,
    fill: tuple,
) -> None:
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    draw.text((x_right - tw, y), text, font=font, fill=fill)


# ── DB ヘルパー ───────────────────────────────────────────────────────

def _fetch_hit_records(
    conn: sqlite3.Connection,
    race_id: str,
    min_payout: float = 0,
) -> list[dict]:
    """指定レースの的中レコードを払戻降順で返す。"""
    rows = conn.execute(
        """
        SELECT pr.id, pr.payout, pr.profit,
               p.model_type, p.bet_type, p.expected_value,
               p.recommended_bet, p.combination_json
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE p.race_id = ?
          AND pr.is_hit = 1
          AND (pr.payout IS NULL OR pr.payout >= ?)
        ORDER BY pr.payout DESC
        LIMIT 5
        """,
        (race_id, min_payout),
    ).fetchall()
    result = []
    for row in rows:
        combo: list = json.loads(row[7]) if row[7] else []
        result.append({
            "payout":      row[1] or 0,
            "profit":      row[2] or 0,
            "model_type":  row[3] or "",
            "bet_type":    row[4] or "",
            "ev":          row[5] or 0.0,
            "rec_bet":     row[6] or 0,
            "combo":       combo,
        })
    return result


def _fetch_race_info(conn: sqlite3.Connection, race_id: str) -> dict:
    row = conn.execute(
        "SELECT race_name, venue, race_number, date, distance, surface FROM races WHERE race_id=?",
        (race_id,),
    ).fetchone()
    if not row:
        return {}
    return {
        "race_name":   row[0] or "",
        "venue":       row[1] or "",
        "race_number": row[2] or 0,
        "date":        row[3] or "",
        "distance":    row[4] or 0,
        "surface":     row[5] or "",
    }


def _fetch_top_hit_races(
    conn: sqlite3.Connection,
    date_str: str,
    top_n: int = 5,
    min_payout: float = 1000,
) -> list[str]:
    """指定日の的中レースを払戻降順で返す。"""
    rows = conn.execute(
        """
        SELECT p.race_id, MAX(pr.payout) AS max_pay
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        JOIN races r ON r.race_id = p.race_id
        WHERE r.date = ? AND pr.is_hit = 1
          AND (pr.payout IS NULL OR pr.payout >= ?)
        GROUP BY p.race_id
        ORDER BY max_pay DESC
        LIMIT ?
        """,
        (date_str, min_payout, top_n),
    ).fetchall()
    return [row[0] for row in rows]


def _fetch_monthly_stats(conn: sqlite3.Connection, date_str: str) -> dict:
    """date_str の月の卍・本命モデル統計を返す。"""
    month = date_str[:7]   # YYYY-MM
    row = conn.execute(
        """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN pr.is_hit = 1 THEN 1 ELSE 0 END) AS hits,
               COALESCE(SUM(pr.payout), 0) AS total_payout,
               COALESCE(SUM(pr.profit), 0) AS total_profit,
               COALESCE(SUM(CASE WHEN pr.profit IS NOT NULL
                                 THEN p.recommended_bet ELSE 0 END), 0) AS total_bet
        FROM predictions p
        JOIN prediction_results pr ON pr.prediction_id = p.id
        JOIN races r ON r.race_id = p.race_id
        WHERE r.date LIKE ? || '%'
        """,
        (month,),
    ).fetchone()
    total, hits, payout, profit, bet = row if row else (0, 0, 0, 0, 0)
    roi = (payout / bet * 100) if bet and bet > 0 else 0.0
    hit_rate = (hits / total * 100) if total and total > 0 else 0.0
    return {
        "total":    total or 0,
        "hits":     hits or 0,
        "profit":   profit or 0,
        "roi":      roi,
        "hit_rate": hit_rate,
        "month":    month,
    }


# ── カード描画メイン ──────────────────────────────────────────────────

def _combo_display(combo: list, bet_type: str, max_len: int = 3) -> str:
    """買い目の組み合わせを「14-3-11」形式で返す。"""
    if not combo:
        return "—"
    entry = combo[0]
    if isinstance(entry, list):
        return "-".join(str(h) for h in entry[:max_len])
    return str(entry)


def generate_card(
    conn: sqlite3.Connection,
    race_id: str,
    min_payout: float = 0,
    out_path: Path | None = None,
) -> Path | None:
    """
    的中証明カード画像を生成して保存する。

    Returns:
        保存したファイルパス。的中なしの場合は None。
    """
    from PIL import Image, ImageDraw, ImageFilter

    race = _fetch_race_info(conn, race_id)
    hits = _fetch_hit_records(conn, race_id, min_payout=min_payout)

    if not hits:
        return None

    # 月間統計
    date_str   = race.get("date", "")
    venue      = race.get("venue", "")
    race_no    = race.get("race_number", 0)
    stats      = _fetch_monthly_stats(conn, date_str)
    race_name  = race.get("race_name") or f"{venue}{race_no}R"
    date_disp  = date_str.replace("-", "/") if date_str else ""
    best_hit   = hits[0]   # 最大払戻

    # ── 背景 ─────────────────────────────────────────────────────────
    bg = _make_bg(_W, _H)
    img = bg.convert("RGBA")
    draw = ImageDraw.Draw(img)

    # ── フォント ──────────────────────────────────────────────────────
    f_xl    = _load_font(80, bold=True)    # ロゴ・最大数値
    f_lg    = _load_font(54, bold=True)    # 見出し
    f_md    = _load_font(40, bold=True)    # サブ見出し
    f_sm    = _load_font(30)               # 本文
    f_xs    = _load_font(24)               # 補足
    f_tag   = _load_font(20)              # タグ

    # ── 装飾ライン（上部） ────────────────────────────────────────────
    _draw_horizontal_gradient_line(
        img, 0, 0, _W, _C["accent"], _C["accent2"], width=6
    )

    # ── UMALOGI ロゴ ─────────────────────────────────────────────────
    _center_text(draw, "UMALOGI", 28, f_xl, _C["accent"], shadow=True)
    _center_text(draw, "AI競馬予想システム", 118, f_xs, _C["silver"])

    # ── 的中バッジ ────────────────────────────────────────────────────
    badge_y = 170
    badge_h = 80
    _draw_rounded_rect(
        draw,
        _W // 2 - 200, badge_y,
        _W // 2 + 200, badge_y + badge_h,
        radius=40,
        fill=_C["green"] + (230,),
        outline=_C["white"],
        outline_width=3,
    )
    _center_text(draw, "◎  的 中 ！", badge_y + 15, f_lg, _C["white"])

    # ── 区切りライン ──────────────────────────────────────────────────
    _draw_horizontal_gradient_line(
        img, 268, 60, _W - 60, _C["accent"], _C["accent2"], width=2
    )

    # ── レース情報カード ─────────────────────────────────────────────
    card_x0, card_y0, card_x1, card_y1 = 50, 285, _W - 50, 460
    _draw_rounded_rect(
        draw, card_x0, card_y0, card_x1, card_y1,
        radius=18,
        fill=(22, 30, 58, 200),
        outline=(_C["accent"][0], _C["accent"][1], _C["accent"][2], 150),
        outline_width=2,
    )
    _center_text(draw, f"【 {race_name} 】", card_y0 + 20, f_md, _C["accent"], shadow=True)
    _center_text(draw,
        f"{date_disp}（{venue}）{race_no}R",
        card_y0 + 78, f_sm, _C["silver"],
    )

    # ── 最大払戻ハイライト ────────────────────────────────────────────
    hl_y0, hl_y1 = 478, 650
    _draw_rounded_rect(
        draw, 50, hl_y0, _W - 50, hl_y1,
        radius=18,
        fill=(40, 20, 0, 200),
        outline=(_C["accent2"][0], _C["accent2"][1], _C["accent2"][2], 200),
        outline_width=3,
    )
    bet_type  = best_hit["bet_type"]
    combo_str = _combo_display(best_hit["combo"], bet_type)
    ev_str    = f"EV {best_hit['ev']:.2f}"
    pay_val   = int(best_hit["payout"])
    pay_str   = f"¥{pay_val:,}"

    _center_text(draw, f"{bet_type}  {combo_str}", hl_y0 + 18, f_md, _C["white"], shadow=True)
    _center_text(draw, pay_str, hl_y0 + 68, f_xl, _C["accent"], shadow=True,
                 shadow_color=(100, 60, 0))
    _center_text(draw, f"払戻金額  /  {ev_str}", hl_y0 + 148, f_xs, _C["silver"])

    # ── 複数的中がある場合の追加行 ────────────────────────────────────
    if len(hits) > 1:
        y_extra = hl_y1 + 12
        for extra in hits[1:3]:
            tag = f"{extra['bet_type']} {_combo_display(extra['combo'], extra['bet_type'])}  ¥{int(extra['payout']):,}"
            _center_text(draw, tag, y_extra, f_xs, _C["silver"])
            y_extra += 34

    # ── 月間実績カード ────────────────────────────────────────────────
    stat_y0 = 690
    stat_y1 = 920
    _draw_rounded_rect(
        draw, 50, stat_y0, _W - 50, stat_y1,
        radius=18,
        fill=(18, 24, 48, 200),
        outline=(_C["accent"][0], _C["accent"][1], _C["accent"][2], 120),
        outline_width=2,
    )

    month_label = stats["month"].replace("-", "/") + " 月間実績"
    _center_text(draw, month_label, stat_y0 + 16, f_sm, _C["accent"])

    # 区切り線
    draw.line([(80, stat_y0 + 58), (_W - 80, stat_y0 + 58)],
              fill=_C["accent"] + (80,), width=1)

    # 3列レイアウト
    cols = [
        ("ROI",   f"{stats['roi']:.0f}%",   _C["green"] if stats["roi"] >= 100 else _C["accent"]),
        ("月間純利", f"¥{int(stats['profit']):,}", _C["green"] if stats["profit"] >= 0 else _C["red"]),
        ("的中率",  f"{stats['hit_rate']:.1f}%", _C["white"]),
    ]
    col_w = (_W - 100) // 3
    for i, (label, val, color) in enumerate(cols):
        cx = 50 + col_w * i + col_w // 2
        bbox = draw.textbbox((0, 0), val, font=f_lg)
        tw   = bbox[2] - bbox[0]
        # 値
        draw.text((cx - tw // 2, stat_y0 + 76), val, font=f_lg, fill=color)
        # ラベル
        bbox2 = draw.textbbox((0, 0), label, font=f_xs)
        tw2   = bbox2[2] - bbox2[0]
        draw.text((cx - tw2 // 2, stat_y0 + 152), label, font=f_xs, fill=_C["silver"])

    # 補足
    _center_text(
        draw,
        f"予想件数 {stats['total']} 件 / 的中 {stats['hits']} 件",
        stat_y0 + 198,
        f_xs, _C["silver"],
    )

    # ── フッター ──────────────────────────────────────────────────────
    _draw_horizontal_gradient_line(
        img, _H - 8, 0, _W, _C["accent2"], _C["accent"], width=4
    )
    _center_text(
        draw,
        "※本予想は情報提供のみ。馬券購入は自己責任で。JRA-VANデータ使用。",
        _H - 52, f_tag, (120, 130, 150),
    )

    # ── RGBA → RGB 変換して PNG 保存 ─────────────────────────────────
    final = Image.new("RGB", (_W, _H), (10, 14, 28))
    final.paste(img, mask=img.split()[3])

    # 出力パス
    if out_path is None:
        safe_name = (race_name or race_id).replace("（", "(").replace("）", ")")
        import re
        safe_name = re.sub(r'[\\/:*?"<>|]', "_", safe_name)
        date_raw  = (date_str or "").replace("-", "")
        filename  = f"{date_raw}_R{race_no:02d}_{safe_name}.png"
        out_path  = _DEFAULT_OUT_DIR / filename

    out_path.parent.mkdir(parents=True, exist_ok=True)
    final.save(str(out_path), "PNG", optimize=True)
    return out_path


# ── メイン ───────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="的中実績カード画像を生成する")
    p.add_argument("--race-id",    help="対象レースID（指定時は --date より優先）")
    p.add_argument("--date",       help="対象日 YYYYMMDD（省略時=本日）")
    p.add_argument("--top",        type=int, default=3,
                   help="日次で上位何レースを生成するか（デフォルト 3）")
    p.add_argument("--min-payout", type=float, default=1000,
                   help="最低払戻金額フィルタ（円、デフォルト 1000）")
    p.add_argument("--out",        help="出力ファイルパス（単一レースのみ有効）")
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    from src.database.init_db import init_db
    conn = init_db()

    if args.race_id:
        race_ids = [args.race_id]
        date_str = (
            f"{args.race_id[2:6]}-{args.race_id[6:8]}-{args.race_id[8:10]}"
        )
    else:
        raw = (args.date or dt_date.today().strftime("%Y%m%d")).replace("-", "")
        date_str = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
        race_ids = _fetch_top_hit_races(
            conn, date_str, top_n=args.top, min_payout=args.min_payout
        )

    if not race_ids:
        print(f"[CARD] 対象レースが見つかりません (date={date_str}, min_payout={args.min_payout})")
        conn.close()
        return

    print(f"[CARD] カード生成対象: {len(race_ids)} レース")

    for race_id in race_ids:
        out_path = Path(args.out) if args.out and len(race_ids) == 1 else None
        saved = generate_card(
            conn=conn,
            race_id=race_id,
            min_payout=args.min_payout,
            out_path=out_path,
        )
        if saved:
            print(f"[CARD] 保存: {saved}")
        else:
            print(f"[CARD] 的中なし / 閾値未達: {race_id}")

    conn.close()


if __name__ == "__main__":
    main()
