"""
scripts/generate_sns_post.py — X（Twitter）用投稿テキストの自動生成

2パターンを生成して outputs/sns/ に保存する。

  パターンA（レース前・note誘導）:
    AIが弾き出した高EV穴馬をnoteで公開中というポスト

  パターンB（レース後・的中実績）:
    払戻金額・券種・ROIを強調した実績報告ポスト

Usage:
    py -3 scripts/generate_sns_post.py --race-id 202604010203 --pattern ab
    py -3 scripts/generate_sns_post.py --race-id 202604010203 --note-url https://note.com/xxx
    py -3 scripts/generate_sns_post.py --date 20260503 --pattern b
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import sys
from datetime import date as dt_date
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

_OUT_DIR = _ROOT / "outputs" / "sns"
_CARDS_DIR = _ROOT / "outputs" / "cards"

# ── ハッシュタグ定数 ────────────────────────────────────────────────
_TAGS_COMMON = "#競馬予想 #AI競馬 #UMALOGI"
_TAGS_HIT    = "#競馬的中 #万馬券 #競馬予想 #AI競馬 #UMALOGI"
_TAGS_PRE    = "#競馬予想 #AI競馬 #穴馬 #期待値 #UMALOGI"

# X の文字数制限（URL は 23 文字固定換算）
_X_LIMIT = 280   # 英字換算。日本語は1文字=2相当なので実質~140文字


# ── DB ヘルパー ──────────────────────────────────────────────────────

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


def _fetch_best_hit(conn: sqlite3.Connection, race_id: str) -> dict | None:
    """最大払戻の的中レコードを返す。的中なしなら None。"""
    row = conn.execute(
        """
        SELECT pr.payout, pr.profit,
               p.bet_type, p.combination_json, p.expected_value, p.model_type
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        WHERE p.race_id = ? AND pr.is_hit = 1
        ORDER BY pr.payout DESC
        LIMIT 1
        """,
        (race_id,),
    ).fetchone()
    if not row:
        return None
    combo = json.loads(row[3]) if row[3] else []
    return {
        "payout":     row[0] or 0,
        "profit":     row[1] or 0,
        "bet_type":   row[2] or "",
        "combo":      combo,
        "ev":         row[4] or 0.0,
        "model_type": row[5] or "",
    }


def _fetch_top_ev_pick(conn: sqlite3.Connection, race_id: str) -> dict | None:
    """最大EV予想（レース前投稿用）を返す。"""
    row = conn.execute(
        """
        SELECT p.expected_value, p.bet_type, p.combination_json, p.model_type
        FROM predictions p
        WHERE p.race_id = ?
        ORDER BY p.expected_value DESC
        LIMIT 1
        """,
        (race_id,),
    ).fetchone()
    if not row:
        return None
    combo = json.loads(row[2]) if row[2] else []
    return {
        "ev":         row[0] or 0.0,
        "bet_type":   row[1] or "",
        "combo":      combo,
        "model_type": row[3] or "",
    }


def _fetch_monthly_roi(conn: sqlite3.Connection, date_str: str) -> float:
    """date_str の月の ROI（%）を返す。"""
    month = date_str[:7]
    row = conn.execute(
        """
        SELECT COALESCE(SUM(pr.payout),0),
               COALESCE(SUM(CASE WHEN pr.profit IS NOT NULL THEN p.recommended_bet ELSE 0 END),0)
        FROM predictions p
        JOIN prediction_results pr ON pr.prediction_id = p.id
        JOIN races r ON r.race_id = p.race_id
        WHERE r.date LIKE ? || '%'
        """,
        (month,),
    ).fetchone()
    payout, bet = row if row else (0, 0)
    return (payout / bet * 100) if bet and bet > 0 else 0.0


def _find_card_path(race_id: str) -> str | None:
    """race_id に対応する的中カード画像のパスを検索する。"""
    date_raw = f"{race_id[2:6]}{race_id[6:8]}{race_id[8:10]}"
    race_no  = int(race_id[10:12])
    pattern  = f"{date_raw}_R{race_no:02d}_*.png"
    matches  = list(_CARDS_DIR.glob(pattern))
    return str(matches[0]) if matches else None


# ── テキスト生成 ────────────────────────────────────────────────────

def _combo_str(combo: list, bet_type: str) -> str:
    if not combo:
        return ""
    entry = combo[0]
    if isinstance(entry, list):
        horses = "-".join(str(h) for h in entry[:3])
        return f"{bet_type} {horses}"
    return f"{bet_type} {entry}"


def _surface_str(s: str) -> str:
    return {"T": "芝", "D": "ダート"}.get(s, "")


def generate_pattern_a(
    race: dict,
    pick: dict | None,
    note_url: str = "",
) -> str:
    """
    パターンA: レース前・note誘導ポスト（140文字以内）

    例:
    🎯【AI競馬予想】2026/05/03 新潟3R
    期待値EV6.0の穴馬をnoteで公開中！
    JRA-VANデータ分析・AI自動予想 #競馬予想 #AI競馬
    👇 note: https://note.com/xxx
    """
    race_name  = race.get("race_name") or f"{race.get('venue','')}{race.get('race_number',0)}R"
    date_disp  = (race.get("date") or "").replace("-", "/")
    ev_str     = f"EV{pick['ev']:.1f}" if pick else "高期待値"
    combo      = _combo_str(pick["combo"], pick["bet_type"]) if pick else ""
    surface    = _surface_str(race.get("surface", ""))
    dist       = f"{race.get('distance',0)}m" if race.get("distance") else ""
    cond_str   = f"（{surface}{dist}）" if surface or dist else ""

    url_part   = f"\n👇 note: {note_url}" if note_url else ""

    variants = [
        (
            f"🎯【AI競馬予想】{date_disp} {race_name}{cond_str}\n"
            f"UMALOGIが{ev_str}の高期待値買い目を算出！\n"
            f"JRA-VANデータ×LightGBM AIの本命予想をnoteで公開中。\n"
            f"{_TAGS_PRE}{url_part}"
        ),
        (
            f"📊 {date_disp} {race_name} AI予想公開\n"
            f"期待値{ev_str}の穴馬を狙い打ち。\n"
            f"的中率・回収率データ付きの詳細根拠はnoteをチェック！\n"
            f"{_TAGS_PRE}{url_part}"
        ),
        (
            f"🤖 UMALOGI AI予想｜{race_name}（{date_disp}）\n"
            f"JRA公式データのみを学習したAIが{ev_str}の買い目を算出。\n"
            f"根拠・買い目・金額すべてnoteで公開中！\n"
            f"{_TAGS_PRE}{url_part}"
        ),
    ]
    return random.choice(variants)


def generate_pattern_b(
    race: dict,
    hit: dict,
    monthly_roi: float,
    card_path: str | None = None,
) -> str:
    """
    パターンB: レース後・的中実績ポスト（140文字以内）

    例:
    🔥三連単 14-3-11 的中！¥505,920
    新潟3R でUMALOGI AI予想が炸裂 💥
    今月ROI 201%・純利 +¥77万
    来週の予想もお楽しみに！ #競馬的中 ...
    """
    race_name  = race.get("race_name") or f"{race.get('venue','')}{race.get('race_number',0)}R"
    date_disp  = (race.get("date") or "").replace("-", "/")
    pay        = int(hit["payout"])
    combo      = _combo_str(hit["combo"], hit["bet_type"])
    roi_str    = f"{monthly_roi:.0f}%" if monthly_roi > 0 else "—"

    # ROI 別リアクション
    if pay >= 100000:
        bang = "💥💥 大爆発！"
    elif pay >= 30000:
        bang = "🔥 炸裂！"
    else:
        bang = "✅ 的中！"

    card_note = f"\n📸 画像: {card_path}" if card_path else ""

    variants = [
        (
            f"🔥【的中報告】{date_disp} {race_name}\n"
            f"{combo} {bang} ¥{pay:,}\n"
            f"今月ROI {roi_str} — UMALOGIのAI予想が炸裂しました！\n"
            f"来週の予想もnoteをお楽しみに💰\n"
            f"{_TAGS_HIT}{card_note}"
        ),
        (
            f"🎊 {combo} 的中！¥{pay:,}\n"
            f"{race_name}（{date_disp}）で爆発！\n"
            f"UMALOGI 今月ROI {roi_str}。AIが期待値ベースで厳選した結果です。\n"
            f"予想詳細はnoteで公開中👇\n"
            f"{_TAGS_HIT}{card_note}"
        ),
        (
            f"💰 ¥{pay:,} {bang}\n"
            f"{date_disp} {race_name} {combo}\n"
            f"UMALOGIのAI予想（EV={hit['ev']:.1f}）が正解。今月ROI {roi_str}。\n"
            f"来週も予想公開予定です！\n"
            f"{_TAGS_HIT}{card_note}"
        ),
    ]
    return random.choice(variants)


# ── 保存ユーティリティ ────────────────────────────────────────────

def _safe_name(s: str) -> str:
    import re
    s = s.replace("（", "(").replace("）", ")")
    return re.sub(r'[\\/:*?"<>|]', "_", s).strip()


def _save_post(text: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    print(f"[SNS] 保存: {out_path}")


# ── メイン処理 ────────────────────────────────────────────────────

def generate_posts(
    conn: sqlite3.Connection,
    race_id: str,
    patterns: str = "ab",
    note_url: str = "",
) -> dict[str, Path]:
    """
    指定レースの SNS ポスト文を生成して保存する。

    Returns:
        {"a": Path, "b": Path} — 生成したファイルのパス辞書
    """
    race    = _fetch_race_info(conn, race_id)
    date_str = race.get("date", "")
    race_name = race.get("race_name") or f"{race.get('venue','')}{race.get('race_number',0)}R"
    date_raw  = (date_str or "").replace("-", "")
    race_no   = race.get("race_number", 0)
    safe      = _safe_name(race_name)

    saved: dict[str, Path] = {}

    if "a" in patterns:
        pick = _fetch_top_ev_pick(conn, race_id)
        text = generate_pattern_a(race, pick, note_url=note_url)
        path = _OUT_DIR / f"{date_raw}_R{race_no:02d}_{safe}_prerace.txt"
        _save_post(text, path)
        saved["a"] = path

    if "b" in patterns:
        hit = _fetch_best_hit(conn, race_id)
        if hit:
            roi  = _fetch_monthly_roi(conn, date_str)
            card = _find_card_path(race_id)
            text = generate_pattern_b(race, hit, monthly_roi=roi, card_path=card)
            path = _OUT_DIR / f"{date_raw}_R{race_no:02d}_{safe}_hit.txt"
            _save_post(text, path)
            saved["b"] = path
        else:
            print(f"[SNS] 的中なし → パターンB スキップ: {race_id}")

    return saved


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="X（Twitter）用投稿テキストを自動生成する")
    p.add_argument("--race-id",   help="対象レースID（指定時は --date より優先）")
    p.add_argument("--date",      help="対象日 YYYYMMDD（省略時=本日）")
    p.add_argument("--top",       type=int, default=3,
                   help="日次で上位何レースを処理するか（デフォルト 3）")
    p.add_argument("--pattern",   default="ab",
                   help="生成パターン a / b / ab（デフォルト ab）")
    p.add_argument("--note-url",  default="",
                   help="パターンA に埋め込む note 記事 URL（省略可）")
    p.add_argument("--min-payout", type=float, default=0,
                   help="パターンB の最低払戻フィルタ（円、デフォルト 0）")
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    from src.database.init_db import init_db
    conn = init_db()

    if args.race_id:
        race_ids = [args.race_id]
    else:
        raw = (args.date or dt_date.today().strftime("%Y%m%d")).replace("-", "")
        date_str = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"

        # EV 上位レースを対象
        rows = conn.execute(
            """
            SELECT p.race_id, MAX(p.expected_value) AS max_ev
            FROM predictions p
            JOIN races r ON r.race_id = p.race_id
            WHERE r.date = ?
            GROUP BY p.race_id
            ORDER BY max_ev DESC
            LIMIT ?
            """,
            (date_str, args.top),
        ).fetchall()
        race_ids = [r[0] for r in rows]

    if not race_ids:
        print("[SNS] 対象レースが見つかりません")
        conn.close()
        return

    print(f"[SNS] 処理対象: {len(race_ids)} レース")
    for race_id in race_ids:
        generate_posts(
            conn=conn,
            race_id=race_id,
            patterns=args.pattern,
            note_url=args.note_url,
        )

    conn.close()


if __name__ == "__main__":
    main()
