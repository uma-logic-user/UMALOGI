"""race_name の賞金等級プレフィックス・末尾ゴミ除去 + prediction_horses 洗浄"""
import re
import sqlite3
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
REPL_RE = re.compile(r"�")          # 置換文字
FWSP_RE = re.compile(r"[　　\s]+$")  # 末尾の全角スペース
QAT_RE  = re.compile(r"(\?@)+")          # SJIS全角スペース誤デコード ?@ パターン


def clean_race_name(v: str) -> str:
    # 先頭の賞金等級コード (5桁数字) を除去
    v = re.sub(r"^\d{5}", "", v)
    # 置換文字を除去
    v = REPL_RE.sub("", v)
    # ?@ パターン (SJIS 全角スペース 0x81 0x40 の誤デコード) をスペースに変換して strip
    v = QAT_RE.sub(" ", v).strip()
    # 末尾の全角スペース・空白
    v = FWSP_RE.sub("", v)
    # 残存制御文字
    v = CTRL_RE.sub("", v)
    return v.strip()


def main() -> None:
    conn = sqlite3.connect("data/umalogi.db")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # ── races.race_name ──────────────────────────────────────────────
    rows = conn.execute(
        "SELECT race_id, race_name FROM races WHERE race_name IS NOT NULL"
    ).fetchall()

    need_fix = [
        (rid, v) for rid, v in rows
        if re.match(r"^\d{5}", v or "")
        or "�" in (v or "")
        or "　" in (v or "")
        or "?@" in (v or "")
    ]
    print(f"races.race_name: {len(need_fix)}/{len(rows)} 件を修正")

    for rid, v in need_fix:
        new_v = clean_race_name(v)
        conn.execute("UPDATE races SET race_name=? WHERE race_id=?", (new_v, rid))

    # ── prediction_horses.horse_name ─────────────────────────────────
    rows2 = conn.execute(
        "SELECT id, horse_name FROM prediction_horses WHERE horse_name IS NOT NULL"
    ).fetchall()
    bad2 = [
        (i, v) for i, v in rows2
        if isinstance(v, str) and (
            CTRL_RE.search(v) or "�" in v or "　" in v
        )
    ]
    print(f"prediction_horses.horse_name: {len(bad2)} 件を修正")

    for i, v in bad2:
        new_v = CTRL_RE.sub("", REPL_RE.sub("", v)).strip()
        conn.execute(
            "UPDATE prediction_horses SET horse_name=? WHERE id=?", (new_v, i)
        )

    conn.commit()

    # ── 修正後サンプル確認 ────────────────────────────────────────────
    print("\n=== races.race_name 修正後サンプル ===")
    for row in conn.execute(
        "SELECT race_id, race_name FROM races ORDER BY date DESC LIMIT 15"
    ).fetchall():
        print(f"  {row[0]}: {repr(row[1])}")

    print("\n=== prediction_horses 修正後サンプル ===")
    for row in conn.execute(
        "SELECT id, horse_name FROM prediction_horses LIMIT 8"
    ).fetchall():
        print(f"  id={row[0]}: {repr(row[1])}")

    conn.close()
    print("\n完了")


if __name__ == "__main__":
    main()
