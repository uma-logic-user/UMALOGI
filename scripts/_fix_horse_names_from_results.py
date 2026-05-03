"""
entries.horse_name / prediction_horses.horse_name / horses.horse_name を
race_results.horse_name (信頼できるソース) で修復する。

cp1252 逆変換も試みて horses テーブルを再洗浄する。
"""
import re
import sqlite3
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
REPL_RE = re.compile(r"[ï¿½�]")
QAT_RE  = re.compile(r"(\?@)+")


def has_garbage(v: str) -> bool:
    return bool(
        CTRL_RE.search(v) or "?" in v or "�" in v
        or QAT_RE.search(v) or "ï" in v
    )


def try_cp1252_recover(v: str) -> str:
    """cp1252逆変換でSJISバイト列を回復してcp932デコードを試みる。"""
    try:
        raw = v.encode("cp1252", errors="replace")
        recovered = raw.decode("cp932", errors="replace")
        cleaned = CTRL_RE.sub("", REPL_RE.sub("", recovered)).strip()
        return cleaned
    except Exception:
        return CTRL_RE.sub("", REPL_RE.sub("", v)).strip()


def main() -> None:
    conn = sqlite3.connect("data/umalogi.db")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # ── 1. horses.horse_name を cp1252 逆変換で再洗浄 ─────────────────
    rows = conn.execute(
        "SELECT horse_id, horse_name FROM horses WHERE horse_name IS NOT NULL"
    ).fetchall()
    need = [(hid, v) for hid, v in rows if isinstance(v, str) and has_garbage(v)]
    print(f"horses.horse_name: {len(need)} 件を cp1252 逆変換で再洗浄")
    for hid, v in need:
        new_v = try_cp1252_recover(v)
        conn.execute("UPDATE horses SET horse_name=? WHERE horse_id=?", (new_v, hid))

    # ── 2. entries.horse_name を race_results.horse_name で修復 ───────
    result = conn.execute("""
        UPDATE entries
        SET horse_name = (
            SELECT rr.horse_name FROM race_results rr
            WHERE rr.horse_id = entries.horse_id
              AND rr.horse_name IS NOT NULL
              AND rr.horse_name != ''
            LIMIT 1
        )
        WHERE horse_name IS NULL
           OR horse_name LIKE '%?%'
           OR horse_name LIKE '%' || char(65533) || '%'
    """)
    print(f"entries.horse_name: {result.rowcount} 件を race_results から修復")

    # ── 3. prediction_horses.horse_name を race_results で修復 ────────
    result2 = conn.execute("""
        UPDATE prediction_horses
        SET horse_name = (
            SELECT rr.horse_name FROM race_results rr
            WHERE rr.horse_id = prediction_horses.horse_id
              AND rr.horse_name IS NOT NULL
              AND rr.horse_name != ''
            LIMIT 1
        )
        WHERE horse_id IS NOT NULL
          AND (horse_name IS NULL
           OR horse_name LIKE '%?%'
           OR horse_name LIKE '%' || char(65533) || '%')
    """)
    print(f"prediction_horses.horse_name: {result2.rowcount} 件を race_results から修復")

    conn.commit()

    # ── 確認サンプル ──────────────────────────────────────────────────
    print("\n=== prediction_horses 修復後サンプル ===")
    for row in conn.execute("""
        SELECT ph.horse_name, ph.model_score
        FROM prediction_horses ph
        JOIN predictions p ON p.id=ph.prediction_id
        JOIN races r ON r.race_id=p.race_id
        WHERE r.date='2026-05-02'
        ORDER BY ph.id DESC LIMIT 8
    """).fetchall():
        print(f"  name={repr(row[0])} score={row[1]:.4f}")

    print("\n=== horses.horse_name 修復後サンプル (先頭5件) ===")
    for row in conn.execute("SELECT horse_id, horse_name FROM horses LIMIT 5").fetchall():
        print(f"  {row[0]}: {repr(row[1])}")

    conn.close()
    print("\n完了")


if __name__ == "__main__":
    main()
